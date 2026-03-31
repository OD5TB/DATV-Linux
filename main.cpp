#include "dvbs2_tx.h"

#ifdef HAVE_HACKRF
#include <libhackrf/hackrf.h>
#endif

#ifdef HAVE_PLUTO
#include <iio.h>
#endif

#ifdef HAVE_SOAPYSDR
#include <SoapySDR/Device.hpp>
#include <SoapySDR/Formats.hpp>
#include <SoapySDR/Errors.hpp>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cmath>
#include <complex>
#include <csignal>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <poll.h>
#include <cstdlib>
#include <limits>
#include <pthread.h>
#include <sched.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

using cf = std::complex<float>;

constexpr size_t kRingIqCapacity = 8u * 1024u * 1024u; // ~= 64 MiB float IQ ring
constexpr int kDefaultNumTaps = 101;
constexpr int kLowSrNumTaps = 161;
constexpr uint32_t kHackrfMinSampleRate = 2000000u;
constexpr uint32_t kPlutoSafeMinSampleRate = 2083333u;
constexpr uint32_t kPlutoCompatTargetSampleRate = 2000000u;
constexpr uint32_t kPlutoPreferredCleanMaxSymbolRate = 500000u;
constexpr uint32_t kHackrfPreferredCleanMinSymbolRate = 200000u;
constexpr uint32_t kLimePreferredFixedTenMinSymbolRate = 100000u;
constexpr uint32_t kLimeMinSampleRate = 1000000u;
constexpr float kHackrfFullScale = 127.0f;
constexpr float kPlutoFullScale = 2047.0f;
constexpr float kLimeFullScale = 32767.0f;
constexpr int kPlutoSampleLeftShift = 4; // AD9361 TX data is 12-bit MSB-aligned inside a 16-bit container
constexpr long long kPlutoSampleRateReadbackToleranceHz = 2;

enum class OutputDevice { HackRF, PlutoStock, PlutoDVB2F5OEO, Pluto03032402, Lime };
enum class SampleRateMode { AutoSafe, PlutoF5oeoCompat };
enum class GainMode { Adaptive, Fixed };

bool is_pluto_family(OutputDevice device)
{
    return device == OutputDevice::PlutoStock ||
           device == OutputDevice::PlutoDVB2F5OEO ||
           device == OutputDevice::Pluto03032402;
}

const char* output_device_label(OutputDevice device)
{
    switch (device) {
    case OutputDevice::PlutoStock: return "ADALM Pluto / Pluto+ (stock libiio path)";
    case OutputDevice::PlutoDVB2F5OEO: return "PlutoDVB2 / F5OEO (direct libiio backend contract)";
    case OutputDevice::Pluto03032402: return "Pluto 0303/2402 (direct libiio backend contract)";
    case OutputDevice::Lime: return "LimeSDR Mini (SoapySDR)";
    case OutputDevice::HackRF:
    default: return "HackRF One";
    }
}

OutputDevice parse_output_device(const std::string& value)
{
    std::string lower;
    for (unsigned char ch : value) lower.push_back(static_cast<char>(std::tolower(ch)));
    if (lower.empty() || lower == "hackrf" || lower == "hackrfone" || lower == "hackrf_one") return OutputDevice::HackRF;
    if (lower == "pluto" || lower == "plutosdr" || lower == "pluto+" || lower == "plutoplus" ||
        lower == "adalmpluto" || lower == "adalm_pluto") return OutputDevice::PlutoStock;
    if (lower == "plutodvb" || lower == "plutodvb2" || lower == "f5oeo" || lower == "plutof5oeo") return OutputDevice::PlutoDVB2F5OEO;
    if (lower == "0303" || lower == "2402" || lower == "pluto0303" || lower == "pluto_0303" ||
        lower == "plutof5oeo0303" || lower == "pluto_0303_2402" || lower == "pluto03032402") return OutputDevice::Pluto03032402;
    if (lower == "lime" || lower == "limesdr" || lower == "limesdrmini" || lower == "limesdr_mini" ||
        lower == "lime_mini" || lower == "lime-mini") return OutputDevice::Lime;
    throw std::runtime_error("Unsupported RF output device: " + value);
}

std::string normalise_pluto_uri(const std::string& value)
{
    if (value.empty()) return "ip:192.168.2.1";
    if (value.rfind("ip:", 0) == 0 || value.rfind("usb:", 0) == 0 || value.rfind("serial:", 0) == 0) return value;
    return "ip:" + value;
}


uint32_t minimum_sample_rate_for_device(OutputDevice device)
{
    if (is_pluto_family(device)) return kPlutoSafeMinSampleRate;
    switch (device) {
    case OutputDevice::Lime: return kLimeMinSampleRate;
    case OutputDevice::HackRF:
    default: return kHackrfMinSampleRate;
    }
}

bool parse_low_sr_stabilize_from_env()
{
    const char* value = std::getenv("DATV_LOW_SR_STABILIZE");
    if (value == nullptr) return false;
    std::string lower;
    for (const unsigned char ch : std::string(value)) lower.push_back(static_cast<char>(std::tolower(ch)));
    return lower == "1" || lower == "true" || lower == "yes" || lower == "on";
}

int choose_rrc_num_taps(uint32_t symbol_rate, bool low_sr_stabilize)
{
    if (!low_sr_stabilize) {
        return kDefaultNumTaps;
    }
    if (symbol_rate < 250000u) {
        return kLowSrNumTaps;
    }
    return kDefaultNumTaps;
}

SampleRateMode parse_sample_rate_mode_from_env()
{
    const char* value = std::getenv("DATV_TX_SAMPLE_RATE_MODE");
    if (value == nullptr) return SampleRateMode::AutoSafe;
    std::string lower;
    for (const unsigned char ch : std::string(value)) lower.push_back(static_cast<char>(std::tolower(ch)));
    if (lower == "pluto_f5oeo_compat" || lower == "pluto-compat" || lower == "compat") {
        return SampleRateMode::PlutoF5oeoCompat;
    }
    return SampleRateMode::AutoSafe;
}

const char* sample_rate_mode_name(SampleRateMode mode)
{
    switch (mode) {
    case SampleRateMode::PlutoF5oeoCompat: return "pluto-f5oeo-compat";
    case SampleRateMode::AutoSafe:
    default: return "auto-safe";
    }
}

GainMode parse_gain_mode_from_env()
{
    const char* value = std::getenv("DATV_TX_GAIN_MODE");
    if (value == nullptr) return GainMode::Fixed;
    std::string lower;
    for (const unsigned char ch : std::string(value)) lower.push_back(static_cast<char>(std::tolower(ch)));
    if (lower == "fixed" || lower == "manual") {
        return GainMode::Fixed;
    }
    return GainMode::Adaptive;
}

const char* gain_mode_name(GainMode mode)
{
    switch (mode) {
    case GainMode::Fixed: return "fixed";
    case GainMode::Adaptive:
    default: return "adaptive";
    }
}

float parse_fixed_gain_from_env()
{
    const char* value = std::getenv("DATV_TX_FIXED_GAIN");
    if (value == nullptr || *value == '\0') return 1.0f;
    char* end = nullptr;
    const double parsed = std::strtod(value, &end);
    if (end == value || !std::isfinite(parsed)) return 1.0f;
    return static_cast<float>(std::clamp(parsed, 0.05, 16.0));
}

int choose_even_sps_with_floor(uint32_t symbol_rate, uint32_t minimum_rate, int min_sps)
{
    if (symbol_rate == 0u) return std::max(2, min_sps);
    int value = std::max(min_sps, static_cast<int>(std::ceil(static_cast<double>(minimum_rate) / static_cast<double>(symbol_rate))));
    if (value & 1) {
        ++value;
    }
    return value;
}

int choose_even_sps_near_target(uint32_t symbol_rate, uint32_t target_rate, int min_sps, int max_sps)
{
    if (symbol_rate == 0u) return std::max(2, min_sps);
    int start = (min_sps & 1) ? (min_sps + 1) : min_sps;
    int best = std::max(2, start);
    uint64_t best_delta = std::numeric_limits<uint64_t>::max();
    for (int candidate = start; candidate <= max_sps; candidate += 2) {
        const uint64_t sample_rate = static_cast<uint64_t>(symbol_rate) * static_cast<uint64_t>(candidate);
        const uint64_t delta = (sample_rate > target_rate) ? (sample_rate - target_rate) : (target_rate - sample_rate);
        if (delta < best_delta) {
            best_delta = delta;
            best = candidate;
        }
    }
    return best;
}

int choose_even_sps_near_target_with_floor(uint32_t symbol_rate, uint32_t target_rate, uint32_t minimum_rate,
                                           int min_sps, int max_sps)
{
    if (symbol_rate == 0u) return std::max(2, min_sps);
    int start = (min_sps & 1) ? (min_sps + 1) : min_sps;
    int best = -1;
    uint64_t best_delta = std::numeric_limits<uint64_t>::max();
    for (int candidate = start; candidate <= max_sps; candidate += 2) {
        const uint64_t sample_rate = static_cast<uint64_t>(symbol_rate) * static_cast<uint64_t>(candidate);
        if (sample_rate < static_cast<uint64_t>(minimum_rate)) {
            continue;
        }
        const uint64_t delta = (sample_rate > target_rate) ? (sample_rate - target_rate) : (target_rate - sample_rate);
        if (delta < best_delta) {
            best_delta = delta;
            best = candidate;
        }
    }
    if (best > 0) {
        return best;
    }
    return choose_even_sps_with_floor(symbol_rate, minimum_rate, min_sps);
}

uint32_t choose_pluto_rf_bandwidth(uint32_t symbol_rate, RollOff rolloff, uint32_t sample_rate)
{
    const double occupied_bw = static_cast<double>(symbol_rate) * (1.0 + static_cast<double>(rolloff_to_alpha(rolloff)));
    const double target_bw = std::max(occupied_bw * 1.25, static_cast<double>(sample_rate) * 0.10);
    return static_cast<uint32_t>(std::clamp(target_bw, 350000.0, 56000000.0));
}

[[maybe_unused]] size_t choose_pluto_buffer_samples(uint32_t sample_rate)
{
    if (sample_rate < 3000000u) return 65536u;
    if (sample_rate <= 5000000u) return 131072u;
    return 262144u;
}

uint32_t choose_hackrf_rf_bandwidth(uint32_t symbol_rate, RollOff rolloff, uint32_t sample_rate)
{
    const double occupied_bw = static_cast<double>(symbol_rate) * (1.0 + static_cast<double>(rolloff_to_alpha(rolloff)));
    const double target_bw = std::max(occupied_bw * 1.60, static_cast<double>(sample_rate) * 0.22);
    return static_cast<uint32_t>(std::clamp(target_bw, 500000.0, std::min(28000000.0, static_cast<double>(sample_rate))));
}

uint32_t choose_lime_rf_bandwidth(uint32_t symbol_rate, RollOff rolloff, uint32_t sample_rate)
{
    const double occupied_bw = static_cast<double>(symbol_rate) * (1.0 + static_cast<double>(rolloff_to_alpha(rolloff)));
    const double target_bw = std::max(occupied_bw * 1.35, static_cast<double>(sample_rate) * 0.12);
    return static_cast<uint32_t>(std::clamp(target_bw, 300000.0, std::min(60000000.0, static_cast<double>(sample_rate))));
}

uint32_t choose_output_rf_bandwidth(OutputDevice device, uint32_t symbol_rate, RollOff rolloff, uint32_t sample_rate)
{
    if (is_pluto_family(device)) {
        return choose_pluto_rf_bandwidth(symbol_rate, rolloff, sample_rate);
    }
    if (device == OutputDevice::Lime) {
        return choose_lime_rf_bandwidth(symbol_rate, rolloff, sample_rate);
    }
    return choose_hackrf_rf_bandwidth(symbol_rate, rolloff, sample_rate);
}

int choose_samples_per_symbol(uint32_t symbol_rate, OutputDevice device, SampleRateMode mode)
{
    const uint32_t min_rate = minimum_sample_rate_for_device(device);
    if (is_pluto_family(device)) {
        if (mode == SampleRateMode::PlutoF5oeoCompat && symbol_rate > 250000u) {
            return choose_even_sps_near_target_with_floor(symbol_rate, kPlutoCompatTargetSampleRate, min_rate, 4, 10);
        }
        const int min_sps = (symbol_rate <= kPlutoPreferredCleanMaxSymbolRate) ? 10 : 8;
        return choose_even_sps_with_floor(symbol_rate, min_rate, min_sps);
    }
    if (device == OutputDevice::Lime) {
        const int min_sps = (symbol_rate >= kLimePreferredFixedTenMinSymbolRate) ? 10 : 8;
        return choose_even_sps_with_floor(symbol_rate, min_rate, min_sps);
    }
    const int min_sps = (symbol_rate >= kHackrfPreferredCleanMinSymbolRate) ? 10 : 8;
    return choose_even_sps_with_floor(symbol_rate, min_rate, min_sps);
}

struct IqRingSample {
    float i = 0.0f;
    float q = 0.0f;
};

struct OutputMapperProfile {
    float peak_target = 0.9f;
    float rms_target = 0.24f;
    float full_scale = kHackrfFullScale;
};

struct OutputMapperState {
    float peak_env = 0.5f;
    float rms_env = 0.12f;
};

std::unique_ptr<IqRingSample[]> g_ring;
std::atomic<size_t> g_head{0};
std::atomic<size_t> g_tail{0};
OutputDevice g_output_device = OutputDevice::HackRF;
OutputMapperState g_hackrf_mapper_state{};
OutputMapperState g_pluto_mapper_state{};
OutputMapperState g_lime_mapper_state{};
GainMode g_gain_mode = GainMode::Fixed;
float g_fixed_tx_gain = 1.0f;
uint32_t g_hackrf_programmed_sample_rate = 0;
uint32_t g_hackrf_programmed_rf_bandwidth = 0;
uint64_t g_hackrf_programmed_frequency_hz = 0;
bool g_hackrf_readback_valid = false;
std::atomic<bool> g_stop_requested{false};
std::atomic<bool> g_producer_done{false};
std::atomic<bool> g_tx_started{false};

std::atomic<uint64_t> g_frames_processed{0};
std::atomic<uint64_t> g_payload_bytes_processed{0};
std::atomic<uint64_t> g_underflows{0};
std::atomic<uint64_t> g_overflows{0};

enum class TransportStopReason : uint32_t {
    None = 0,
    InputEof = 1,
    InputEofMidPacket = 2,
    SyncLost = 3,
    SyncRelockFailed = 4,
    InputReadError = 5,
    InputStalled = 6,
};

std::atomic<TransportStopReason> g_transport_stop_reason{TransportStopReason::None};
std::atomic<uint64_t> g_transport_relocks{0};
std::string g_transport_stop_detail;

const char* transport_stop_reason_string(TransportStopReason reason)
{
    switch (reason) {
    case TransportStopReason::None:
        return "none";
    case TransportStopReason::InputEof:
        return "input EOF before next BBFRAME payload";
    case TransportStopReason::InputEofMidPacket:
        return "input EOF while reassembling a TS packet";
    case TransportStopReason::SyncLost:
        return "transport sync lost at expected 188-byte boundary";
    case TransportStopReason::SyncRelockFailed:
        return "transport re-lock failed before input ended";
    case TransportStopReason::InputReadError:
        return "transport input read failed";
    case TransportStopReason::InputStalled:
        return "transport input stalled waiting for payload";
    }
    return "unknown";
}

#ifdef HAVE_HACKRF
bool g_hackrf_initialized = false;
hackrf_device* g_device = nullptr;
#endif
#ifdef HAVE_PLUTO
iio_context* g_iio_ctx = nullptr;
iio_device* g_pluto_phy = nullptr;
iio_device* g_pluto_tx = nullptr;
iio_channel* g_pluto_phy_tx = nullptr;
iio_channel* g_pluto_lo = nullptr;
iio_channel* g_pluto_tx_i = nullptr;
iio_channel* g_pluto_tx_q = nullptr;
iio_buffer* g_pluto_buffer = nullptr;
long long g_pluto_readback_frequency_hz = 0;
long long g_pluto_readback_rf_bandwidth_hz = 0;
long long g_pluto_readback_sample_rate = 0;
bool g_pluto_readback_valid = false;
#endif
#ifdef HAVE_SOAPYSDR
SoapySDR::Device* g_lime_device = nullptr;
SoapySDR::Stream* g_lime_stream = nullptr;
double g_lime_readback_sample_rate = 0.0;
double g_lime_readback_rf_bandwidth = 0.0;
double g_lime_readback_frequency_hz = 0.0;
double g_lime_readback_gain_db = 0.0;
bool g_lime_readback_valid = false;
#endif
std::chrono::steady_clock::time_point g_start_time;

float g_dc_i = 0.0f;
float g_dc_q = 0.0f;

size_t ring_fill_samples()
{
    const size_t head = g_head.load(std::memory_order_acquire);
    const size_t tail = g_tail.load(std::memory_order_acquire);
    return head >= tail ? (head - tail) : (kRingIqCapacity - tail + head);
}

size_t ring_free_samples()
{
    return (kRingIqCapacity - 1u) - ring_fill_samples();
}

OutputMapperProfile mapper_profile_for_device(OutputDevice device)
{
    if (is_pluto_family(device)) {
        return OutputMapperProfile{0.92f, 0.30f, kPlutoFullScale};
    }
    if (device == OutputDevice::Lime) {
        return OutputMapperProfile{0.92f, 0.30f, kLimeFullScale};
    }
    return OutputMapperProfile{0.90f, 0.24f, kHackrfFullScale};
}

float mapper_gain_for_sample(OutputMapperState& state, const IqRingSample& sample, const OutputMapperProfile& profile)
{
    if (g_gain_mode == GainMode::Fixed) {
        return std::clamp(g_fixed_tx_gain, 0.05f, 16.0f);
    }

    const float peak = std::max(std::fabs(sample.i), std::fabs(sample.q));
    const float power = 0.5f * ((sample.i * sample.i) + (sample.q * sample.q));

    state.peak_env = std::max(peak, state.peak_env * 0.9992f);
    state.rms_env = (state.rms_env * 0.9992f) + (power * 0.0008f);

    const float safe_peak = std::max(state.peak_env, 1.0e-4f);
    const float safe_rms = std::sqrt(std::max(state.rms_env, 1.0e-6f));

    const float peak_gain = profile.peak_target / safe_peak;
    const float rms_gain = profile.rms_target / safe_rms;
    return std::clamp(std::min(peak_gain, rms_gain), 0.25f, 12.0f);
}

[[maybe_unused]] void map_sample_to_pluto(const IqRingSample& sample, OutputMapperState& state, int16_t& out_i, int16_t& out_q)
{
    const OutputMapperProfile profile = mapper_profile_for_device(OutputDevice::PlutoStock);
    const float gain = mapper_gain_for_sample(state, sample, profile);

    const int16_t raw_i = static_cast<int16_t>(
        std::lrint(std::clamp(sample.i * gain, -1.0f, 1.0f) * profile.full_scale)
    );
    const int16_t raw_q = static_cast<int16_t>(
        std::lrint(std::clamp(sample.q * gain, -1.0f, 1.0f) * profile.full_scale)
    );

    // Pluto / AD9361 TX samples travel in a signed 16-bit libiio container, but only the
    // upper 12 bits are significant on the device side. Left-align the signed 12-bit values
    // so the DAC sees full-scale symbols instead of a 1/16 amplitude signal.
    out_i = static_cast<int16_t>(raw_i << kPlutoSampleLeftShift);
    out_q = static_cast<int16_t>(raw_q << kPlutoSampleLeftShift);
}

[[maybe_unused]] void map_sample_to_lime(const IqRingSample& sample, OutputMapperState& state, int16_t& out_i, int16_t& out_q)
{
    const OutputMapperProfile profile = mapper_profile_for_device(OutputDevice::Lime);
    const float gain = mapper_gain_for_sample(state, sample, profile);
    out_i = static_cast<int16_t>(
        std::lrint(std::clamp(sample.i * gain, -1.0f, 1.0f) * profile.full_scale)
    );
    out_q = static_cast<int16_t>(
        std::lrint(std::clamp(sample.q * gain, -1.0f, 1.0f) * profile.full_scale)
    );
}

bool set_thread_priority(int priority)
{
    sched_param param{};
    param.sched_priority = priority;
    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &param) == 0) {
        return true;
    }
    if (pthread_setschedparam(pthread_self(), SCHED_RR, &param) == 0) {
        return true;
    }
    return false;
}

void request_stop(int)
{
    g_stop_requested.store(true, std::memory_order_release);
}

uint32_t gold_to_root(uint32_t goldcode)
{
    uint32_t x = 1u;
    for (uint32_t g = 0; g < goldcode; ++g) {
        x = (((x ^ (x >> 7)) & 1u) << 17) | (x >> 1);
    }
    return x;
}

void push_iq_sample(float i, float q)
{
    const float power = (i * i) + (q * q);
    if (power > 0.95f) {
        const float scale = std::sqrt(0.95f / power);
        i *= scale;
        q *= scale;
    }

    g_dc_i = (0.9995f * g_dc_i) + (0.0005f * i);
    g_dc_q = (0.9995f * g_dc_q) + (0.0005f * q);
    i -= g_dc_i;
    q -= g_dc_q;

    if (ring_free_samples() < 1u) {
        g_overflows.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    const size_t head = g_head.load(std::memory_order_acquire);
    const size_t next_head = (head + 1u) % kRingIqCapacity;

    g_ring[head] = IqRingSample{i, q};
    g_head.store(next_head, std::memory_order_release);
}

class RRCInterpolator {
public:
    RRCInterpolator(int sps, std::vector<float> taps)
        : sps_(sps), taps_(std::move(taps)), hist_i_(taps_.size(), 0.0f), hist_q_(taps_.size(), 0.0f)
    {
    }

    void push_symbol(const cf& symbol)
    {
        for (int phase = 0; phase < sps_; ++phase) {
            newest_ = (newest_ + 1u) % taps_.size();
            hist_i_[newest_] = (phase == 0) ? symbol.real() : 0.0f;
            hist_q_[newest_] = (phase == 0) ? symbol.imag() : 0.0f;

            float out_i = 0.0f;
            float out_q = 0.0f;
            size_t idx = (newest_ + 1u) % taps_.size();
            for (size_t k = 0; k < taps_.size(); ++k) {
                out_i += taps_[k] * hist_i_[idx];
                out_q += taps_[k] * hist_q_[idx];
                idx++;
                if (idx == taps_.size()) {
                    idx = 0;
                }
            }
            push_iq_sample(out_i, out_q);
        }
    }

private:
    int sps_;
    std::vector<float> taps_;
    std::vector<float> hist_i_;
    std::vector<float> hist_q_;
    size_t newest_ = 0;
};

class TransportStreamAdapter {
public:
    TransportStreamAdapter()
        : input_stall_timeout_ms_(read_input_stall_timeout_ms())
    {
        build_crc_table();
    }

    int syncd_bits() const
    {
        return at_packet_boundary_ ? 0 : ((187 - packet_offset_) * 8);
    }

    TransportStopReason stop_reason() const
    {
        return stop_reason_;
    }

    const std::string& stop_detail() const
    {
        return stop_detail_;
    }

    bool fill_payload(std::vector<uint8_t>& payload)
    {
        for (uint8_t& byte : payload) {
            if (!next_transport_byte(byte)) {
                return false;
            }
        }
        return true;
    }

private:
    enum class TsLockState : uint8_t {
        SearchSync = 0,
        InPacket = 1,
        LostSync = 2,
    };

    std::vector<uint8_t> buffer_;
    size_t offset_ = 0;
    TsLockState state_ = TsLockState::SearchSync;
    bool at_packet_boundary_ = true;
    int packet_offset_ = 0;
    uint8_t crc_ = 0;
    std::array<uint8_t, 256> crc_table_{};
    TransportStopReason stop_reason_ = TransportStopReason::None;
    std::string stop_detail_;
    bool has_locked_once_ = false;
    int input_stall_timeout_ms_ = 0;

    void build_crc_table()
    {
        for (int i = 0; i < 256; ++i) {
            int r = i;
            int crc = 0;
            for (int j = 7; j >= 0; --j) {
                if (((r >> j) & 1) ^ ((crc & 0x80) ? 1 : 0)) {
                    crc = ((crc << 1) ^ 0xD5) & 0xff;
                } else {
                    crc = (crc << 1) & 0xff;
                }
            }
            crc_table_[static_cast<size_t>(i)] = static_cast<uint8_t>(crc & 0xff);
        }
    }

    static int read_input_stall_timeout_ms()
    {
        const char* value = std::getenv("DATV_INPUT_STALL_MS");
        if (value == nullptr || *value == '\0') {
            return 0;
        }
        char* end = nullptr;
        const long parsed = std::strtol(value, &end, 10);
        if (end == value || (end != nullptr && *end != '\0') || parsed <= 0L) {
            return 0;
        }
        return static_cast<int>(std::min<long>(parsed, 600000L));
    }

    void set_stop_reason(TransportStopReason reason, const std::string& detail)
    {
        if (stop_reason_ == TransportStopReason::None) {
            stop_reason_ = reason;
            stop_detail_ = detail;
        }
    }

    void reset_packet_state_for_search()
    {
        at_packet_boundary_ = true;
        packet_offset_ = 0;
        crc_ = 0;
        state_ = TsLockState::SearchSync;
    }

    void begin_packet_after_sync(uint8_t& out)
    {
        // DVB-S2 mode adaptation carries the 188-byte MPEG-TS packet as 187 payload bytes plus
        // the 8-bit CRC over those 187 bytes. The 0x47 sync byte is therefore consumed for packet
        // alignment here and replaced on output by the running CRC byte for the previous packet.
        out = crc_;
        crc_ = 0;
        at_packet_boundary_ = false;
        packet_offset_ = 0;
        state_ = TsLockState::InPacket;
    }

    bool refill()
    {
        if (offset_ > 0) {
            buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<long>(offset_));
            offset_ = 0;
        }

        if (input_stall_timeout_ms_ > 0) {
            pollfd pfd{};
            pfd.fd = STDIN_FILENO;
            pfd.events = POLLIN | POLLHUP | POLLERR;
            int poll_result = -1;
            do {
                poll_result = ::poll(&pfd, 1, input_stall_timeout_ms_);
            } while (poll_result < 0 && errno == EINTR && !g_stop_requested.load(std::memory_order_acquire));

            if (poll_result == 0) {
                set_stop_reason(TransportStopReason::InputStalled,
                                "transport input stalled waiting for payload for " + std::to_string(input_stall_timeout_ms_) + " ms");
                return false;
            }
            if (poll_result < 0) {
                set_stop_reason(TransportStopReason::InputReadError,
                                std::string("stdin poll failed: ") + std::strerror(errno));
                return false;
            }
        }

        uint8_t temp[8192];
        ssize_t nread = -1;
        do {
            nread = ::read(STDIN_FILENO, temp, sizeof(temp));
        } while (nread < 0 && errno == EINTR && !g_stop_requested.load(std::memory_order_acquire));

        if (nread == 0) {
            const bool mid_packet = !at_packet_boundary_;
            set_stop_reason(mid_packet ? TransportStopReason::InputEofMidPacket : TransportStopReason::InputEof,
                            mid_packet ? "stdin reached EOF before the current 188-byte TS packet was complete"
                                       : "stdin reached EOF while waiting for the next BBFRAME payload");
            return false;
        }
        if (nread < 0) {
            set_stop_reason(TransportStopReason::InputReadError,
                            std::string("stdin read failed: ") + std::strerror(errno));
            return false;
        }
        buffer_.insert(buffer_.end(), temp, temp + nread);
        return true;
    }

    bool read_raw_byte(uint8_t& out)
    {
        while (offset_ >= buffer_.size()) {
            if (!refill()) {
                return false;
            }
        }
        out = buffer_[offset_++];
        if (offset_ > 4096 && offset_ * 2 > buffer_.size()) {
            buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<long>(offset_));
            offset_ = 0;
        }
        return true;
    }

    bool align_to_sync()
    {
        uint8_t byte = 0;
        while (!g_stop_requested.load(std::memory_order_acquire)) {
            if (!read_raw_byte(byte)) {
                if (stop_reason_ == TransportStopReason::SyncLost) {
                    stop_reason_ = TransportStopReason::SyncRelockFailed;
                    stop_detail_ = "sync was lost and no new 0x47 sync byte was found before input ended";
                }
                return false;
            }
            if (byte == 0x47) {
                if (has_locked_once_) {
                    g_transport_relocks.fetch_add(1, std::memory_order_relaxed);
                } else {
                    has_locked_once_ = true;
                }
                return true;
            }
        }
        return false;
    }

    static std::string format_hex_byte(uint8_t byte)
    {
        char buf[5];
        std::snprintf(buf, sizeof(buf), "%02X", byte);
        return std::string(buf);
    }

    bool start_or_relock_packet(uint8_t& out)
    {
        if (!align_to_sync()) {
            return false;
        }
        begin_packet_after_sync(out);
        return true;
    }

    bool next_transport_byte(uint8_t& out)
    {
        if (at_packet_boundary_) {
            if (state_ == TsLockState::SearchSync || state_ == TsLockState::LostSync || !has_locked_once_) {
                return start_or_relock_packet(out);
            }

            uint8_t sync = 0;
            if (!read_raw_byte(sync)) {
                return false;
            }
            if (sync != 0x47) {
                set_stop_reason(TransportStopReason::SyncLost,
                                "expected 0x47 at the next 188-byte TS boundary but received 0x" + format_hex_byte(sync));
                state_ = TsLockState::LostSync;
                reset_packet_state_for_search();
                state_ = TsLockState::LostSync;
                return start_or_relock_packet(out);
            }
            begin_packet_after_sync(out);
            return true;
        }

        uint8_t byte = 0;
        if (!read_raw_byte(byte)) {
            return false;
        }

        out = byte;
        crc_ = crc_table_[static_cast<uint8_t>(byte ^ crc_)];
        ++packet_offset_;
        if (packet_offset_ >= 187) {
            at_packet_boundary_ = true;
            packet_offset_ = 0;
        }
        state_ = TsLockState::InPacket;
        return true;
    }
};

size_t estimate_frame_iq_samples(const DVB_Params& cfg, bool pilots, int sps)
{
    return static_cast<size_t>((cfg.plframe_symbols(pilots) + kLowSrNumTaps + 8) * sps);
}

void producer_thread(DVB_Params cfg, RollOff rolloff, bool pilots, uint32_t root_code, int sps, std::vector<float> taps)
{
    set_thread_affinity(3);
    set_thread_priority(70);

    TransportStreamAdapter ts;
    RRCInterpolator shaper(sps, std::move(taps));

    std::vector<uint8_t> payload(static_cast<size_t>(cfg.payload_bytes()), 0u);
    std::vector<uint8_t> frame_bits;
    std::vector<cf> plframe;

    const size_t needed_free = estimate_frame_iq_samples(cfg, pilots, sps);

    while (!g_stop_requested.load(std::memory_order_acquire)) {
        while (ring_free_samples() < needed_free && !g_stop_requested.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        if (g_stop_requested.load(std::memory_order_acquire)) {
            break;
        }

        const int syncd = ts.syncd_bits();
        if (!ts.fill_payload(payload)) {
            g_transport_stop_reason.store(ts.stop_reason(), std::memory_order_release);
            g_transport_stop_detail = ts.stop_detail();
            break;
        }

        build_bbframe(frame_bits, payload, cfg, rolloff, syncd);
        bb_randomize(frame_bits, cfg.kbch);
        bch_encode(frame_bits, cfg);
        ldpc_encode(frame_bits, cfg);
        assemble_plframe(plframe, cfg, frame_bits, pilots, root_code);

        for (const cf& sym : plframe) {
            shaper.push_symbol(sym);
        }

        g_frames_processed.fetch_add(1, std::memory_order_relaxed);
        g_payload_bytes_processed.fetch_add(static_cast<uint64_t>(payload.size()), std::memory_order_relaxed);
    }

    g_producer_done.store(true, std::memory_order_release);
}

#ifdef HAVE_HACKRF
int tx_callback(hackrf_transfer* transfer)
{
    const size_t len = static_cast<size_t>(transfer->valid_length);
    if (len == 0u) {
        return 0;
    }

    const size_t needed_samples = len / 2u;
    const size_t fill = ring_fill_samples();
    const size_t samples_to_consume = std::min(fill, needed_samples);

    if (samples_to_consume < needed_samples) {
        std::memset(transfer->buffer, 0, len);
        if (!g_producer_done.load(std::memory_order_acquire)) {
            g_underflows.fetch_add(1, std::memory_order_relaxed);
        }
    }

    size_t tail = g_tail.load(std::memory_order_acquire);
    const OutputMapperProfile profile = mapper_profile_for_device(OutputDevice::HackRF);
    for (size_t n = 0; n < samples_to_consume; ++n) {
        const IqRingSample sample = g_ring[tail];
        const float gain = mapper_gain_for_sample(g_hackrf_mapper_state, sample, profile);
        transfer->buffer[(2u * n) + 0u] = static_cast<uint8_t>(static_cast<int8_t>(
            std::lrint(std::clamp(sample.i * gain, -1.0f, 1.0f) * profile.full_scale)));
        transfer->buffer[(2u * n) + 1u] = static_cast<uint8_t>(static_cast<int8_t>(
            std::lrint(std::clamp(sample.q * gain, -1.0f, 1.0f) * profile.full_scale)));
        tail = (tail + 1u) % kRingIqCapacity;
    }
    if (len & 1u) {
        transfer->buffer[len - 1u] = 0;
    }
    g_tail.store(tail, std::memory_order_release);
    return 0;
}

void cleanup_hackrf()
{
    g_hackrf_programmed_sample_rate = 0;
    g_hackrf_programmed_rf_bandwidth = 0;
    g_hackrf_programmed_frequency_hz = 0;
    g_hackrf_readback_valid = false;
    if (g_device != nullptr) {
        if (g_tx_started.load(std::memory_order_acquire)) {
            hackrf_stop_tx(g_device);
            g_tx_started.store(false, std::memory_order_release);
        }
        hackrf_close(g_device);
        g_device = nullptr;
    }
    if (g_hackrf_initialized) {
        hackrf_exit();
        g_hackrf_initialized = false;
    }
}
#else
void cleanup_hackrf() {}
#endif


#ifdef HAVE_PLUTO
bool pluto_write_attr_longlong(iio_channel* ch, const char* attr, long long value)
{
    return ch != nullptr && iio_channel_attr_write_longlong(ch, attr, value) >= 0;
}

bool pluto_write_attr_longlong_retry(iio_channel* ch, const char* attr, long long value, int attempts, int delay_ms)
{
    for (int attempt = 1; attempt <= std::max(attempts, 1); ++attempt) {
        if (pluto_write_attr_longlong(ch, attr, value)) {
            return true;
        }
        if (attempt < attempts) {
            std::this_thread::sleep_for(std::chrono::milliseconds(std::max(delay_ms, 0)));
        }
    }
    return false;
}

bool pluto_write_attr_string(iio_channel* ch, const char* attr, const std::string& value)
{
    return ch != nullptr && iio_channel_attr_write(ch, attr, value.c_str()) >= 0;
}

bool pluto_write_attr_string_retry(iio_channel* ch, const char* attr, const std::string& value, int attempts, int delay_ms)
{
    for (int attempt = 1; attempt <= std::max(attempts, 1); ++attempt) {
        if (pluto_write_attr_string(ch, attr, value)) {
            return true;
        }
        if (attempt < attempts) {
            std::this_thread::sleep_for(std::chrono::milliseconds(std::max(delay_ms, 0)));
        }
    }
    return false;
}

bool pluto_read_attr_longlong(iio_channel* ch, const char* attr, long long& value)
{
    if (ch == nullptr) {
        return false;
    }
    return iio_channel_attr_read_longlong(ch, attr, &value) >= 0;
}

bool pluto_wait_for_attr_longlong(iio_channel* ch, const char* attr, long long expected, int attempts, int delay_ms, long long* last_value = nullptr, long long tolerance = 0)
{
    long long value = 0;
    for (int attempt = 1; attempt <= std::max(attempts, 1); ++attempt) {
        if (pluto_read_attr_longlong(ch, attr, value)) {
            if (last_value != nullptr) {
                *last_value = value;
            }
            if (std::llabs(value - expected) <= std::max<long long>(0, tolerance)) {
                return true;
            }
        }
        if (attempt < attempts) {
            std::this_thread::sleep_for(std::chrono::milliseconds(std::max(delay_ms, 0)));
        }
    }
    return false;
}

void pluto_disable_dds_tones()
{
    if (!g_pluto_tx) return;

    for (int idx = 0; idx < 8; ++idx) {
        const std::string name = "altvoltage" + std::to_string(idx);
        iio_channel* dds = iio_device_find_channel(g_pluto_tx, name.c_str(), true);
        if (!dds) {
            continue;
        }

        pluto_write_attr_longlong_retry(dds, "raw", 0, 2, 15);
        pluto_write_attr_string_retry(dds, "scale", "0", 2, 15);
        pluto_write_attr_longlong_retry(dds, "frequency", 0, 1, 0);
        pluto_write_attr_longlong_retry(dds, "phase", 0, 1, 0);
    }
}

void pluto_prime_zero_buffers(int pushes = 3)
{
    if (!g_pluto_buffer) return;

    auto* start = static_cast<char*>(iio_buffer_start(g_pluto_buffer));
    auto* end = static_cast<char*>(iio_buffer_end(g_pluto_buffer));
    const size_t bytes = static_cast<size_t>(end - start);
    std::memset(start, 0, bytes);

    for (int i = 0; i < std::max(pushes, 1); ++i) {
        if (iio_buffer_push(g_pluto_buffer) < 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(12));
    }
}

double clamp_pluto_attenuation_db(double value)
{
    if (std::isnan(value) || !std::isfinite(value)) return 0.0;
    value = std::clamp(value, 0.0, 89.75);
    return std::round(value * 4.0) / 4.0;
}


void cleanup_pluto()
{
    if (g_pluto_buffer) { iio_buffer_destroy(g_pluto_buffer); g_pluto_buffer = nullptr; }
    if (g_pluto_tx_i) { iio_channel_disable(g_pluto_tx_i); g_pluto_tx_i = nullptr; }
    if (g_pluto_tx_q) { iio_channel_disable(g_pluto_tx_q); g_pluto_tx_q = nullptr; }
    g_pluto_phy_tx = nullptr;
    g_pluto_lo = nullptr;
    g_pluto_phy = nullptr;
    g_pluto_tx = nullptr;
    g_pluto_readback_frequency_hz = 0;
    g_pluto_readback_rf_bandwidth_hz = 0;
    g_pluto_readback_sample_rate = 0;
    g_pluto_readback_valid = false;
    if (g_iio_ctx) { iio_context_destroy(g_iio_ctx); g_iio_ctx = nullptr; }
}

void setup_pluto(uint64_t frequency_hz, uint32_t sample_rate, uint32_t bb_bw, double tx_attenuation_db, const std::string& uri)
{
    const double attenuation = clamp_pluto_attenuation_db(tx_attenuation_db);
    char atten_buf[32];
    std::snprintf(atten_buf, sizeof(atten_buf), "%.2f", -attenuation);

    std::string last_error = "Failed to create Pluto libiio context for " + uri;
    for (int setup_attempt = 1; setup_attempt <= 4; ++setup_attempt) {
        cleanup_pluto();

        g_iio_ctx = iio_create_context_from_uri(uri.c_str());
        if (!g_iio_ctx) {
            last_error = "Failed to create Pluto libiio context for " + uri;
        } else {
            g_pluto_phy = iio_context_find_device(g_iio_ctx, "ad9361-phy");
            g_pluto_tx = iio_context_find_device(g_iio_ctx, "cf-ad9361-dds-core-lpc");
            if (!g_pluto_phy || !g_pluto_tx) {
                last_error = "PlutoSDR TX devices not found via libiio";
            } else {
                g_pluto_phy_tx = iio_device_find_channel(g_pluto_phy, "voltage0", true);
                g_pluto_lo = iio_device_find_channel(g_pluto_phy, "altvoltage1", true);
                g_pluto_tx_i = iio_device_find_channel(g_pluto_tx, "voltage0", true);
                g_pluto_tx_q = iio_device_find_channel(g_pluto_tx, "voltage1", true);
                if (!g_pluto_phy_tx || !g_pluto_lo || !g_pluto_tx_i || !g_pluto_tx_q) {
                    last_error = "Required Pluto channels not found";
                } else if (!pluto_write_attr_longlong_retry(g_pluto_lo, "frequency", static_cast<long long>(frequency_hz), 4, 120)) {
                    last_error = "Failed to set Pluto LO frequency";
                } else if (!pluto_write_attr_longlong_retry(g_pluto_phy_tx, "rf_bandwidth", static_cast<long long>(bb_bw), 4, 120)) {
                    last_error = "Failed to set Pluto rf_bandwidth";
                } else if (!pluto_write_attr_longlong_retry(g_pluto_phy_tx, "sampling_frequency", static_cast<long long>(sample_rate), 8, 300)) {
                    last_error = "Failed to set Pluto sampling_frequency to " + std::to_string(sample_rate) + " S/s";
                } else if (!pluto_write_attr_string(g_pluto_phy_tx, "hardwaregain", atten_buf)) {
                    last_error = "Failed to set Pluto TX attenuation/hardwaregain";
                } else {
                    long long actual_frequency_hz = 0;
                    long long actual_rf_bandwidth_hz = 0;
                    long long actual_sample_rate = 0;
                    const bool sample_rate_ok = pluto_wait_for_attr_longlong(
                        g_pluto_phy_tx,
                        "sampling_frequency",
                        static_cast<long long>(sample_rate),
                        10,
                        120,
                        &actual_sample_rate,
                        kPlutoSampleRateReadbackToleranceHz
                    );
                    pluto_read_attr_longlong(g_pluto_lo, "frequency", actual_frequency_hz);
                    pluto_read_attr_longlong(g_pluto_phy_tx, "rf_bandwidth", actual_rf_bandwidth_hz);
                    g_pluto_readback_frequency_hz = actual_frequency_hz;
                    g_pluto_readback_rf_bandwidth_hz = actual_rf_bandwidth_hz;
                    g_pluto_readback_sample_rate = actual_sample_rate;
                    g_pluto_readback_valid = sample_rate_ok;
                    if (!sample_rate_ok) {
                        last_error = "Pluto sampling_frequency read-back mismatch: requested " + std::to_string(sample_rate) +
                                     " S/s but device reports " + std::to_string(actual_sample_rate) +
                                     " S/s (allowed read-back tolerance ±" + std::to_string(kPlutoSampleRateReadbackToleranceHz) + " Hz)";
                    } else {
                        pluto_disable_dds_tones();
                        iio_channel_enable(g_pluto_tx_i);
                        iio_channel_enable(g_pluto_tx_q);
                        g_pluto_buffer = iio_device_create_buffer(g_pluto_tx, choose_pluto_buffer_samples(sample_rate), false);
                        if (g_pluto_buffer) {
                            pluto_prime_zero_buffers();
                            return;
                        }
                        last_error = "Failed to create Pluto TX buffer";
                    }
                }
            }
        }

        cleanup_pluto();
        if (setup_attempt < 4) {
            std::this_thread::sleep_for(std::chrono::milliseconds(350 + (setup_attempt * 200)));
        }
    }

    throw std::runtime_error(
        last_error + " (check libiio access, device reachability, or the requested sample rate)"
    );
}

void pluto_tx_loop()
{
    set_thread_affinity(2);
    set_thread_priority(60);
    while (!g_stop_requested.load(std::memory_order_acquire)) {
        const bool producer_done = g_producer_done.load(std::memory_order_acquire);
        const size_t fill = ring_fill_samples();
        if (producer_done && fill == 0u) break;

        if (fill == 0u) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            continue;
        }

        auto* raw_start = static_cast<char*>(iio_buffer_start(g_pluto_buffer));
        auto* raw_end = static_cast<char*>(iio_buffer_end(g_pluto_buffer));
        auto* ptr = static_cast<char*>(iio_buffer_first(g_pluto_buffer, g_pluto_tx_i));
        const ptrdiff_t step = iio_buffer_step(g_pluto_buffer);
        if (step < static_cast<ptrdiff_t>(sizeof(int16_t) * 2u)) {
            throw std::runtime_error("Pluto TX buffer step is smaller than one IQ sample");
        }

        const size_t max_bytes = static_cast<size_t>(raw_end - raw_start);
        std::memset(raw_start, 0, max_bytes);

        size_t max_samples = 0u;
        for (char* probe = ptr; probe < raw_end; probe += step) {
            ++max_samples;
        }
        const size_t samples_to_write = std::min(fill, max_samples);
        size_t tail = g_tail.load(std::memory_order_acquire);

        for (size_t n = 0; n < samples_to_write && ptr < raw_end; ++n, ptr += step) {
            const IqRingSample sample = g_ring[tail];
            int16_t out_i = 0;
            int16_t out_q = 0;
            map_sample_to_pluto(sample, g_pluto_mapper_state, out_i, out_q);
            auto* iq = reinterpret_cast<int16_t*>(ptr);
            iq[0] = out_i;
            iq[1] = out_q;
            tail = (tail + 1u) % kRingIqCapacity;
        }

        if (samples_to_write == 0u) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            continue;
        }
        g_tail.store(tail, std::memory_order_release);
        if (iio_buffer_push(g_pluto_buffer) < 0) throw std::runtime_error("Pluto TX buffer push failed");
    }
}
#else
void cleanup_pluto() {}
#endif

#ifdef HAVE_SOAPYSDR
double clamp_lime_gain_db(double value)
{
    return std::clamp(value, 0.0, 64.0);
}

std::string normalise_lime_device_args(const std::string& value)
{
    return value.empty() ? std::string("driver=lime") : value;
}

void cleanup_lime()
{
    if (g_lime_device != nullptr) {
        if (g_lime_stream != nullptr) {
            try { g_lime_device->deactivateStream(g_lime_stream, 0, 0); } catch (...) {}
            try { g_lime_device->closeStream(g_lime_stream); } catch (...) {}
            g_lime_stream = nullptr;
        }
        SoapySDR::Device::unmake(g_lime_device);
        g_lime_device = nullptr;
    }
    g_lime_readback_sample_rate = 0.0;
    g_lime_readback_rf_bandwidth = 0.0;
    g_lime_readback_frequency_hz = 0.0;
    g_lime_readback_gain_db = 0.0;
    g_lime_readback_valid = false;
}

void setup_lime(uint64_t frequency_hz, uint32_t sample_rate, uint32_t bb_bw, double tx_gain_db, const std::string& device_args)
{
    cleanup_lime();
    const std::string args = normalise_lime_device_args(device_args);

    std::vector<SoapySDR::Kwargs> devices = SoapySDR::Device::enumerate(args);
    if (devices.empty() && args != "driver=lime") {
        devices = SoapySDR::Device::enumerate("driver=lime");
    }
    if (devices.empty()) {
        throw std::runtime_error("No LimeSDR devices detected via SoapySDR (check SoapySDR + SoapyLMS7 + USB permissions)");
    }

    g_lime_device = SoapySDR::Device::make(args);
    if (g_lime_device == nullptr) {
        throw std::runtime_error("SoapySDR could not create a LimeSDR device for args: " + args);
    }

    if (g_lime_device->getNumChannels(SOAPY_SDR_TX) < 1) {
        cleanup_lime();
        throw std::runtime_error("Selected LimeSDR device does not expose a TX channel");
    }

    const double gain_db = clamp_lime_gain_db(tx_gain_db);
    g_lime_readback_sample_rate = 0.0;
    g_lime_readback_rf_bandwidth = 0.0;
    g_lime_readback_frequency_hz = 0.0;
    g_lime_readback_gain_db = 0.0;
    g_lime_readback_valid = false;
    g_lime_device->setSampleRate(SOAPY_SDR_TX, 0, static_cast<double>(sample_rate));
    g_lime_device->setFrequency(SOAPY_SDR_TX, 0, static_cast<double>(frequency_hz));
    try { g_lime_device->setBandwidth(SOAPY_SDR_TX, 0, static_cast<double>(bb_bw)); } catch (...) {}
    try { g_lime_device->setGain(SOAPY_SDR_TX, 0, gain_db); } catch (...) {}
    try { g_lime_device->writeSetting("CALIBRATE", "TX"); } catch (...) {}

    g_lime_stream = g_lime_device->setupStream(SOAPY_SDR_TX, SOAPY_SDR_CS16);
    if (g_lime_stream == nullptr) {
        cleanup_lime();
        throw std::runtime_error("SoapySDR setupStream failed for LimeSDR TX");
    }

    const int activate_status = g_lime_device->activateStream(g_lime_stream, 0, 0, 0);
    if (activate_status != 0) {
        const std::string detail = SoapySDR::errToStr(activate_status);
        cleanup_lime();
        throw std::runtime_error("LimeSDR stream activation failed: " + detail);
    }

    try { g_lime_readback_sample_rate = g_lime_device->getSampleRate(SOAPY_SDR_TX, 0); } catch (...) {}
    try { g_lime_readback_rf_bandwidth = g_lime_device->getBandwidth(SOAPY_SDR_TX, 0); } catch (...) {}
    try { g_lime_readback_frequency_hz = g_lime_device->getFrequency(SOAPY_SDR_TX, 0); } catch (...) {}
    try { g_lime_readback_gain_db = g_lime_device->getGain(SOAPY_SDR_TX, 0); } catch (...) {}
    g_lime_readback_valid = g_lime_readback_sample_rate > 0.0 || g_lime_readback_frequency_hz > 0.0;
}

void lime_tx_loop()
{
    std::vector<int16_t> interleaved(16384u * 2u, 0);
    while (!g_stop_requested.load(std::memory_order_acquire)) {
        const bool producer_done = g_producer_done.load(std::memory_order_acquire);
        const size_t fill = ring_fill_samples();
        if (producer_done && fill == 0u) break;

        if (fill == 0u) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            continue;
        }

        const size_t samples_to_write = std::min(fill, interleaved.size() / 2u);
        size_t tail = g_tail.load(std::memory_order_acquire);
        for (size_t n = 0; n < samples_to_write; ++n) {
            const IqRingSample sample = g_ring[tail];
            map_sample_to_lime(sample, g_lime_mapper_state, interleaved[2u * n], interleaved[(2u * n) + 1u]);
            tail = (tail + 1u) % kRingIqCapacity;
        }

        void* buffs[] = {interleaved.data()};
        int flags = 0;
        long long time_ns = 0;
        const int written = g_lime_device->writeStream(g_lime_stream, buffs, samples_to_write, flags, time_ns, 100000);
        if (written < 0) {
            throw std::runtime_error(std::string("LimeSDR writeStream failed: ") + SoapySDR::errToStr(written));
        }
        if (written == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        const size_t committed = static_cast<size_t>(written);
        const size_t current_tail = g_tail.load(std::memory_order_acquire);
        g_tail.store((current_tail + committed) % kRingIqCapacity, std::memory_order_release);
    }
}
#else
std::string normalise_lime_device_args(const std::string& value)
{
    return value.empty() ? std::string("driver=lime") : value;
}
void cleanup_lime() {}
#endif

} // namespace

int main(int argc, char** argv)
{
    if (argc < 6) {
        std::cerr << "Usage: " << argv[0]
                  << " <MODCOD> <FREQ_HZ> <SYMBOL_RATE> <TX_LEVEL> <AMP_ENABLE> [ROLLOFF] [PILOTS] [GOLDCODE] [DEVICE] [DEVICE_ARG]"
                  << std::endl;
        std::cerr << "Example MODCODs: QPSK_1/2_S, QPSK_3/4_N, 8PSK_3/4_S, 8PSK_5/6_N" << std::endl;
        std::cerr << "Device examples: hackrf, adalmpluto, plutodvb, f5oeo, limesdrmini" << std::endl;
        return 1;
    }

    std::signal(SIGINT, request_stop);
    std::signal(SIGTERM, request_stop);
    std::signal(SIGPIPE, SIG_IGN);

    try {
        const DVB_Params cfg = get_fec_config(argv[1]);
        const uint64_t frequency_hz = std::stoull(argv[2]);
        const uint32_t symbol_rate = static_cast<uint32_t>(std::stoul(argv[3]));
        const double tx_level = std::stod(argv[4]);
        const uint8_t amp_enable = static_cast<uint8_t>(std::stoul(argv[5]) ? 1u : 0u);
        const RollOff rolloff = (argc >= 7) ? parse_rolloff(argv[6]) : RollOff::RO_20;
        const bool pilots = (argc >= 8) ? (std::stoi(argv[7]) != 0) : true;
        const uint32_t gold_code = (argc >= 9) ? static_cast<uint32_t>(std::stoul(argv[8])) : 0u;
        const OutputDevice output_device = (argc >= 10) ? parse_output_device(argv[9]) : OutputDevice::HackRF;
        const std::string device_arg = (argc >= 11) ? std::string(argv[10]) : std::string();
        const std::string pluto_uri = normalise_pluto_uri(device_arg);
        const std::string lime_device_args = normalise_lime_device_args(device_arg);
        const uint32_t root_code = gold_to_root(gold_code);

        if (symbol_rate == 0u) {
            throw std::runtime_error("Symbol rate must be greater than zero");
        }

        const SampleRateMode sample_rate_mode = parse_sample_rate_mode_from_env();
        const bool low_sr_stabilize = parse_low_sr_stabilize_from_env();
        const GainMode gain_mode = parse_gain_mode_from_env();
        const float fixed_gain = parse_fixed_gain_from_env();
        const int sps = choose_samples_per_symbol(symbol_rate, output_device, sample_rate_mode);
        const uint32_t sample_rate = symbol_rate * static_cast<uint32_t>(sps);
        const uint32_t bb_bw = choose_output_rf_bandwidth(output_device, symbol_rate, rolloff, sample_rate);
        const int rrc_num_taps = choose_rrc_num_taps(symbol_rate, low_sr_stabilize);

        std::vector<float> rrc_taps;
        generate_rrc_taps(rrc_taps, rrc_num_taps, sps, rolloff_to_alpha(rolloff));

        g_ring = std::make_unique<IqRingSample[]>(kRingIqCapacity);
        std::fill_n(g_ring.get(), kRingIqCapacity, IqRingSample{});
        g_output_device = output_device;
        g_hackrf_mapper_state = OutputMapperState{};
        g_pluto_mapper_state = OutputMapperState{};
        g_lime_mapper_state = OutputMapperState{};
        g_gain_mode = gain_mode;
        g_fixed_tx_gain = fixed_gain;
        g_hackrf_programmed_sample_rate = 0;
        g_hackrf_programmed_rf_bandwidth = 0;
        g_hackrf_programmed_frequency_hz = 0;
        g_hackrf_readback_valid = false;
#ifdef HAVE_HACKRF
        g_hackrf_initialized = false;
#endif
        g_head.store(0, std::memory_order_release);
        g_tail.store(0, std::memory_order_release);
        g_frames_processed.store(0, std::memory_order_release);
        g_payload_bytes_processed.store(0, std::memory_order_release);
        g_underflows.store(0, std::memory_order_release);
        g_overflows.store(0, std::memory_order_release);
        g_transport_stop_reason.store(TransportStopReason::None, std::memory_order_release);
        g_transport_relocks.store(0, std::memory_order_release);
        g_transport_stop_detail.clear();
        g_producer_done.store(false, std::memory_order_release);
        g_stop_requested.store(false, std::memory_order_release);
        g_start_time = std::chrono::steady_clock::now();

        set_thread_affinity(2);
        set_thread_priority(50);

        std::cout << "========================================" << std::endl;
        std::cout << "DVB-S2 transmitter" << std::endl;
        std::cout << "MODCOD:      " << cfg.name << std::endl;
        std::cout << "Modulation:  " << cfg.modulation_name() << std::endl;
        std::cout << "Frame type:  " << (cfg.short_frame ? "Short" : "Normal") << std::endl;
        std::cout << "Kbch/Nbch:   " << cfg.kbch << " / " << cfg.nbch << std::endl;
        std::cout << "Nldpc:       " << cfg.nldpc << std::endl;
        std::cout << "Payload:     " << cfg.payload_bytes() << " bytes per BBFRAME" << std::endl;
        std::cout << "Pilots:      " << (pilots ? "ON" : "OFF") << std::endl;
        std::cout << "Roll-off:    " << rolloff_to_string(rolloff) << std::endl;
        std::cout << "Gold code:   " << gold_code << " (root " << root_code << ")" << std::endl;
        std::cout << "Frequency:   " << std::fixed << std::setprecision(3)
                  << (static_cast<double>(frequency_hz) / 1.0e6) << " MHz" << std::endl;
        std::cout << "Symbol rate: " << (static_cast<double>(symbol_rate) / 1000.0) << " kS/s" << std::endl;
        std::cout << "Sample rate: " << (static_cast<double>(sample_rate) / 1.0e6) << " MS/s (SPS=" << sps << ")" << std::endl;
        if (is_pluto_family(output_device)) {
            std::cout << "RF BW:       " << (static_cast<double>(bb_bw) / 1.0e6) << " MHz" << std::endl;
        } else {
            std::cout << "BB BW:       " << (static_cast<double>(bb_bw) / 1.0e6) << " MHz" << std::endl;
        }
        std::cout << "RRC taps:    " << rrc_num_taps;
        if (low_sr_stabilize && symbol_rate < 250000u) {
            std::cout << " (low-SR stabilization)";
        }
        std::cout << std::endl;
        if (is_pluto_family(output_device)) {
            std::cout << "SR mode:     " << sample_rate_mode_name(sample_rate_mode) << std::endl;
            if (sample_rate_mode == SampleRateMode::AutoSafe && symbol_rate <= kPlutoPreferredCleanMaxSymbolRate) {
                std::cout << "SR note:     Pluto clean-mode keeps SPS >= 10 up to 500 kS/s for a more robust constellation" << std::endl;
            }
            if (sample_rate_mode == SampleRateMode::PlutoF5oeoCompat && symbol_rate > 250000u) {
                const int compat_target_sps = choose_even_sps_near_target(symbol_rate, kPlutoCompatTargetSampleRate, 4, 10);
                const uint32_t compat_target_rate = symbol_rate * static_cast<uint32_t>(compat_target_sps);
                if (compat_target_rate < kPlutoSafeMinSampleRate && sample_rate >= kPlutoSafeMinSampleRate) {
                    std::cout << "SR note:     compat target " << std::fixed << std::setprecision(3)
                              << (static_cast<double>(compat_target_rate) / 1.0e6)
                              << " MS/s was below Pluto safe floor; guarded to "
                              << (static_cast<double>(sample_rate) / 1.0e6) << " MS/s" << std::endl;
                }
            }
        }
        std::cout << "Device:      " << output_device_label(output_device) << std::endl;
        if (output_device == OutputDevice::PlutoDVB2F5OEO || output_device == OutputDevice::Pluto03032402) {
            std::cout << "Backend:     split Pluto contract preserved in C++; direct libiio TX path remains shared here" << std::endl;
        }
        std::cout << "TX mapper:   " << gain_mode_name(gain_mode) << " (default is fixed; set DATV_TX_GAIN_MODE=adaptive to restore adaptive mapping)";
        if (gain_mode == GainMode::Fixed) {
            std::cout << " (gain=" << std::fixed << std::setprecision(2) << fixed_gain << "x)";
        }
        std::cout << std::endl;
        if (is_pluto_family(output_device)) {
            std::cout << "TX atten:    " << std::fixed << std::setprecision(2) << tx_level << " dB" << std::endl;
            std::cout << "Pluto URI:   " << pluto_uri << std::endl;
        } else if (output_device == OutputDevice::Lime) {
            std::cout << "TX gain:     " << std::fixed << std::setprecision(2) << tx_level << " dB" << std::endl;
            std::cout << "Soapy args:  " << lime_device_args << std::endl;
        } else {
            std::cout << "TX gain:     " << static_cast<int>(tx_level) << " dB" << std::endl;
            std::cout << "Amp:         " << (amp_enable ? "ON" : "OFF") << std::endl;
        }
        std::cout << "Est. TS rate:" << std::setprecision(0)
                  << (cfg.transport_bitrate(static_cast<double>(symbol_rate), pilots) / 1000.0) << " kbps" << std::endl;
        std::cout << "========================================" << std::endl;

        if (output_device == OutputDevice::HackRF) {
#ifdef HAVE_HACKRF
            const uint32_t tx_gain = static_cast<uint32_t>(std::clamp(std::lround(tx_level), 0l, 47l));
            if (hackrf_init() != HACKRF_SUCCESS) throw std::runtime_error("hackrf_init failed");
            g_hackrf_initialized = true;
            if (hackrf_open(&g_device) != HACKRF_SUCCESS) throw std::runtime_error("hackrf_open failed");
            if (hackrf_set_freq(g_device, frequency_hz) != HACKRF_SUCCESS) throw std::runtime_error("hackrf_set_freq failed");
            if (hackrf_set_sample_rate(g_device, sample_rate) != HACKRF_SUCCESS) throw std::runtime_error("hackrf_set_sample_rate failed");
            if (hackrf_set_baseband_filter_bandwidth(g_device, bb_bw) != HACKRF_SUCCESS) throw std::runtime_error("hackrf_set_baseband_filter_bandwidth failed");
            if (hackrf_set_txvga_gain(g_device, tx_gain) != HACKRF_SUCCESS) throw std::runtime_error("hackrf_set_txvga_gain failed");
            if (hackrf_set_amp_enable(g_device, amp_enable) != HACKRF_SUCCESS) throw std::runtime_error("hackrf_set_amp_enable failed");
            g_hackrf_programmed_sample_rate = sample_rate;
            g_hackrf_programmed_rf_bandwidth = bb_bw;
            g_hackrf_programmed_frequency_hz = frequency_hz;
            g_hackrf_readback_valid = true;
#else
            throw std::runtime_error("This build does not include HackRF support (libhackrf not found at build time)");
#endif
        } else if (is_pluto_family(output_device)) {
#ifdef HAVE_PLUTO
            setup_pluto(frequency_hz, sample_rate, bb_bw, tx_level, pluto_uri);
            if (g_pluto_readback_valid) {
                std::cout << "Pluto readback: sample_rate=" << g_pluto_readback_sample_rate
                          << " S/s | rf_bw=" << g_pluto_readback_rf_bandwidth_hz
                          << " Hz | LO=" << g_pluto_readback_frequency_hz << " Hz" << std::endl;
            }
#else
            throw std::runtime_error("This build does not include Pluto support (libiio not found at build time)");
#endif
        } else {
#ifdef HAVE_SOAPYSDR
            setup_lime(frequency_hz, sample_rate, bb_bw, tx_level, lime_device_args);
            if (g_lime_readback_valid) {
                std::cout << "Lime readback: sample_rate=" << std::fixed << std::setprecision(0) << g_lime_readback_sample_rate
                          << " S/s | rf_bw=" << g_lime_readback_rf_bandwidth
                          << " Hz | LO=" << g_lime_readback_frequency_hz
                          << " Hz | gain=" << g_lime_readback_gain_db << " dB" << std::endl;
            }
#else
            throw std::runtime_error("This build does not include LimeSDR support (SoapySDR not found at build time)");
#endif
        }

        if (g_hackrf_readback_valid) {
            std::cout << "HackRF status: sample_rate=" << g_hackrf_programmed_sample_rate
                      << " S/s | bb_bw=" << g_hackrf_programmed_rf_bandwidth
                      << " Hz | LO=" << g_hackrf_programmed_frequency_hz
                      << " Hz" << std::endl;
        }

        std::thread producer(producer_thread, cfg, rolloff, pilots, root_code, sps, rrc_taps);

        const size_t prime_target = std::min(kRingIqCapacity / 4u, estimate_frame_iq_samples(cfg, pilots, sps) * 4u);
        while (!g_stop_requested.load(std::memory_order_acquire) &&
               !g_producer_done.load(std::memory_order_acquire) &&
               ring_fill_samples() < prime_target) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        if (ring_fill_samples() == 0u) {
            g_stop_requested.store(true, std::memory_order_release);
            if (producer.joinable()) {
                producer.join();
            }
            cleanup_hackrf();
            cleanup_pluto();
            cleanup_lime();
            std::cerr << "[error] No transport-stream input reached the transmitter" << std::endl;
            return 2;
        }

        if (output_device == OutputDevice::HackRF) {
#ifdef HAVE_HACKRF
            if (hackrf_start_tx(g_device, tx_callback, nullptr) != HACKRF_SUCCESS) {
                g_stop_requested.store(true, std::memory_order_release);
                if (producer.joinable()) {
                    producer.join();
                }
                throw std::runtime_error("hackrf_start_tx failed");
            }
            g_tx_started.store(true, std::memory_order_release);
#else
            throw std::runtime_error("This build does not include HackRF support (libhackrf not found at build time)");
#endif
        }

        std::thread pluto_tx_thread;
        std::thread lime_tx_thread;
#ifdef HAVE_PLUTO
        if (is_pluto_family(output_device)) {
            pluto_tx_thread = std::thread(pluto_tx_loop);
        }
#endif
#ifdef HAVE_SOAPYSDR
        if (output_device == OutputDevice::Lime) {
            lime_tx_thread = std::thread(lime_tx_loop);
        }
#endif

        auto last_print = std::chrono::steady_clock::now();
        while (!g_stop_requested.load(std::memory_order_acquire)) {
            const bool producer_done = g_producer_done.load(std::memory_order_acquire);
            const size_t fill = ring_fill_samples();
            if (producer_done && fill == 0u) {
                break;
            }

            const auto now = std::chrono::steady_clock::now();
            if (now - last_print >= std::chrono::seconds(1)) {
                const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - g_start_time).count();
                const double elapsed_s = std::max(0.001, elapsed_ms / 1000.0);
                const uint64_t frames = g_frames_processed.load(std::memory_order_relaxed);
                const double fps = frames / elapsed_s;
                const double kbps = (g_payload_bytes_processed.load(std::memory_order_relaxed) * 8.0) / elapsed_s / 1000.0;
                const int fill_pct = static_cast<int>((100.0 * static_cast<double>(fill)) / static_cast<double>(kRingIqCapacity));

                std::cout << "\rFrames: " << frames
                          << " | Buffer: " << ((fill * sizeof(IqRingSample)) / 1024u) << " KB (" << fill_pct << "%)"
                          << " | Avg: " << std::fixed << std::setprecision(1) << fps << " fps"
                          << " | Underflows: " << g_underflows.load(std::memory_order_relaxed)
                          << " | Overflows: " << g_overflows.load(std::memory_order_relaxed)
                          << " | Rate: " << std::setprecision(0) << kbps << " kbps (producer)   "
                          << std::flush;
                last_print = now;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        g_stop_requested.store(true, std::memory_order_release);
        if (producer.joinable()) {
            producer.join();
        }
#ifdef HAVE_PLUTO
        if (pluto_tx_thread.joinable()) {
            pluto_tx_thread.join();
        }
#endif
#ifdef HAVE_SOAPYSDR
        if (lime_tx_thread.joinable()) {
            lime_tx_thread.join();
        }
#endif

        cleanup_hackrf();
        cleanup_pluto();
        cleanup_lime();

        const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - g_start_time).count();
        const double elapsed_s = std::max(0.001, elapsed_ms / 1000.0);
        const double avg_kbps = (g_payload_bytes_processed.load(std::memory_order_relaxed) * 8.0) / elapsed_s / 1000.0;

        std::cout << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Frames sent:     " << g_frames_processed.load(std::memory_order_relaxed) << std::endl;
        std::cout << "Duration:        " << std::fixed << std::setprecision(1) << elapsed_s << " s" << std::endl;
        std::cout << "Average producer rate: " << std::setprecision(0) << avg_kbps << " kbps" << std::endl;
        std::cout << "Underflows:      " << g_underflows.load(std::memory_order_relaxed) << std::endl;
        std::cout << "Overflows:       " << g_overflows.load(std::memory_order_relaxed) << std::endl;
        std::cout << "TS re-locks:     " << g_transport_relocks.load(std::memory_order_relaxed) << std::endl;
        const TransportStopReason transport_stop_reason = g_transport_stop_reason.load(std::memory_order_acquire);
        if (transport_stop_reason != TransportStopReason::None) {
            std::cout << "Transport stop:  " << transport_stop_reason_string(transport_stop_reason) << std::endl;
            if (!g_transport_stop_detail.empty()) {
                std::cout << "Detail:          " << g_transport_stop_detail << std::endl;
            }
        }
        std::cout << "========================================" << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        g_stop_requested.store(true, std::memory_order_release);
        cleanup_hackrf();
        cleanup_pluto();
        cleanup_lime();
        std::cerr << "[fatal] " << ex.what() << std::endl;
        return 1;
    }
}
