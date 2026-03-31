#include "dvbs2_tx.h"

#include <array>
#include <cmath>
#include <stdexcept>
#include <utility>
#include <vector>

using cf = std::complex<float>;

const float qpsk_f_i[4] = {
    0.70710678118f,
    0.70710678118f,
   -0.70710678118f,
   -0.70710678118f,
};

const float qpsk_f_q[4] = {
    0.70710678118f,
   -0.70710678118f,
    0.70710678118f,
   -0.70710678118f,
};

namespace {

constexpr float kInvSqrt2 = 0.70710678118f;

const float psk8_f_i[8] = {
     kInvSqrt2,  1.0f, -1.0f, -kInvSqrt2,
     0.0f,       kInvSqrt2, -kInvSqrt2,  0.0f,
};

const float psk8_f_q[8] = {
     kInvSqrt2,  0.0f,  0.0f, -kInvSqrt2,
     1.0f,      -kInvSqrt2,  kInvSqrt2, -1.0f,
};

constexpr std::array<int, 16> g_16apsk_point_to_label = {12,14,15,13,4,0,8,10,2,6,7,3,11,9,1,5};
constexpr std::array<int, 32> g_32apsk_point_to_label = {17,21,23,19,16,0,1,5,4,20,22,6,7,3,2,18,24,8,25,9,13,29,12,28,30,14,31,15,11,27,10,26};

std::array<cf, 16> build_16apsk_constellation()
{
    std::array<cf, 16> points{};
    std::array<cf, 16> by_label{};
    for (int i = 0; i < 4; ++i) {
        const float angle = static_cast<float>((M_PI / 4.0) + (i * (M_PI / 2.0)));
        points[static_cast<size_t>(i)] = std::polar(1.0f, angle);
    }
    for (int i = 0; i < 12; ++i) {
        const float angle = static_cast<float>((M_PI / 12.0) + (i * (M_PI / 6.0)));
        points[static_cast<size_t>(4 + i)] = std::polar(1.0f, angle);
    }
    for (size_t point = 0; point < g_16apsk_point_to_label.size(); ++point) {
        by_label[static_cast<size_t>(g_16apsk_point_to_label[point])] = points[point];
    }
    return by_label;
}

std::array<uint8_t, 16> build_16apsk_ring_classes()
{
    std::array<uint8_t, 16> by_label{};
    for (size_t point = 0; point < g_16apsk_point_to_label.size(); ++point) {
        by_label[static_cast<size_t>(g_16apsk_point_to_label[point])] = (point < 4) ? 0u : 1u;
    }
    return by_label;
}

std::array<cf, 32> build_32apsk_constellation()
{
    std::array<cf, 32> points{};
    std::array<cf, 32> by_label{};
    for (int i = 0; i < 4; ++i) {
        const float angle = static_cast<float>((M_PI / 4.0) + (i * (M_PI / 2.0)));
        points[static_cast<size_t>(i)] = std::polar(1.0f, angle);
    }
    for (int i = 0; i < 12; ++i) {
        const float angle = static_cast<float>((M_PI / 12.0) + (i * (M_PI / 6.0)));
        points[static_cast<size_t>(4 + i)] = std::polar(1.0f, angle);
    }
    for (int i = 0; i < 16; ++i) {
        const float angle = static_cast<float>(i * (M_PI / 8.0));
        points[static_cast<size_t>(16 + i)] = std::polar(1.0f, angle);
    }
    for (size_t point = 0; point < g_32apsk_point_to_label.size(); ++point) {
        by_label[static_cast<size_t>(g_32apsk_point_to_label[point])] = points[point];
    }
    return by_label;
}

std::array<uint8_t, 32> build_32apsk_ring_classes()
{
    std::array<uint8_t, 32> by_label{};
    for (size_t point = 0; point < g_32apsk_point_to_label.size(); ++point) {
        const uint8_t ring_class = (point < 4) ? 0u : ((point < 16) ? 1u : 2u);
        by_label[static_cast<size_t>(g_32apsk_point_to_label[point])] = ring_class;
    }
    return by_label;
}

const std::array<cf, 16> g_16apsk_base = build_16apsk_constellation();
const std::array<uint8_t, 16> g_16apsk_ring = build_16apsk_ring_classes();
const std::array<cf, 32> g_32apsk_base = build_32apsk_constellation();
const std::array<uint8_t, 32> g_32apsk_ring = build_32apsk_ring_classes();

const unsigned int g_rm_basis[7] = {
    0x90AC2DDDu,
    0x55555555u,
    0x33333333u,
    0x0F0F0F0Fu,
    0x00FF00FFu,
    0x0000FFFFu,
    0xFFFFFFFFu,
};

const int g_pl_scramble[64] = {
    0,1,1,1,0,0,0,1,1,0,0,1,1,1,0,1,1,0,0,0,0,0,1,1,1,1,0,0,1,0,0,1,
    0,1,0,1,0,0,1,1,0,1,0,0,0,0,1,0,0,0,1,0,1,1,0,1,1,1,1,1,1,0,1,0,
};

const int g_ph_sync_seq[26] = {
    0,1,1,0,0,0,1,1,0,1,0,0,1,0,1,1,1,0,1,0,0,0,0,0,1,0,
};

inline cf header_symbol(int parity, int bit)
{
    if (parity == 0) {
        return bit == 0 ? cf(kInvSqrt2, kInvSqrt2) : cf(-kInvSqrt2, -kInvSqrt2);
    }
    return bit == 0 ? cf(-kInvSqrt2, kInvSqrt2) : cf(kInvSqrt2, -kInvSqrt2);
}

inline cf rotate_symbol(const cf& in, int rotation)
{
    switch (rotation & 0x3) {
    case 0:
        return in;
    case 1:
        return cf(-in.imag(), in.real());
    case 2:
        return -in;
    default:
        return cf(in.imag(), -in.real());
    }
}

void b_64_8_code(uint8_t in, std::array<int, 64>& out)
{
    unsigned int temp = 0u;
    if (in & 0x80u) temp ^= g_rm_basis[0];
    if (in & 0x40u) temp ^= g_rm_basis[1];
    if (in & 0x20u) temp ^= g_rm_basis[2];
    if (in & 0x10u) temp ^= g_rm_basis[3];
    if (in & 0x08u) temp ^= g_rm_basis[4];
    if (in & 0x04u) temp ^= g_rm_basis[5];
    if (in & 0x02u) temp ^= g_rm_basis[6];

    unsigned int bit = 0x80000000u;
    for (int m = 0; m < 32; ++m) {
        out[m * 2] = (temp & bit) ? 1 : 0;
        out[(m * 2) + 1] = out[m * 2] ^ (in & 0x01u ? 1 : 0);
        bit >>= 1;
    }

    for (int m = 0; m < 64; ++m) {
        out[m] ^= g_pl_scramble[m];
    }
}

std::array<int, 64> encode_pls(int modcod, bool is_short_frame, bool pilots)
{
    std::array<int, 64> out{};
    const uint8_t type = static_cast<uint8_t>((is_short_frame ? 2u : 0u) | (pilots ? 1u : 0u));
    const uint8_t code = (modcod & 0x80) ? static_cast<uint8_t>(modcod | (type & 0x1u))
                                          : static_cast<uint8_t>((modcod << 2) | type);
    b_64_8_code(code, out);
    return out;
}

bool is_8psk_rate_3_5(const DVB_Params& cfg)
{
    return cfg.modulation == Modulation::PSK8 && cfg.name.find("3/5") != std::string::npos;
}

float apsk16_gamma_for(const DVB_Params& cfg)
{
    if (cfg.name.find("2/3") != std::string::npos) return 3.15f;
    if (cfg.name.find("3/4") != std::string::npos) return 2.85f;
    if (cfg.name.find("4/5") != std::string::npos) return 2.75f;
    if (cfg.name.find("5/6") != std::string::npos) return 2.70f;
    if (cfg.name.find("8/9") != std::string::npos) return 2.60f;
    if (cfg.name.find("9/10") != std::string::npos) return 2.57f;
    throw std::runtime_error("Unsupported 16APSK gamma for " + cfg.name);
}

std::pair<float, float> apsk32_gamma_for(const DVB_Params& cfg)
{
    if (cfg.name.find("3/4") != std::string::npos) return {2.84f, 5.27f};
    if (cfg.name.find("4/5") != std::string::npos) return {2.72f, 4.87f};
    if (cfg.name.find("5/6") != std::string::npos) return {2.64f, 4.64f};
    if (cfg.name.find("8/9") != std::string::npos) return {2.54f, 4.33f};
    if (cfg.name.find("9/10") != std::string::npos) return {2.53f, 4.30f};
    throw std::runtime_error("Unsupported 32APSK gamma for " + cfg.name);
}

std::vector<uint8_t> interleave_bits(const DVB_Params& cfg, const std::vector<uint8_t>& frame_bits)
{
    if (!cfg.requires_bit_interleaver()) {
        return std::vector<uint8_t>(frame_bits.begin(), frame_bits.begin() + cfg.nldpc);
    }

    const int columns = cfg.bits_per_symbol();
    const int rows = cfg.nldpc / columns;
    std::vector<uint8_t> out(static_cast<size_t>(cfg.nldpc), 0u);
    const bool reverse_readout = is_8psk_rate_3_5(cfg);

    size_t out_pos = 0;
    for (int row = 0; row < rows; ++row) {
        if (reverse_readout) {
            for (int column = columns - 1; column >= 0; --column) {
                out[out_pos++] = frame_bits[static_cast<size_t>((column * rows) + row)];
            }
        } else {
            for (int column = 0; column < columns; ++column) {
                out[out_pos++] = frame_bits[static_cast<size_t>((column * rows) + row)];
            }
        }
    }
    return out;
}

cf normalise_symbol(const cf& symbol, float normaliser)
{
    return symbol / normaliser;
}

cf apsk16_symbol(const DVB_Params& cfg, int idx)
{
    const float gamma = apsk16_gamma_for(cfg);
    const float average_power = (4.0f + (12.0f * gamma * gamma)) / 16.0f;
    const float normaliser = std::sqrt(average_power);
    cf symbol = g_16apsk_base[static_cast<size_t>(idx)];
    if (g_16apsk_ring[static_cast<size_t>(idx)] == 1u) {
        symbol *= gamma;
    }
    return normalise_symbol(symbol, normaliser);
}

cf apsk32_symbol(const DVB_Params& cfg, int idx)
{
    const auto gamma = apsk32_gamma_for(cfg);
    const float average_power = (4.0f + (12.0f * gamma.first * gamma.first) + (16.0f * gamma.second * gamma.second)) / 32.0f;
    const float normaliser = std::sqrt(average_power);
    cf symbol = g_32apsk_base[static_cast<size_t>(idx)];
    const uint8_t ring_class = g_32apsk_ring[static_cast<size_t>(idx)];
    if (ring_class == 2u) {
        symbol *= gamma.second;
    } else if (ring_class == 1u) {
        symbol *= gamma.first;
    }
    return normalise_symbol(symbol, normaliser);
}

cf modulation_symbol(const DVB_Params& cfg, const uint8_t* bits)
{
    if (cfg.modulation == Modulation::QPSK) {
        const int idx = (bits[0] << 1) | bits[1];
        return cf(qpsk_f_i[idx], qpsk_f_q[idx]);
    }
    if (cfg.modulation == Modulation::PSK8) {
        const int idx = (bits[0] << 2) | (bits[1] << 1) | bits[2];
        return cf(psk8_f_i[idx], psk8_f_q[idx]);
    }
    if (cfg.modulation == Modulation::APSK16) {
        const int idx = (bits[0] << 3) | (bits[1] << 2) | (bits[2] << 1) | bits[3];
        return apsk16_symbol(cfg, idx);
    }
    if (cfg.modulation == Modulation::APSK32) {
        const int idx = (bits[0] << 4) | (bits[1] << 3) | (bits[2] << 2) | (bits[3] << 1) | bits[4];
        return apsk32_symbol(cfg, idx);
    }
    throw std::runtime_error("Unsupported modulation in PLFRAME assembly: " + cfg.name);
}

} // namespace

int PhysicalLayerScrambler::parity(uint32_t value, uint32_t mask)
{
    value &= mask;
    value ^= (value >> 16);
    value ^= (value >> 8);
    value ^= (value >> 4);
    value ^= (value >> 2);
    value ^= (value >> 1);
    return static_cast<int>(value & 0x1u);
}

void PhysicalLayerScrambler::reset(uint32_t root_code)
{
    x_ = (root_code == 0u) ? 1u : (root_code & 0x3ffffu);
    y_ = 0x3ffffu;
}

int PhysicalLayerScrambler::rotation()
{
    const int xa = parity(x_, 0x8050u);
    const int xb = parity(x_, 0x0081u);
    const int xc = static_cast<int>(x_ & 0x1u);

    x_ >>= 1;
    if (xb != 0) {
        x_ |= 0x20000u;
    }

    const int ya = parity(y_, 0x04A1u);
    const int yb = parity(y_, 0xFF60u);
    const int yc = static_cast<int>(y_ & 0x1u);

    y_ >>= 1;
    if (ya != 0) {
        y_ |= 0x20000u;
    }

    const int zna = xc ^ yc;
    const int znb = xa ^ yb;
    return (znb << 1) | zna;
}

std::vector<cf> generate_pl_header(int modcod, bool is_short_frame, bool pilots)
{
    std::vector<cf> header;
    header.reserve(90);

    const std::array<int, 64> pls = encode_pls(modcod, is_short_frame, pilots);
    for (int i = 0; i < 26; ++i) {
        header.push_back(header_symbol(i & 0x1, g_ph_sync_seq[i]));
    }
    for (int i = 0; i < 64; ++i) {
        header.push_back(header_symbol((26 + i) & 0x1, pls[i]));
    }
    return header;
}

void assemble_plframe(std::vector<cf>& output,
                      const DVB_Params& cfg,
                      const std::vector<uint8_t>& frame_bits,
                      bool pilots,
                      uint32_t root_code)
{
    if (static_cast<int>(frame_bits.size()) < cfg.nldpc) {
        throw std::runtime_error("PLFRAME assembly input shorter than Nldpc");
    }

    output.clear();
    output.reserve(static_cast<size_t>(cfg.plframe_symbols(pilots)));

    const auto header = generate_pl_header(cfg.modcod, cfg.short_frame, pilots);
    output.insert(output.end(), header.begin(), header.end());

    const std::vector<uint8_t> mapped_bits = interleave_bits(cfg, frame_bits);
    const int bits_per_symbol = cfg.bits_per_symbol();
    const int slots = cfg.slots();

    PhysicalLayerScrambler scrambler(root_code);
    for (int slot = 0; slot < slots; ++slot) {
        const int slot_symbol_base = slot * 90;
        for (int sym = 0; sym < 90; ++sym) {
            const int symbol_index = slot_symbol_base + sym;
            const int bit_base = symbol_index * bits_per_symbol;
            const cf symbol = modulation_symbol(cfg, &mapped_bits[static_cast<size_t>(bit_base)]);
            output.push_back(rotate_symbol(symbol, scrambler.rotation()));
        }

        if (pilots && ((slot + 1) % 16 == 0) && (slot != (slots - 1))) {
            const cf pilot(kInvSqrt2, kInvSqrt2);
            for (int p = 0; p < 36; ++p) {
                output.push_back(rotate_symbol(pilot, scrambler.rotation()));
            }
        }
    }
}
