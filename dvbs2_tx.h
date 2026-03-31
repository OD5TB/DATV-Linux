#ifndef DVBS2_TX_H
#define DVBS2_TX_H

#include <array>
#include <complex>
#include <cstdint>
#include <string>
#include <vector>

enum class RollOff : uint8_t {
    RO_35 = 0,
    RO_25 = 1,
    RO_20 = 2,
};

enum class Modulation : uint8_t {
    QPSK = 2,
    PSK8 = 3,
    APSK16 = 4,
    APSK32 = 5,
};

struct DVB_Params {
    std::string name;
    Modulation modulation = Modulation::QPSK;
    int modcod = 0;
    int kbch = 0;
    int nbch = 0;
    int nldpc = 0;
    int bch_bits = 0;
    bool short_frame = false;

    int payload_bits() const { return kbch - 80; }
    int payload_bytes() const { return (kbch - 80) / 8; }
    int ldpc_parity_bits() const { return nldpc - nbch; }
    int q_val() const { return ldpc_parity_bits() / 360; }
    int bits_per_symbol() const { return static_cast<int>(modulation); }
    int xfec_symbols() const { return nldpc / bits_per_symbol(); }
    int slots() const { return xfec_symbols() / 90; }
    int pilot_blocks(bool pilots) const { return pilots ? ((slots() - 1) / 16) : 0; }
    int pilot_symbols(bool pilots) const { return pilot_blocks(pilots) * 36; }
    int plframe_symbols(bool pilots) const { return 90 + xfec_symbols() + pilot_symbols(pilots); }
    double transport_bitrate(double symbol_rate, bool pilots) const {
        return symbol_rate * static_cast<double>(payload_bits()) /
               static_cast<double>(plframe_symbols(pilots));
    }
    const char* modulation_name() const {
        switch (modulation) {
        case Modulation::PSK8:
            return "8PSK";
        case Modulation::APSK16:
            return "16APSK";
        case Modulation::APSK32:
            return "32APSK";
        case Modulation::QPSK:
        default:
            return "QPSK";
        }
    }
    bool requires_bit_interleaver() const { return modulation != Modulation::QPSK; }
};

class PhysicalLayerScrambler {
public:
    explicit PhysicalLayerScrambler(uint32_t root_code = 1) { reset(root_code); }
    void reset(uint32_t root_code = 1);
    int rotation();

private:
    static int parity(uint32_t value, uint32_t mask);
    uint32_t x_ = 1;
    uint32_t y_ = 0x3ffff;
};

extern const float qpsk_f_i[4];
extern const float qpsk_f_q[4];

DVB_Params get_fec_config(const std::string& selection);
RollOff parse_rolloff(const std::string& value);
const char* rolloff_to_string(RollOff rolloff);
float rolloff_to_alpha(RollOff rolloff);

uint8_t calculate_bbheader_crc8(const uint8_t* data, int len);
void build_bbframe(std::vector<uint8_t>& frame_bits,
                   const std::vector<uint8_t>& payload_bytes,
                   const DVB_Params& cfg,
                   RollOff rolloff,
                   int syncd_bits);
void bb_randomize(std::vector<uint8_t>& bits, int kbch);
void bch_init();
void bch_encode(std::vector<uint8_t>& bits, const DVB_Params& cfg);
void ldpc_encode(std::vector<uint8_t>& bits, const DVB_Params& cfg);

std::vector<std::complex<float>> generate_pl_header(int modcod,
                                                    bool is_short_frame,
                                                    bool pilots);
void assemble_plframe(std::vector<std::complex<float>>& output,
                      const DVB_Params& cfg,
                      const std::vector<uint8_t>& frame_bits,
                      bool pilots,
                      uint32_t root_code = 1);

void generate_rrc_taps(std::vector<float>& taps, int num_taps, int sps, float alpha);
void set_thread_affinity(int core);

#endif
