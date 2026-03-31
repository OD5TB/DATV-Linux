#include "dvbs2_tx.h"
#include "dvb_s2_tables.hh"

#include <algorithm>
#include <array>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace {

std::array<uint8_t, 64800> g_bb_randomise{};
std::once_flag g_bb_randomise_once;

std::array<unsigned int, 6> g_poly_n12{};
std::array<unsigned int, 5> g_poly_n10{};
std::array<unsigned int, 4> g_poly_n8{};
std::array<unsigned int, 6> g_poly_s12{};
std::once_flag g_bch_once;

struct LDPCPlan {
    int nbch = 0;
    int pbits = 0;
    std::vector<uint16_t> src;
    std::vector<uint16_t> dst;
};

void init_bb_randomiser()
{
    int sr = 0x4A80;
    for (size_t i = 0; i < g_bb_randomise.size(); ++i) {
        const int b = ((sr) ^ (sr >> 1)) & 1;
        g_bb_randomise[i] = static_cast<uint8_t>(b);
        sr >>= 1;
        if (b) {
            sr |= 0x4000;
        }
    }
}

int poly_mult(const int* ina, int lena, const int* inb, int lenb, int* out)
{
    std::memset(out, 0, sizeof(int) * static_cast<size_t>(lena + lenb));
    for (int i = 0; i < lena; ++i) {
        for (int j = 0; j < lenb; ++j) {
            if (ina[i] != 0 && inb[j] != 0) {
                out[i + j]++;
            }
        }
    }

    int max_index = 0;
    for (int i = 0; i < (lena + lenb); ++i) {
        out[i] &= 1;
        if (out[i] != 0) {
            max_index = i;
        }
    }
    return max_index + 1;
}

void poly_pack(const int* pin, unsigned int* pout, int len)
{
    const int words = (len + 31) / 32;
    int ptr = 0;
    for (int i = 0; i < words; ++i) {
        unsigned int temp = 0x80000000u;
        pout[i] = 0;
        for (int j = 0; j < 32; ++j) {
            if (ptr < len && pin[ptr] != 0) {
                pout[i] |= temp;
            }
            temp >>= 1;
            ++ptr;
        }
    }
}

inline void reg_4_shift(unsigned int* sr)
{
    sr[3] = (sr[3] >> 1) | (sr[2] << 31);
    sr[2] = (sr[2] >> 1) | (sr[1] << 31);
    sr[1] = (sr[1] >> 1) | (sr[0] << 31);
    sr[0] = (sr[0] >> 1);
}

inline void reg_5_shift(unsigned int* sr)
{
    sr[4] = (sr[4] >> 1) | (sr[3] << 31);
    sr[3] = (sr[3] >> 1) | (sr[2] << 31);
    sr[2] = (sr[2] >> 1) | (sr[1] << 31);
    sr[1] = (sr[1] >> 1) | (sr[0] << 31);
    sr[0] = (sr[0] >> 1);
}

inline void reg_6_shift(unsigned int* sr)
{
    sr[5] = (sr[5] >> 1) | (sr[4] << 31);
    sr[4] = (sr[4] >> 1) | (sr[3] << 31);
    sr[3] = (sr[3] >> 1) | (sr[2] << 31);
    sr[2] = (sr[2] >> 1) | (sr[1] << 31);
    sr[1] = (sr[1] >> 1) | (sr[0] << 31);
    sr[0] = (sr[0] >> 1);
}

void bch_init_impl()
{
    const int polyn01[] = {1,0,1,1,0,1,0,0,0,0,0,0,0,0,0,0,1};
    const int polyn02[] = {1,1,0,0,1,1,1,0,1,0,0,0,0,0,0,0,1};
    const int polyn03[] = {1,0,1,1,1,1,0,1,1,1,1,1,0,0,0,0,1};
    const int polyn04[] = {1,0,1,0,1,0,1,0,0,1,0,1,1,0,1,0,1};
    const int polyn05[] = {1,1,1,1,0,1,0,0,1,1,1,1,1,0,0,0,1};
    const int polyn06[] = {1,0,1,0,1,1,0,1,1,1,1,0,1,1,1,1,1};
    const int polyn07[] = {1,0,1,0,0,1,1,0,1,1,1,1,0,1,0,1,1};
    const int polyn08[] = {1,1,1,0,0,1,1,0,1,1,0,0,1,1,1,0,1};
    const int polyn09[] = {1,0,0,0,0,1,0,1,0,1,1,1,0,0,0,0,1};
    const int polyn10[] = {1,1,1,0,0,1,0,1,1,0,1,0,1,1,1,0,1};
    const int polyn11[] = {1,0,1,1,0,1,0,0,0,1,0,1,1,1,0,0,1};
    const int polyn12[] = {1,1,0,0,0,1,1,1,0,1,0,1,1,0,0,0,1};

    const int polys01[] = {1,1,0,1,0,1,0,0,0,0,0,0,0,0,1};
    const int polys02[] = {1,0,0,0,0,0,1,0,1,0,0,1,0,0,1};
    const int polys03[] = {1,1,1,0,0,0,1,0,0,1,1,0,0,0,1};
    const int polys04[] = {1,0,0,0,1,0,0,1,1,0,1,0,1,0,1};
    const int polys05[] = {1,0,1,0,1,0,1,0,1,1,0,1,0,1,1};
    const int polys06[] = {1,0,0,1,0,0,0,1,1,1,0,0,0,1,1};
    const int polys07[] = {1,0,1,0,0,1,1,1,0,0,1,1,0,1,1};
    const int polys08[] = {1,0,0,0,0,1,0,0,1,1,1,1,0,0,1};
    const int polys09[] = {1,1,1,1,0,0,0,0,0,1,1,0,0,0,1};
    const int polys10[] = {1,0,0,1,0,0,1,0,0,1,0,1,1,0,1};
    const int polys11[] = {1,0,0,0,1,0,0,0,0,0,0,1,1,0,1};
    const int polys12[] = {1,1,1,1,0,1,1,1,1,0,1,0,0,1,1};

    int polyout[2][200] = {};
    int len = poly_mult(polyn01, 17, polyn02, 17, polyout[0]);
    len = poly_mult(polyn03, 17, polyout[0], len, polyout[1]);
    len = poly_mult(polyn04, 17, polyout[1], len, polyout[0]);
    len = poly_mult(polyn05, 17, polyout[0], len, polyout[1]);
    len = poly_mult(polyn06, 17, polyout[1], len, polyout[0]);
    len = poly_mult(polyn07, 17, polyout[0], len, polyout[1]);
    len = poly_mult(polyn08, 17, polyout[1], len, polyout[0]);
    poly_pack(polyout[0], g_poly_n8.data(), 128);

    len = poly_mult(polyn09, 17, polyout[0], len, polyout[1]);
    len = poly_mult(polyn10, 17, polyout[1], len, polyout[0]);
    poly_pack(polyout[0], g_poly_n10.data(), 160);

    len = poly_mult(polyn11, 17, polyout[0], len, polyout[1]);
    len = poly_mult(polyn12, 17, polyout[1], len, polyout[0]);
    poly_pack(polyout[0], g_poly_n12.data(), 192);

    len = poly_mult(polys01, 15, polys02, 15, polyout[0]);
    len = poly_mult(polys03, 15, polyout[0], len, polyout[1]);
    len = poly_mult(polys04, 15, polyout[1], len, polyout[0]);
    len = poly_mult(polys05, 15, polyout[0], len, polyout[1]);
    len = poly_mult(polys06, 15, polyout[1], len, polyout[0]);
    len = poly_mult(polys07, 15, polyout[0], len, polyout[1]);
    len = poly_mult(polys08, 15, polyout[1], len, polyout[0]);
    len = poly_mult(polys09, 15, polyout[0], len, polyout[1]);
    len = poly_mult(polys10, 15, polyout[1], len, polyout[0]);
    len = poly_mult(polys11, 15, polyout[0], len, polyout[1]);
    len = poly_mult(polys12, 15, polyout[1], len, polyout[0]);
    poly_pack(polyout[0], g_poly_s12.data(), 168);
}

enum class BCHCode {
    N12,
    N10,
    N8,
    S12,
};

BCHCode bch_code_for(const DVB_Params& cfg)
{
    if (cfg.short_frame) {
        return BCHCode::S12;
    }
    switch (cfg.bch_bits) {
    case 192:
        return BCHCode::N12;
    case 160:
        return BCHCode::N10;
    case 128:
        return BCHCode::N8;
    default:
        throw std::runtime_error("Unsupported BCH length for " + cfg.name);
    }
}

template <typename Table>
LDPCPlan build_ldpc_plan_template()
{
    LDPCPlan plan;
    plan.nbch = Table::K;
    plan.pbits = Table::N - Table::K;
    const int rows = Table::K / Table::M;
    const int q = plan.pbits / Table::M;

    plan.src.reserve(static_cast<size_t>(Table::K) * static_cast<size_t>(Table::DEG_MAX));
    plan.dst.reserve(static_cast<size_t>(Table::K) * static_cast<size_t>(Table::DEG_MAX));

    int pos_index = 0;
    int degree_group = 0;
    int rows_left = Table::LEN[degree_group];
    int degree = Table::DEG[degree_group];

    int im = 0;
    for (int row = 0; row < rows; ++row) {
        while (rows_left == 0 && Table::LEN[degree_group] != 0) {
            ++degree_group;
            rows_left = Table::LEN[degree_group];
            degree = Table::DEG[degree_group];
        }

        const int* row_positions = &Table::POS[pos_index];
        for (int n = 0; n < Table::M; ++n, ++im) {
            for (int col = 0; col < degree; ++col) {
                plan.src.push_back(static_cast<uint16_t>(im));
                plan.dst.push_back(static_cast<uint16_t>((row_positions[col] + (n * q)) % plan.pbits));
            }
        }
        pos_index += degree;
        --rows_left;
    }

    return plan;
}

template <typename Table>
const LDPCPlan& cached_ldpc_plan()
{
    static const LDPCPlan plan = build_ldpc_plan_template<Table>();
    return plan;
}

const LDPCPlan& select_ldpc_plan(const DVB_Params& cfg)
{
    const std::string& n = cfg.name;
    if (n.find("1/4") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C1>() : cached_ldpc_plan<DVB_S2_TABLE_B1>();
    }
    if (n.find("1/3") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C2>() : cached_ldpc_plan<DVB_S2_TABLE_B2>();
    }
    if (n.find("2/5") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C3>() : cached_ldpc_plan<DVB_S2_TABLE_B3>();
    }
    if (n.find("1/2") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C4>() : cached_ldpc_plan<DVB_S2_TABLE_B4>();
    }
    if (n.find("3/5") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C5>() : cached_ldpc_plan<DVB_S2_TABLE_B5>();
    }
    if (n.find("2/3") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C6>() : cached_ldpc_plan<DVB_S2_TABLE_B6>();
    }
    if (n.find("3/4") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C7>() : cached_ldpc_plan<DVB_S2_TABLE_B7>();
    }
    if (n.find("4/5") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C8>() : cached_ldpc_plan<DVB_S2_TABLE_B8>();
    }
    if (n.find("5/6") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C9>() : cached_ldpc_plan<DVB_S2_TABLE_B9>();
    }
    if (n.find("8/9") != std::string::npos) {
        return cfg.short_frame ? cached_ldpc_plan<DVB_S2_TABLE_C10>() : cached_ldpc_plan<DVB_S2_TABLE_B10>();
    }
    if (n.find("9/10") != std::string::npos) {
        return cached_ldpc_plan<DVB_S2_TABLE_B11>();
    }
    throw std::runtime_error("No LDPC table for " + cfg.name);
}

uint8_t rolloff_bits(RollOff rolloff)
{
    return static_cast<uint8_t>(rolloff);
}

} // namespace

uint8_t calculate_bbheader_crc8(const uint8_t* data, int len)
{
    uint8_t crc = 0;
    for (int i = 0; i < len; ++i) {
        crc ^= data[i];
        for (int b = 0; b < 8; ++b) {
            if (crc & 0x80) {
                crc = static_cast<uint8_t>((crc << 1) ^ 0xD5);
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}

void build_bbframe(std::vector<uint8_t>& frame_bits,
                   const std::vector<uint8_t>& payload_bytes,
                   const DVB_Params& cfg,
                   RollOff rolloff,
                   int syncd_bits)
{
    if (static_cast<int>(payload_bytes.size()) != cfg.payload_bytes()) {
        throw std::runtime_error("Invalid BBFRAME payload size for " + cfg.name);
    }

    frame_bits.assign(static_cast<size_t>(cfg.kbch), 0u);

    std::array<uint8_t, 10> header{};
    header[0] = static_cast<uint8_t>((3u << 6) | (1u << 5) | (1u << 4) | rolloff_bits(rolloff));
    header[1] = 0x00; // single stream
    const uint16_t upl = 188u * 8u;
    const uint16_t dfl = static_cast<uint16_t>(cfg.payload_bits());
    const uint16_t syncd = static_cast<uint16_t>(std::clamp(syncd_bits, 0, 188 * 8));
    header[2] = static_cast<uint8_t>((upl >> 8) & 0xffu);
    header[3] = static_cast<uint8_t>(upl & 0xffu);
    header[4] = static_cast<uint8_t>((dfl >> 8) & 0xffu);
    header[5] = static_cast<uint8_t>(dfl & 0xffu);
    header[6] = 0x47;
    header[7] = static_cast<uint8_t>((syncd >> 8) & 0xffu);
    header[8] = static_cast<uint8_t>(syncd & 0xffu);
    header[9] = calculate_bbheader_crc8(header.data(), 9);

    int bit_index = 0;
    for (uint8_t byte : header) {
        for (int bit = 7; bit >= 0; --bit) {
            frame_bits[bit_index++] = static_cast<uint8_t>((byte >> bit) & 0x1u);
        }
    }

    for (uint8_t byte : payload_bytes) {
        for (int bit = 7; bit >= 0; --bit) {
            frame_bits[bit_index++] = static_cast<uint8_t>((byte >> bit) & 0x1u);
        }
    }
}

void bb_randomize(std::vector<uint8_t>& bits, int kbch)
{
    std::call_once(g_bb_randomise_once, init_bb_randomiser);
    if (static_cast<int>(bits.size()) < kbch) {
        throw std::runtime_error("BB randomiser input shorter than Kbch");
    }
    for (int i = 0; i < kbch; ++i) {
        bits[static_cast<size_t>(i)] ^= g_bb_randomise[static_cast<size_t>(i)];
    }
}

void bch_init()
{
    std::call_once(g_bch_once, bch_init_impl);
}

void bch_encode(std::vector<uint8_t>& bits, const DVB_Params& cfg)
{
    bch_init();
    if (static_cast<int>(bits.size()) < cfg.kbch) {
        throw std::runtime_error("BCH encoder input shorter than Kbch");
    }
    bits.resize(static_cast<size_t>(cfg.nbch), 0u);

    unsigned int shift[6] = {};
    switch (bch_code_for(cfg)) {
    case BCHCode::N12:
        std::memset(shift, 0, sizeof(unsigned int) * 6);
        for (int i = 0; i < cfg.kbch; ++i) {
            const uint8_t in = bits[static_cast<size_t>(i)] & 0x1u;
            const uint8_t feedback = static_cast<uint8_t>(in ^ (shift[5] & 0x1u));
            reg_6_shift(shift);
            if (feedback != 0) {
                for (size_t w = 0; w < g_poly_n12.size(); ++w) {
                    shift[w] ^= g_poly_n12[w];
                }
            }
        }
        for (int n = 0; n < 192; ++n) {
            bits[static_cast<size_t>(cfg.kbch + n)] = static_cast<uint8_t>(shift[5] & 0x1u);
            reg_6_shift(shift);
        }
        break;
    case BCHCode::N10:
        std::memset(shift, 0, sizeof(unsigned int) * 5);
        for (int i = 0; i < cfg.kbch; ++i) {
            const uint8_t in = bits[static_cast<size_t>(i)] & 0x1u;
            const uint8_t feedback = static_cast<uint8_t>(in ^ (shift[4] & 0x1u));
            reg_5_shift(shift);
            if (feedback != 0) {
                for (size_t w = 0; w < g_poly_n10.size(); ++w) {
                    shift[w] ^= g_poly_n10[w];
                }
            }
        }
        for (int n = 0; n < 160; ++n) {
            bits[static_cast<size_t>(cfg.kbch + n)] = static_cast<uint8_t>(shift[4] & 0x1u);
            reg_5_shift(shift);
        }
        break;
    case BCHCode::N8:
        std::memset(shift, 0, sizeof(unsigned int) * 4);
        for (int i = 0; i < cfg.kbch; ++i) {
            const uint8_t in = bits[static_cast<size_t>(i)] & 0x1u;
            const uint8_t feedback = static_cast<uint8_t>(in ^ (shift[3] & 0x1u));
            reg_4_shift(shift);
            if (feedback != 0) {
                for (size_t w = 0; w < g_poly_n8.size(); ++w) {
                    shift[w] ^= g_poly_n8[w];
                }
            }
        }
        for (int n = 0; n < 128; ++n) {
            bits[static_cast<size_t>(cfg.kbch + n)] = static_cast<uint8_t>(shift[3] & 0x1u);
            reg_4_shift(shift);
        }
        break;
    case BCHCode::S12:
        std::memset(shift, 0, sizeof(unsigned int) * 6);
        for (int i = 0; i < cfg.kbch; ++i) {
            const uint8_t in = bits[static_cast<size_t>(i)] & 0x1u;
            const uint8_t feedback = static_cast<uint8_t>(in ^ ((shift[5] & 0x01000000u) ? 1u : 0u));
            reg_6_shift(shift);
            if (feedback != 0) {
                for (size_t w = 0; w < g_poly_s12.size(); ++w) {
                    shift[w] ^= g_poly_s12[w];
                }
            }
        }
        for (int n = 0; n < 168; ++n) {
            bits[static_cast<size_t>(cfg.kbch + n)] = static_cast<uint8_t>((shift[5] & 0x01000000u) ? 1u : 0u);
            reg_6_shift(shift);
        }
        break;
    }
}

void ldpc_encode(std::vector<uint8_t>& bits, const DVB_Params& cfg)
{
    const LDPCPlan& plan = select_ldpc_plan(cfg);
    if (plan.nbch != cfg.nbch) {
        throw std::runtime_error("LDPC table/K mismatch for " + cfg.name);
    }
    if (static_cast<int>(bits.size()) < cfg.nbch) {
        throw std::runtime_error("LDPC encoder input shorter than Nbch");
    }
    bits.resize(static_cast<size_t>(cfg.nldpc), 0u);

    std::vector<uint8_t> parity(static_cast<size_t>(plan.pbits), 0u);
    const size_t edges = plan.src.size();
    for (size_t i = 0; i < edges; ++i) {
        parity[plan.dst[i]] ^= bits[plan.src[i]];
    }
    for (int i = 1; i < plan.pbits; ++i) {
        parity[static_cast<size_t>(i)] ^= parity[static_cast<size_t>(i - 1)];
    }

    std::copy(parity.begin(), parity.end(), bits.begin() + cfg.nbch);
}
