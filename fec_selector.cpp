#include "dvbs2_tx.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace {

std::string normalise_key(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return value;
}

} // namespace

DVB_Params get_fec_config(const std::string& selection)
{
    static const std::unordered_map<std::string, DVB_Params> configs = {
        {"QPSK_1/4_S",  {"QPSK_1/4_S",  Modulation::QPSK,  1,  3072,  3240, 16200, 168, true}},
        {"QPSK_1/3_S",  {"QPSK_1/3_S",  Modulation::QPSK,  2,  5232,  5400, 16200, 168, true}},
        {"QPSK_2/5_S",  {"QPSK_2/5_S",  Modulation::QPSK,  3,  6312,  6480, 16200, 168, true}},
        {"QPSK_1/2_S",  {"QPSK_1/2_S",  Modulation::QPSK,  4,  7032,  7200, 16200, 168, true}},
        {"QPSK_3/5_S",  {"QPSK_3/5_S",  Modulation::QPSK,  5,  9552,  9720, 16200, 168, true}},
        {"QPSK_2/3_S",  {"QPSK_2/3_S",  Modulation::QPSK,  6, 10632, 10800, 16200, 168, true}},
        {"QPSK_3/4_S",  {"QPSK_3/4_S",  Modulation::QPSK,  7, 11712, 11880, 16200, 168, true}},
        {"QPSK_4/5_S",  {"QPSK_4/5_S",  Modulation::QPSK,  8, 12432, 12600, 16200, 168, true}},
        {"QPSK_5/6_S",  {"QPSK_5/6_S",  Modulation::QPSK,  9, 13152, 13320, 16200, 168, true}},
        {"QPSK_8/9_S",  {"QPSK_8/9_S",  Modulation::QPSK, 10, 14232, 14400, 16200, 168, true}},

        {"QPSK_1/4_N",  {"QPSK_1/4_N",  Modulation::QPSK,  1, 16008, 16200, 64800, 192, false}},
        {"QPSK_1/3_N",  {"QPSK_1/3_N",  Modulation::QPSK,  2, 21408, 21600, 64800, 192, false}},
        {"QPSK_2/5_N",  {"QPSK_2/5_N",  Modulation::QPSK,  3, 25728, 25920, 64800, 192, false}},
        {"QPSK_1/2_N",  {"QPSK_1/2_N",  Modulation::QPSK,  4, 32208, 32400, 64800, 192, false}},
        {"QPSK_3/5_N",  {"QPSK_3/5_N",  Modulation::QPSK,  5, 38688, 38880, 64800, 192, false}},
        {"QPSK_2/3_N",  {"QPSK_2/3_N",  Modulation::QPSK,  6, 43040, 43200, 64800, 160, false}},
        {"QPSK_3/4_N",  {"QPSK_3/4_N",  Modulation::QPSK,  7, 48408, 48600, 64800, 192, false}},
        {"QPSK_4/5_N",  {"QPSK_4/5_N",  Modulation::QPSK,  8, 51648, 51840, 64800, 192, false}},
        {"QPSK_5/6_N",  {"QPSK_5/6_N",  Modulation::QPSK,  9, 53840, 54000, 64800, 160, false}},
        {"QPSK_8/9_N",  {"QPSK_8/9_N",  Modulation::QPSK, 10, 57472, 57600, 64800, 128, false}},
        {"QPSK_9/10_N", {"QPSK_9/10_N", Modulation::QPSK, 11, 58192, 58320, 64800, 128, false}},

        {"8PSK_3/5_S",  {"8PSK_3/5_S",  Modulation::PSK8, 12,  9552,  9720, 16200, 168, true}},
        {"8PSK_2/3_S",  {"8PSK_2/3_S",  Modulation::PSK8, 13, 10632, 10800, 16200, 168, true}},
        {"8PSK_3/4_S",  {"8PSK_3/4_S",  Modulation::PSK8, 14, 11712, 11880, 16200, 168, true}},
        {"8PSK_5/6_S",  {"8PSK_5/6_S",  Modulation::PSK8, 15, 13152, 13320, 16200, 168, true}},
        {"8PSK_8/9_S",  {"8PSK_8/9_S",  Modulation::PSK8, 16, 14232, 14400, 16200, 168, true}},

        {"8PSK_3/5_N",  {"8PSK_3/5_N",  Modulation::PSK8, 12, 38688, 38880, 64800, 192, false}},
        {"8PSK_2/3_N",  {"8PSK_2/3_N",  Modulation::PSK8, 13, 43040, 43200, 64800, 160, false}},
        {"8PSK_3/4_N",  {"8PSK_3/4_N",  Modulation::PSK8, 14, 48408, 48600, 64800, 192, false}},
        {"8PSK_5/6_N",  {"8PSK_5/6_N",  Modulation::PSK8, 15, 53840, 54000, 64800, 160, false}},
        {"8PSK_8/9_N",  {"8PSK_8/9_N",  Modulation::PSK8, 16, 57472, 57600, 64800, 128, false}},
        {"8PSK_9/10_N", {"8PSK_9/10_N", Modulation::PSK8, 17, 58192, 58320, 64800, 128, false}},

        {"16APSK_2/3_S",  {"16APSK_2/3_S",  Modulation::APSK16, 18, 10632, 10800, 16200, 168, true}},
        {"16APSK_3/4_S",  {"16APSK_3/4_S",  Modulation::APSK16, 19, 11712, 11880, 16200, 168, true}},
        {"16APSK_4/5_S",  {"16APSK_4/5_S",  Modulation::APSK16, 20, 12432, 12600, 16200, 168, true}},
        {"16APSK_5/6_S",  {"16APSK_5/6_S",  Modulation::APSK16, 21, 13152, 13320, 16200, 168, true}},
        {"16APSK_8/9_S",  {"16APSK_8/9_S",  Modulation::APSK16, 22, 14232, 14400, 16200, 168, true}},

        {"16APSK_2/3_N",  {"16APSK_2/3_N",  Modulation::APSK16, 18, 43040, 43200, 64800, 160, false}},
        {"16APSK_3/4_N",  {"16APSK_3/4_N",  Modulation::APSK16, 19, 48408, 48600, 64800, 192, false}},
        {"16APSK_4/5_N",  {"16APSK_4/5_N",  Modulation::APSK16, 20, 51648, 51840, 64800, 192, false}},
        {"16APSK_5/6_N",  {"16APSK_5/6_N",  Modulation::APSK16, 21, 53840, 54000, 64800, 160, false}},
        {"16APSK_8/9_N",  {"16APSK_8/9_N",  Modulation::APSK16, 22, 57472, 57600, 64800, 128, false}},
        {"16APSK_9/10_N", {"16APSK_9/10_N", Modulation::APSK16, 23, 58192, 58320, 64800, 128, false}},

        {"32APSK_3/4_S",  {"32APSK_3/4_S",  Modulation::APSK32, 24, 11712, 11880, 16200, 168, true}},
        {"32APSK_4/5_S",  {"32APSK_4/5_S",  Modulation::APSK32, 25, 12432, 12600, 16200, 168, true}},
        {"32APSK_5/6_S",  {"32APSK_5/6_S",  Modulation::APSK32, 26, 13152, 13320, 16200, 168, true}},
        {"32APSK_8/9_S",  {"32APSK_8/9_S",  Modulation::APSK32, 27, 14232, 14400, 16200, 168, true}},

        {"32APSK_3/4_N",  {"32APSK_3/4_N",  Modulation::APSK32, 24, 48408, 48600, 64800, 192, false}},
        {"32APSK_4/5_N",  {"32APSK_4/5_N",  Modulation::APSK32, 25, 51648, 51840, 64800, 192, false}},
        {"32APSK_5/6_N",  {"32APSK_5/6_N",  Modulation::APSK32, 26, 53840, 54000, 64800, 160, false}},
        {"32APSK_8/9_N",  {"32APSK_8/9_N",  Modulation::APSK32, 27, 57472, 57600, 64800, 128, false}},
        {"32APSK_9/10_N", {"32APSK_9/10_N", Modulation::APSK32, 28, 58192, 58320, 64800, 128, false}},
    };

    const std::string key = normalise_key(selection);
    if (key == "QPSK_9/10_S" || key == "8PSK_9/10_S" || key == "16APSK_9/10_S" || key == "32APSK_9/10_S") {
        throw std::invalid_argument("DVB-S2 short FECFRAME does not define 9/10 coding");
    }

    const auto it = configs.find(key);
    if (it == configs.end()) {
        throw std::invalid_argument("Unsupported DVB-S2 MODCOD: " + selection);
    }
    return it->second;
}

RollOff parse_rolloff(const std::string& value)
{
    const std::string key = value;
    if (key == "0.35" || key == "35" || key == "RO_35") {
        return RollOff::RO_35;
    }
    if (key == "0.25" || key == "25" || key == "RO_25") {
        return RollOff::RO_25;
    }
    if (key == "0.20" || key == "20" || key == "RO_20") {
        return RollOff::RO_20;
    }
    throw std::invalid_argument("Unsupported roll-off: " + value);
}

const char* rolloff_to_string(RollOff rolloff)
{
    switch (rolloff) {
    case RollOff::RO_35:
        return "0.35";
    case RollOff::RO_25:
        return "0.25";
    case RollOff::RO_20:
        return "0.20";
    }
    return "0.20";
}

float rolloff_to_alpha(RollOff rolloff)
{
    switch (rolloff) {
    case RollOff::RO_35:
        return 0.35f;
    case RollOff::RO_25:
        return 0.25f;
    case RollOff::RO_20:
        return 0.20f;
    }
    return 0.20f;
}
