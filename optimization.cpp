#include "dvbs2_tx.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

void set_thread_affinity(int core)
{
    const int num_cores = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
    if (num_cores <= 0) {
        return;
    }

    if (core < 0) {
        core = 0;
    }
    if (core >= num_cores) {
        core = num_cores - 1;
    }

    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);

    const int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
        std::cerr << "[warn] CPU affinity not set: " << std::strerror(rc) << std::endl;
    }
}

void generate_rrc_taps(std::vector<float>& taps, int num_taps, int sps, float alpha)
{
    taps.assign(static_cast<size_t>(num_taps), 0.0f);

    const double center = 0.5 * static_cast<double>(num_taps - 1);
    double sum = 0.0;

    for (int i = 0; i < num_taps; ++i) {
        const double t = (static_cast<double>(i) - center) / static_cast<double>(sps);
        double h = 0.0;

        if (std::abs(t) < 1.0e-12) {
            h = 1.0 - alpha + (4.0 * alpha / M_PI);
        } else if (std::abs(std::abs(4.0 * alpha * t) - 1.0) < 1.0e-9) {
            const double term1 = (1.0 + 2.0 / M_PI) * std::sin(M_PI / (4.0 * alpha));
            const double term2 = (1.0 - 2.0 / M_PI) * std::cos(M_PI / (4.0 * alpha));
            h = (alpha / std::sqrt(2.0)) * (term1 + term2);
        } else {
            const double num = std::sin(M_PI * t * (1.0 - alpha)) +
                               (4.0 * alpha * t) * std::cos(M_PI * t * (1.0 + alpha));
            const double den = M_PI * t * (1.0 - std::pow(4.0 * alpha * t, 2.0));
            h = num / den;
        }

        const double w = 0.54 - 0.46 * std::cos((2.0 * M_PI * i) / static_cast<double>(num_taps - 1));
        taps[static_cast<size_t>(i)] = static_cast<float>(h * w);
        sum += taps[static_cast<size_t>(i)];
    }

    if (sum != 0.0) {
        const double norm = static_cast<double>(sps) / sum;
        for (float& tap : taps) {
            tap = static_cast<float>(tap * norm);
        }
    }
}
