// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "FiberSchedThread.h"
#include "runtime/Stats.h"
#include "primitives/Assert.h"
#include "sys/XLog.h"
#include "xcalar/compute/localtypes/ParentChild.pb.h"
#include "std_chrono_compat.h"
#include "util/Standard.h"
#include "util/Stopwatch.h"
#include "util/Standard.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <numeric>

// TODO(Oleg): move this out into a lib.
namespace math
{
template <typename I>
double
mean(I begin, I end)
{
    DCHECK(begin != end);
    double sum = 0;
    size_t count = 0;
    while (begin != end) {
        sum += *begin;
        ++begin;
        ++count;
    }

    return sum / count;
}

template <typename I>
double
stdDev(I begin, I end)
{
    DCHECK(begin != end);
    auto smean = mean(begin, end);
    size_t count = std::distance(begin, end);
    if (count == 1) return 0.0;

    double sq_sum = 0.0;
    while (begin != end) {
        sq_sum += (*begin - smean) * (*begin - smean);
        ++begin;
    }

    return sqrt(sq_sum / (count - 1));
}
}  // namespace math

StatsCollector *StatsCollector::instance_ = nullptr;

////////////////////////////////////// Stats  //////////////////////////////////

void
Stats::start()
{
    DCHECK(id_ == -1);
    id_ = StatsCollector::instance().start(shared_from_this());
    DCHECK(id_ > -1);
}

void
Stats::finish()
{
    counters_.finish = std::chrono::steady_clock::now();
}

////////////////////////////////// StatsCollector //////////////////////////////

struct StatsCollector::State {
    // Sequentially increasing ID for each piece of stats.
    std::atomic<int64_t> next_id_{0};
};

StatsCollector::StatsCollector() : state_(std::make_unique<State>()) {}

void
StatsCollector::construct()
{
    DCHECK(instance_ == nullptr);
    instance_ = new StatsCollector;
}

void
StatsCollector::shutdown()
{
    DCHECK(instance_ != nullptr);
    delete instance_;
    instance_ = nullptr;
}

int64_t
StatsCollector::start(std::shared_ptr<Stats> stats)
{
    return state_->next_id_++;
}

Status
StatsCollector::exportFinished(RuntimeHistogramsResponse *resp) const
{
    DCHECK(resp != nullptr);
    auto hist = resp->add_histograms();
    hist->set_duration_sec(60);

    std::map<std::string, stats::Digest> digest;
    Runtime::get()->extractRuntimeStats(&digest);

    // Go through the op objects, lock them and extract stats data.
    for (auto &entry : digest) {
        // Extract the samples so that we can calculate the mean and stddev.
        const size_t count = entry.second.duration.size();
        DCHECK(count > 0);

        // Export the name/count.
        auto item = hist->add_items();
        item->set_name(entry.first);
        item->set_count(count);

        // Calculate and export suspension stats.
        item->set_mean_suspensions(math::mean(entry.second.suspensions.begin(),
                                              entry.second.suspensions.end()));
        std::chrono::microseconds meanSuspended(
            static_cast<int64_t>(math::mean(entry.second.suspendedTime.begin(),
                                            entry.second.suspendedTime.end())));
        item->set_mean_suspended_time_us(meanSuspended.count());

        // Calculate and export the arithmetic mean for latency.
        std::chrono::microseconds meanDuration(
            static_cast<int64_t>(math::mean(entry.second.duration.begin(),
                                            entry.second.duration.end())));
        item->set_mean_duration_us(meanDuration.count());

        // Calculate and export the std dev for latency.
        std::chrono::microseconds stddev(
            static_cast<int64_t>(math::stdDev(entry.second.duration.begin(),
                                              entry.second.duration.end())));
        item->set_duration_stddev(stddev.count());

        // Calculate and export 95th and 99th percentiles for latency.
        std::sort(entry.second.duration.begin(), entry.second.duration.end());
        auto it = entry.second.duration.begin() +
                  entry.second.duration.size() * 95 / 100;
        item->set_duration_95th_us(*it);
        it = entry.second.duration.begin() +
             entry.second.duration.size() * 99 / 100;
        item->set_duration_99th_us(*it);

        // Export the mean infra time.
        int64_t lockingTimeus = std::accumulate(entry.second.infraTime.begin(),
                                                entry.second.infraTime.end(),
                                                0ull);
        item->set_mean_locking_time_us(lockingTimeus / count);
    }

    return StatusOk;
}
