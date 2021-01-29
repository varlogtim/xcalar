// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef STOPWATCH_H
#define STOPWATCH_H

#include <time.h>
#include "primitives/Primitives.h"

//
// A simple class for timing how long an operation takes. Starts timer on
// construction. Stops timer when you call stop().
//

class Stopwatch final
{
  public:
    Stopwatch(clockid_t clockIdParam = CLOCK_MONOTONIC) : clockId_(clockIdParam)
    {
        restart();
    }

    void restart()
    {
        verify(clock_gettime(clockId_, &timeStart_) == 0);
        verify(clock_gettime(CLOCK_REALTIME, &timeStartRealTime_) == 0);
        timeEnd_.tv_sec = 0;
        timeEnd_.tv_nsec = 0;
        timeEndRealTime_.tv_sec = 0;
        timeEndRealTime_.tv_nsec = 0;
    }

    void stop()
    {
        verify(clock_gettime(clockId_, &timeEnd_) == 0);
        verify(clock_gettime(CLOCK_REALTIME, &timeEndRealTime_) == 0);
    }

    inline unsigned long long getElapsedNSecs() const
    {
        if (timeEnd_.tv_sec == 0) {
            // timer hasn't stopped yet
            return 0;
        }
        return getElapsedNSecs(this->timeStart_, this->timeEnd_);
    }

    inline unsigned long long getElapsedMSecs() const
    {
        return getCurElapsedMSecs() / NSecsPerMSec;
    }

    inline unsigned long long getStartRealTime() const
    {
        return getStartRealTime(this->timeStartRealTime_);
    }

    inline unsigned long long getEndRealTime() const
    {
        return getEndRealTime(this->timeEndRealTime_);
    }

    inline unsigned long long getCurElapsedNSecs() const
    {
        if (timeEnd_.tv_sec != 0) {
            // timer has stopped
            return getElapsedNSecs(this->timeStart_, this->timeEnd_);
        }

        struct timespec curTime;
        verify(clock_gettime(clockId_, &curTime) == 0);
        return getElapsedNSecs(this->timeStart_, curTime);
    }

    inline unsigned long long getCurTimeStamp() const
    {
        if (timeEndRealTime_.tv_sec != 0) {
            // timer has stopped
            return getEndRealTime(this->timeEndRealTime_);
        }

        struct timespec curTime;
        verify(clock_gettime(CLOCK_REALTIME, &curTime) == 0);
        return curTime.tv_sec * NSecsPerSec + curTime.tv_nsec;
        ;
    }

    inline unsigned long long getCurElapsedMSecs() const
    {
        return getCurElapsedNSecs() / NSecsPerMSec;
    }

    void getPrintableTime(unsigned long &hours,
                          unsigned long &minutesLeftOver,
                          unsigned long &secondsLeftOver,
                          unsigned long &millisecondsLeftOver) const
    {
        auto msecsTotal = this->getElapsedNSecs() / NSecsPerMSec;
        auto secsTotal = msecsTotal / MSecsPerSec;
        auto minsTotal = secsTotal / SecsPerMinute;

        hours = minsTotal / MinutesPerHour;
        minutesLeftOver = minsTotal - (hours * MinutesPerHour);
        secondsLeftOver = secsTotal - (hours * MinutesPerHour * SecsPerMinute) -
                          (minutesLeftOver * SecsPerMinute);
        millisecondsLeftOver =
            msecsTotal -
            (hours * MinutesPerHour * SecsPerMinute * MSecsPerSec) -
            (minutesLeftOver * SecsPerMinute * MSecsPerSec) -
            (secondsLeftOver * MSecsPerSec);
    }

  private:
    clockid_t clockId_;
    struct timespec timeStart_;
    struct timespec timeEnd_;
    struct timespec timeStartRealTime_;
    struct timespec timeEndRealTime_;

    inline unsigned long long getElapsedNSecs(struct timespec start,
                                              struct timespec end) const
    {
        unsigned long long endNsec = end.tv_sec * NSecsPerSec + end.tv_nsec;
        unsigned long long startNsec =
            start.tv_sec * NSecsPerSec + start.tv_nsec;
        if (endNsec <= startNsec) {
            return 0;
        } else {
            return endNsec - startNsec;
        }
    }

    inline unsigned long long getStartRealTime(struct timespec start) const
    {
        unsigned long long startNSec =
            start.tv_sec * NSecsPerSec + start.tv_nsec;
        return startNSec;
    }

    inline unsigned long long getEndRealTime(struct timespec end) const
    {
        unsigned long long endNSec = end.tv_sec * NSecsPerSec + end.tv_nsec;
        return endNSec;
    }

    // Disallowed.
    Stopwatch(const Stopwatch &) = delete;
    Stopwatch &operator=(const Stopwatch &) = delete;
};

#endif  // STOPWATCH_H
