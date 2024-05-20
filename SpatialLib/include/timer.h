#ifndef SPATIAL_LIB_TIMER_H
#define SPATIAL_LIB_TIMER_H

#include <ctime>

namespace spatial_lib
{
    namespace time
    {
        typedef struct Timer {
            clock_t mbrTimer;
            clock_t iFilterTimer;
            clock_t refTimer;
        } TimerT;

        extern TimerT g_timer;

        // marks current timestamp for mbr filter
        void markMbrTimer();
        // marks current timestamp for intermediate filter
        void markiFilterTimer();
        // marks current timestamp for refinement
        void markRefinementFilterTimer();
        // returns a new timer with current timestamp
        clock_t getNewTimer();
        // returns total time in seconds for input timer
        double getElapsedTime(clock_t &timer);

    }
}


#endif 