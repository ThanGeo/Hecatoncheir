#include "timer.h"

namespace spatial_lib
{
    namespace time
    {
        TimerT g_timer;

        void markMbrTimer() {
            g_timer.mbrTimer = clock();
        }

        void markiFilterTimer() {
            g_timer.iFilterTimer = clock();
        }

        void markRefinementFilterTimer() {
            g_timer.refTimer = clock();
        }

        clock_t getNewTimer() {
            return clock();
        }

        double getElapsedTime(clock_t &timer) {
            return (clock() - timer) / (double) CLOCKS_PER_SEC;
        }
    }
}