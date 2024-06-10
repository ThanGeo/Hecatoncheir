#include "utils.h"

namespace APRIL
{
    // rotate/flip a quadrant appropriately
    inline void rot(uint32_t n, uint32_t &x, uint32_t &y, uint32_t rx, uint32_t ry) {
        if (ry == 0) {
            if (rx == 1) {
                x = n-1 - x;
                y = n-1 - y;
            }

            //Swap x and y
            uint32_t t  = x;
            x = y;
            y = t;
        }
    }

    // convert (x,y) to d
    uint32_t xy2d (uint32_t n, uint32_t x, uint32_t y) {
        uint32_t rx, ry, s;
        uint32_t d=0;
        for (s=n/2; s>0; s/=2) {
            rx = (x & s) > 0;
            ry = (y & s) > 0;
            d += s * s * ((3 * rx) ^ ry);
            rot(s, x, y, rx, ry);
        }
        return d;
    }

    // convert d to (x,y)
    void d2xy(uint32_t n, uint32_t d, uint32_t &x, uint32_t &y) {
        uint32_t rx, ry, s, t=d;
        x = y = 0;
        for (s=1; s<n; s*=2) {
            rx = 1 & (t/2);
            ry = 1 & (t ^ rx);
            rot(s, x, y, rx, ry);
            x += s * rx;
            y += s * ry;
            t /= 4;
        }
    }

    // development only, works just for O5O6EU
    // xMin = -10.594201
    // yMin = 34.761799
    // xMax = 45.893550
    // yMax = 71.170701
    static void mapHilbertXYToCoords(double &x, double &y, uint32_t cellsPerDim){
        // formula to map X from range [A,B] to new range [C,D]
        // NEW_X = ((X-A) / (B-A)) * (D-C) + C

        x = ((x) / (cellsPerDim-1)) * (45.893550 - (-10.594201)) + (-10.594201);
        y = ((y) / (cellsPerDim-1)) * (71.170701 - 34.761799) + 34.761799;

    }

    void printAPRIL(spatial_lib::AprilDataT aprilData) {
        printf("%u A and %u F intervals.\n", aprilData.numIntervalsALL, aprilData.numIntervalsFULL);
        printf("Cell spacings: x = %f, y = %f\n", (45.893550 - (-10.594201)) / 65536, (71.170701 - 34.761799) / 65536);
        // ALL
        for (int i=0; i<aprilData.numIntervalsALL*2; i+=2) {
            uint start = aprilData.intervalsALL.at(i);
            uint end = aprilData.intervalsALL.at(i+1);
            uint x,y;
            double coord_x,coord_y;

            for(int cell = start; cell < end; cell++) {
                // d2xy(65536, cell, x, y);
                // coord_x = x;
                // coord_y = y;
                // mapHilbertXYToCoords(coord_x, coord_y, 65536);
                // printf("(%f,%f)\n", coord_x, coord_y);
            }
        }
        // FULL
        for (int i=0; i<aprilData.numIntervalsFULL*2; i+=2) {
            uint start = aprilData.intervalsFULL.at(i);
            uint end = aprilData.intervalsFULL.at(i+1);
            uint x,y;
            double coord_x,coord_y;

            for(int cell = start; cell < end; cell++) {
                // d2xy(65536, cell, x, y);
                // coord_x = x;
                // coord_y = y;
                // mapHilbertXYToCoords(coord_x, coord_y, 65536);
                // printf("(%f,%f)\n", coord_x, coord_y);
            }
        }
    }
}