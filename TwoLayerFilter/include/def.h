/******************************************************************************
 * Project:  ijoin
 * Purpose:  Compute interval overlap joins
 * Author:   Panagiotis Bouros, pbour@github.io
 ******************************************************************************
 * Copyright (c) 2017, Panagiotis Bouros
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/


#pragma once
#ifndef TWO_LAYER_DEF_H
#define TWO_LAYER_DEF_H

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <queue>
#include <fstream>
#include <future>
#include <unordered_set>
#include <unordered_map>
#include <chrono>
#include "omp.h"
#include <functional>
#include <array>

#include "SpatialLib.h"

using namespace std;


#ifdef __APPLE__

        typedef unsigned long int ulong;
        typedef unsigned short int ushort;
        typedef unsigned int uint;
#endif

namespace two_layer 
{
        
        extern spatial_lib::IntermediateFilterTypeE g_iFilterType;
        extern spatial_lib::MBRFilterTypeE g_mbrFilterType;
        extern spatial_lib::QueryTypeE g_queryType;

        #define EPS 1e-08

        typedef double Coord;
        typedef size_t RecordId;

        class Record;
        class Relation;

        inline int findReferenceCell1(double x, double y, double cellExtent, int numCellsPerDimension) {
                int xInt,yInt;

                xInt = (x + EPS)/cellExtent;
                yInt = (y + EPS)/cellExtent;

                return (yInt * numCellsPerDimension + xInt);
        };

        class Timer{
        private:
                using Clock = std::chrono::high_resolution_clock;
                Clock::time_point start_time, stop_time;

        public:
                Timer()
                {
                        start();
                }

                void start()
                {
                        start_time = Clock::now();
                }


                double getElapsedTimeInSeconds()
                {
                        return std::chrono::duration<double>(stop_time - start_time).count();
                }


                double stop()
                {
                        stop_time = Clock::now();
                        return getElapsedTimeInSeconds();
                }
        };
}



#endif
