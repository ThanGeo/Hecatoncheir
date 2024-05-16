#! /bin/bash

if [ ! -d "build" ]; then
    mkdir build
fi

# cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
cd build && cmake -DCMAKE_C_COMPILER=mpicc -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
