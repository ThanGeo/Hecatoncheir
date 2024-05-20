#! /bin/bash

if [ ! -d "build" ]; then
    mkdir build
fi

cd build && cmake -D CMAKE_C_COMPILER=mpicc.mpich -D CMAKE_CXX_COMPILER=mpicxx.mpich -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
# cd build && cmake -D CMAKE_C_COMPILER=mpicc.openmpi -D CMAKE_CXX_COMPILER=mpicxx.openmpi -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
