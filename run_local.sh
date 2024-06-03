#!/bin/bash

# default execution
cd build
mpirun.mpich -np 2 ./controller $@
cd ..