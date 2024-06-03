#!/bin/bash

# default execution
cd build
mpirun.mpich -np 10 ./controller $@
cd ..