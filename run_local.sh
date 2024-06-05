#!/bin/bash

# default execution
cd build
mpirun.mpich -np 10 ./controller -c ../config_local.ini $@
cd ..