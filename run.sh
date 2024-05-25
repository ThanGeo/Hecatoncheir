#!/bin/bash

# default execution
cd build
# echo mpirun.mpich -np 10 ./controller
mpirun.mpich --hostfile ../hostfile -np 2 ./controller $@
# mpirun.openmpi --hostfile ../hostfile -np 10 ./controller;
cd ..