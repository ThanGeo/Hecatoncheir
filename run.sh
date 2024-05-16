#!/bin/bash

if [ $# -eq 0 ] ; then
    # default execution
    cd build
    # echo mpirun.mpich -np 10 ./controller
    mpirun.mpich -hostfile ../hostfile -np 10 ./controller;
    cd ..
fi