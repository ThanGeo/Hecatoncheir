#!/bin/bash

# default execution
cd build
mpirun.mpich --hostfile ../hostfile -np 2 ./controller -d CLUSTER -c ../config_cluster.ini $@
cd ..