#!/bin/bash

# default execution
cd build
mpirun.mpich --hostfile ../hostfile -np 3 ./controller -t CLUSTER -c ../config_cluster.ini $@
cd ..