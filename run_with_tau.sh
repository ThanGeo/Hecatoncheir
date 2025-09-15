#!/bin/bash

# A helper function to export TAU variables
set_tau_vars() {
  # time+memory profiling
  export TAU_PROFILE=1
  export TAU_TRACK_MEMORY=1
  export TAU_TRACK_HEAP=1
  export TAU_TRACK_MEMORY_FOOTPRINT=1
  export TAU_SHOW_MEMORY_FUNCTIONS=1

  # snapshot data, one profile per node
  export TAU_PROFILE_FORMAT=snapshot
  # merge data (threads)
  # export TAU_PROFILE_FORMAT=merged
  # export TAU_OPTIONS="-tau_options=-optVerbose -optCompInst -optOpenMP -optMPI -optPROFILE"
  
  # communications
  export TAU_TRACK_MPI_COMMUNICATORS=1
  export TAU_COMM_MATRIX=1
  export TAU_TRACK_COMMUNICATOR_EVENTS=1
  export TAU_MPI_PER_COMM_RANK=1
  export TAU_TRACK_MESSAGE=1
  export TAU_TRACK_MESSAGE_SIZE=1
  export TAU_MPI_TRACK_SPAWN=1
  export TAU_SYNCHRONIZE_CLOCKS=1

  # optional
  # export TAU_CALLPATH=1
  # export TAU_CALLPATH_DEPTH=10
}

# Source this function to set the variables in the current shell
set_tau_vars

# Now, call mpirun without -genv. 
# The spawned processes will inherit the environment variables from the shell.
# Use full paths to the executable if needed.
# mpirun.mpich tau_exec -memory build/driver/driver
mpirun.mpich tau_exec build/driver/driver