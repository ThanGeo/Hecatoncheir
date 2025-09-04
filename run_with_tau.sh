#!/bin/bash

# A helper function to export TAU variables
set_tau_vars() {
  export TAU_PROFILE=1
  export TAU_OPTIONS="-tau_options=-optVerbose -optCompInst -optOpenMP -optMPI -optPROFILE"
  export TAU_TRACK_MPI_COMMUNICATORS=1
  export TAU_COMM_MATRIX=1
  export TAU_CALLPATH=1
  export TAU_CALLPATH_DEPTH=10
  export TAU_TRACK_COMMUNICATOR_EVENTS=1
  export TAU_MPI_PER_COMM_RANK=1
  export TAU_PROFILE_PREFIX="rank_"
  export TAU_SYNCHRONIZE_CLOCKS=1
  export TAU_TRACK_MESSAGE=1
  export TAU_MPI_TRACK_SPAWN=1
  # This variable is crucial for distinguishing spawned ranks
  export TAU_MPI_SPAWN_PATH_PREFIX="spawn_"
}

# Source this function to set the variables in the current shell
set_tau_vars

# Now, call mpirun without -genv. 
# The spawned processes will inherit the environment variables from the shell.
# Use full paths to the executable if needed.
mpirun tau_exec build/driver/driver