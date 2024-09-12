#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\e[0;33m'
NC='\033[0m'

# default case (local, 4 processes)
systype='cluster'
buildtype='Release'
# check input
if [ "$#" -eq 0 ]; then
  # default case (local)
  echo -e "*** Building using default build type: ${YELLOW}$systype${NC}, build mode: ${YELLOW}$buildtype${NC} ***"
elif [ "$#" -eq 1 ]; then
  # specified type
  systype=$1
  if [[ "$systype" != "local" && "$systype" != "cluster" ]]; then
    echo -e "${RED}Error${NC}: Invalid build type '$systype'. Supported types: [local,cluster]"
    exit 1
  fi
  echo -e "*** Building using specified build type: ${YELLOW}$systype${NC}, default build mode: ${YELLOW}$buildtype${NC} ***"
elif [ "$#" -eq 2 ]; then
  systype=$1
  buildtype=$2
  if [[ "$systype" != "local" && "$systype" != "cluster" ]]; then
    echo -e "${RED}Error${NC}: Invalid build type '$systype'. Supported types: [local,cluster]"
    exit 1
  fi
  if [[ "$buildtype" != "Debug" && "$buildtype" != "Release" ]]; then
    echo -e "${RED}Error${NC}: Invalid build mode '$buildtype'. Supported modes: [Debug,Release]"
    exit 1
  fi
  echo -e "*** Building using specified build type: ${YELLOW}$systype${NC}, build mode: ${YELLOW}$buildtype${NC} ***"
else
  # wrong
  echo -e "${RED}Error${NC}: too many arguments. Usage: '$0' for default or '$0 <type> <mode>' where type=[local,cluster] and mode=[Debug,Release]"
  exit 1
fi

# create build directory
mkdir -p build

# build the code. It's the same for both local/cluster, but with different build types
cd build && cmake -D CMAKE_C_COMPILER=mpicc.mpich -D CMAKE_CXX_COMPILER=mpicxx.mpich -DCMAKE_BUILD_TYPE=$buildtype .. && cmake --build .
cd ..

# in a cluster of nodes specified by the 'nodes' file, do the following:
# 1) setup directories at each node
# 2) sync code in each node
# 3) create the hostfile
if [ "$systype" = "cluster" ]; then
  # the nodes(addresses) are stored in the file "nodes"
  nodefile="nodes"
  mapfile -t NODES < "$nodefile"
  # get current directory in host machine
  SOURCE_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
  # directory where the code should be placed on each node
  DEST_DIR="$SOURCE_DIR"
  echo -e "-- ${GREEN}Host directory: ${NC}" $SOURCE_DIR
  echo -e "-- ${GREEN}Dest directory: ${NC}" $DEST_DIR
  # make the same directory in the nodes (if it does not exist)
  for node in "${NODES[@]}"; do
    ssh "$node" "mkdir -p '$SOURCE_DIR'"
  done
  # sync function
  sync_code() {
    local node=$1
    rsync -q -avz -e "ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no" --delete --exclude="data/" $SOURCE_DIR/ ${node}:${DEST_DIR}/
  }
  echo "-- Syncing code across machines..."
  # sync for each node and append to hostfile
  hostfile="hostfile"
  > $hostfile
  for node in "${NODES[@]}"; do
    sync_code $node
    # append to hostfile
    echo "$node:1" >> $hostfile
  done
  echo -e "-- ${GREEN}Code sync across machines complete.${NC}"
fi

# set execution parameters
if [ "$systype" = "cluster" ]; then
  # cluster
  hostfile="--hostfile ../hostfile"
  type="CLUSTER"
  configfilepath="../config_cluster.ini"
else
  # local
  hostfile=""
  type="LOCAL"
  configfilepath="../config_local.ini"
fi

# create the execution script
runscript="run.sh"
echo -e "#!/bin/bash" > $runscript
echo -e "if [ \"\$#\" -eq 0 ]; then\n\techo \"Error, must specify number of nodes: Usage: '$0 <numnodes> <arguments>'\"\n\texit 1\nfi" >> $runscript
echo -e "numnodes=\$1" >> $runscript
echo -e "args=\${@:2}" >> $runscript
echo -e "numre='^[0-9]+$'" >> $runscript
echo -e "if ! [[ \$numnodes =~ \$numre ]] ; then\n\techo \"number of nodes must be a number\"\n\texit 1\nfi" >> $runscript
echo -e "echo \"Number of nodes: \$numnodes\"" >> $runscript
echo -e "# edit the hostfile" >> $runscript
echo -e "head -n \$numnodes $nodefile > temp_hostfile.txt && mv temp_hostfile.txt hostfile" >> $runscript
echo -e "# execute" >> $runscript

if [ "$buildtype" = "Debug" ]; then
  echo -e "cd build\nmpirun.mpich -np \$numnodes $hostfile gdb --args ./controller -t $type -c $configfilepath \$args" >> $runscript
else
  echo -e "cd build\nmpirun.mpich -np \$numnodes $hostfile ./controller -t $type -c $configfilepath \$args" >> $runscript
fi

echo -e "cd .." >> $runscript
chmod +x $runscript
echo -e "-- ${GREEN}Generated program script:${NC} $runscript"

# generate doxygen
DOC_SCRIPT="documentation.sh"
# Check if the documentation script exists and is executable
if [ -f "$DOC_SCRIPT" ] && [ -x "$DOC_SCRIPT" ]; then
    # execute
    ./"$DOC_SCRIPT"
else
    echo "Error: $DOC_SCRIPT does not exist or is not executable."
    exit 1
fi