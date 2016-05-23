#!/bin/bash
# ============================================================================
#  GaianDB
#  Copyright IBM Corp. 2007-2014
#  
#  LICENSE: Eclipse Public License v1.0
#  http://www.eclipse.org/legal/epl-v10.html
# ============================================================================

# Do not move this script from the GaianDB home directory (where GAIANDB.jar is).
# If running this script from another directory, GDBH  must be set to the GaianDB home directory.

# For Unix, when double-clicking on this script, we want the home+workspace dirs to be the install location (NOT the user's home folder!):
[[ `dirname $0`!="$PWD" ]] && [[ -z "$GDBH" ]] && export GDBH=`dirname $0` && [[ -z "$GAIAN_WORKSPACE" ]] && export GAIAN_WORKSPACE=$GDBH
[[ -z "$GAIAN_WORKSPACE" ]] && export GAIAN_WORKSPACE=.

[[ -z $GDBH ]] && export GDBH=.

# Pass in as argument to this script the number of nodes to kick off, or set the following variable here:
numnodes=3

# Pass in as 2nd arg the option '-sameconfig' to make all nodes use the same configuration file when starting up.


# The following is equivalent to 
# launchGaianServer.sh -p 6414 &
# launchGaianServer.sh -p 6415 -c gaiandb_config2.properties &
# launchGaianServer.sh -p 6416 -c gaiandb_config3.properties &
# ...

cidx=1

[[ -n $1 ]] && numnodes=$1
[[ $* = *-sameconfig* ]] && unset cidx

echo "numnodes="$numnodes
[[ $numnodes -gt 0 ]] && nohup $GDBH/launchGaianServer.sh -p 6414 | sed "s,\(.\),gdb6414: \1," &

for ((i=2 ;i <= $numnodes; i++)) do let port=6413+$i; [[ -n $cidx ]] && cidx=$i; eval 'nohup $GDBH/launchGaianServer.sh -p $port -c $GAIAN_WORKSPACE/gaiandb_config$cidx.properties | sed "s,\(.\),gdb$port: \1," &'; done

echo "Started all nodes"
read # keeps the parent alive - useful when this script was double-clicked on
