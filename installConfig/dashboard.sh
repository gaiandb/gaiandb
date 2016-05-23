#!/bin/bash
# ============================================================================
#  GaianDB
#  Copyright IBM Corp. 2007-2014
#  
#  LICENSE: Eclipse Public License v1.0
#  http://www.eclipse.org/legal/epl-v10.html
# ============================================================================

# For Unix, when double-clicking on this script, we want the home+workspace dirs to be the install location (NOT the user's home folder!):
[[ `dirname $0`!="$PWD" ]] && [[ -z "$GDBH" ]] && export GDBH=`dirname $0` && [[ -z "$GAIAN_WORKSPACE" ]] && export GAIAN_WORKSPACE=$GDBH

[[ -z $GDBH ]] && export GDBH=.
GDBL=$GDBH/lib

# Obsolete: export CLASSPATH="$GDBL/GAIANDB.jar:$GDBL/GAIANDB-tools.jar:$GDBL/prefusetrimmed.jar:$GDBL/derby.jar:$GDBL/derbyclient.jar"

# Add all jars in $GDBL
function findLibjars { for j in $1/*.jar; do export CLASSPATH="$CLASSPATH:$j"; done; }
findLibjars $GDBL

echo java -cp "$CLASSPATH" com.ibm.gaiandb.apps.dashboard.Dashboard
java -cp "$CLASSPATH" com.ibm.gaiandb.apps.dashboard.Dashboard

