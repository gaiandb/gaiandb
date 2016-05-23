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
# Note that the workspace will be the execution directory: where the config file, database, log file, 
# sysinfo file and other user data files are located.

function findLibjars { for j in $1/*.jar; do export CLASSPATH="$CLASSPATH:$j"; done; }

# Set your node name here
node=

args=$*

[[ -z "$args" && -n "$node" ]] && args=-n $node

echo Launching Server...

# For Unix, when double-clicking on this script, we want the home+workspace dirs to be the install location (NOT the user's home folder!):
[[ `dirname $0`!="$PWD" ]] && [[ -z "$GDBH" ]] && export GDBH=`dirname $0` && [[ -z "$GAIAN_WORKSPACE" ]] && export GAIAN_WORKSPACE=$GDBH

[[ -z "$GDBH" ]] && export GDBH=.
GDBL=$GDBH/lib

export SYSCP="$CLASSPATH"

# ===================================== automatic jar discovery (add user jars to 'ext' folder) =================================

findLibjars $GDBL
findLibjars $GDBL/ext

# ===================================== static jar settings (obsolete) ==========================================================

if false; then

echo "Setting CLASSPATH explicitly (obsolete)";

export CLASSPATH="$GDBL:$GDBL/GAIANDB.jar:$GDBL/db2jcutdown.jar:$GDBL/derbyclient.jar:$GDBL/derbynet.jar:$GDBL/derbytrimmed.jar"
# Add GaianDB WPML plugin
export CLASSPATH="$CLASSPATH:$GDBL/wpml-pfg.jar"

# Optional Jars for interfacing to Omnifind Enterprise Edition and the Micro-Broker (to be obtained externally - see readme)
# export CLASSPATH="$CLASSPATH:$GDBL/siapi.jar:$GDBL/esapi.jar"
#:$GDBL/wmqtt.jar"

# Optional Jars for WPML Policy Management libraries (to be obtained externally - see readme)
# WPML=C:/Code/wpml-1.2/lib
# export CLASSPATH="$CLASSPATH:$WPML/wpml.jar:$WPML/JSON4J.jar:$WPML/arenatk.jar:$WPML/antlr-2.7.7.jar"


# Optional Jars for JDBC access to DB2, MySQL, MS SQLServer data sources.
export CLASSPATH="$CLASSPATH:$GDBL/db2jcc.jar:$GDBL/db2jcc_license_cu.jar"
export CLASSPATH="$CLASSPATH:$GDBL/ojdbc14.jar"

# DB2_HOME=/home/ibm/db2/V9.7
# export CLASSPATH="$CLASSPATH:$DB2_HOME/java/db2jcc.jar:$DB2_HOME/java/db2jcc_license_cu.jar"
# export CLASSPATH="$CLASSPATH:$HOME/MSSQLServer/JDBCDriver/driver2005/Micros~1/sqljdbc_1.0/enu/sqljdbc.jar"
# export CLASSPATH="$CLASSPATH:$HOME/mysql-5.0/mysql-connector-java-5.1.5/mysql-connector-java-5.1.5-bin.jar"

# Apache - POI jars for spreadsheet federation
#export CLASSPATH="$CLASSPATH:$GDBL/geronimo-stax-api_1.0_spec-1.0.jar:$GDBL/poi-ooxml-schemas-3.6-20091214.jar:$GDBL/dom4j-1.6.1.jar:$GDBL/poi-3.6-20091214.jar:$GDBL/poi-ooxml-3.6-20091214.jar:$GDBL/xmlbeans-2.3.0.jar"

fi

# ===================================== static jars setting =====================================================================


export CLASSPATH="$CLASSPATH:$SYSCP"

# JAVA_CMD: Java command - may include path

JAVA_CMD=`[[ -z "$JAVA_HOME" ]] && echo java || echo $JAVA_HOME/bin/java`

# JAVA_OPTS: Default JVM arguments, e.g. heap size

[[ -z "$JAVA_OPTS" ]] && JAVA_OPTS=-Xmx256m

# GAIAN_WORKSPACE: Gaian workspace folder 
# This folder MUST CONTAIN AT LEAST gaiandb_config.properties. Also include derby.properties to enforce authentication (optional).
# Log files and physical Derby database will also be created here.

[[ -z "$GAIAN_WORKSPACE" ]] && GAIAN_WORKSPACE=.

# REMOTE DEBUGGING:                               JAVA_OPTS="-Xmx128m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044"
# GENERATE HEAPDUMP WHEN OutOfMemoryError OCCURS: JAVA_OPTS="-Xmx128m -Xdump:heap:events=systhrow,filter=java/lang/OutOfMemoryError"

while true; do

	# *** START GAIAN ***
	
	"$JAVA_CMD" $JAVA_OPTS -Dderby.system.home="$GAIAN_WORKSPACE" -cp "$CLASSPATH" com.ibm.gaiandb.GaianNode $args
	
	rc=$?
	
	# Recycle the node unless it exited normally
	echo ""
	echo "GaianDB server node with args [$args] has exited. Exit code="$rc
	[[ $rc -eq 3 ]] && echo "Recycling Node..." && continue;
	[[ $rc -ne 2 ]] && break;
	echo "Detected OutOfMemoryError exit code.. deleting heapdump, javacore and Snap files; and recycling GaianDB node"
	rm heapdump.*.phd javacore.*.txt Snap.*.trc
done

#echo "Server exited - Press any key to continue..."
#read
