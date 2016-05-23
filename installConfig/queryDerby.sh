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

[[ -z $GDBH ]] && GDBH=.
GDBL=$GDBH/lib

# Obsolete: export CLASSPATH="$GDBL/:$GDBL/GAIANDB.jar:$GDBL/db2jcutdown.jar:$GDBL/derby.jar:$GDBL/derbyclient.jar"
# Obsolete: export CLASSPATH="$CLASSPATH:$GDBL/cobertura.jar"

# Add all jars in $GDBL
function findLibjars { for j in $1/*.jar; do export CLASSPATH="$CLASSPATH:$j"; done; }
findLibjars $GDBL

export CLASSPATH="$CLASSPATH:$HOME/sqllib/java/db2jcc.jar:$HOME/sqllib/java/db2jcc_license_cu.jar"

# Use "$@" to pass in a double quoted argument as a single argument to java, e.g.
# ./queryDerby.sh -p 6414 "select * from new com.ibm.db2j.GaianTable('LT0') T"
# Note "$*" would split the "select * ..." statement into multiple arguments.

# Java location
JAVA_CMD=`[[ -z $JAVA_HOME ]] && echo java || echo $JAVA_HOME/bin/java`

# Default JVM arguments, e.g. heap size
[[ -z $JAVA_OPTS ]] && JAVA_OPTS=-Xmx128m

#"$JAVA_CMD" -version
"$JAVA_CMD" "$JAVA_OPTS" -cp "$CLASSPATH" com.ibm.gaiandb.tools.SQLDerbyRunner "$@"


# Alternate Derby CLI 'ij':
# java -cp "lib/derbytools.jar:lib/derbyclient.jar" org.apache.derby.tools.ij
# ij> connect 'jdbc:derby://localhost:6414/gaiandb;create=false;user=gaiandb;password=passw0rd';

