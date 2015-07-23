#!/bin/bash
# ============================================================================
#  GaianDB
#  Copyright IBM Corp. 2009
#  
#  LICENSE: Eclipse Public License v1.0
#  http://www.eclipse.org/legal/epl-v10.html
# ============================================================================

# This script file must be run from the same directory as where the GaianDB server was started from because that is where the .dot file will be generated
# 
# How this script works:
# ======================
# It calls the stored procedire 'listexplain' which is a GaianDB API call that performs an 'explain' query on the giben logical table, here 'DERBY_TABLE'. It is equivalent to the query against GaianTable('DERBY_TABLES','explain in graph.dot').
#
# The logical table 'DERBY_TABLES' is system generated and not visible in the config file. It provides a view of all physical tables and their columns in all the networked GaianDB nodes.
#
# Note that a file called 'graph.dot' is produced, which holds a simple test representation of a graph that shows the route of a query through the network of GaianDB nodes. This is transformed into a graph.gif file by the 'dot' program.
#
# The 'dot' program is available as part of 'Graphviz' which can be downloaded from:
# http://www.graphviz.org/Download.php
#

echo Launching Explain Query...

[[ -z $GDBH ]] && GDBH=$PWD
GDBL=$GDBH/lib

echo Removing previous .dot file
rm $GDBH/graph.dot graph.gif >& /dev/null
echo

query=$1

if [ -z $query ]; then
    query="call listexplain('DERBY_TABLES')"
fi

$GDBH/queryDerby.sh "$query"

if [ -z "graph.dot" ]; then
	echo graph.dot created

	hasDot=`which dot`

	if [ -z $hasDot ]; then
    	echo Graphviz is not installed, cannot create graph
    	echo
    	echo You can find Graphviz at http://www.graphviz.org/Download.php
	    exit 1
	fi

	echo Generating gif file from graph.dot
	dot -Tgif $GDBH/graph.dot -o graph.gif
	echo
fi

if [ -z "graph.gif" ]; then
	date=`date +%Y%m%d-%H%M%S`
	filename=$GDBH/graph$date.gif
	mv graph.gif $filename

	echo Graph file created as $filename

	hasGimp=`which gimp`

	if [ -z $hasGimp ]; then
    	echo Cannot find Gimp to open $filename into
    	echo 
    	echo $filename is ready to be viewed using an image viewer of your choice
	else
	    gimp $filename
	fi
fi


