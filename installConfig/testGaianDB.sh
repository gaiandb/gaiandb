#!/bin/bash
# ============================================================================
#  GaianDB
#  Copyright IBM Corp. 2007-2014
#  
#  LICENSE: Eclipse Public License v1.0
#  http://www.eclipse.org/legal/epl-v10.html
# ============================================================================

# Note to users: You can specify [-h GaianNodeHost] [-p GaianNodePort] to send this sample query to any Gaian Node

# For Unix, when double-clicking on this script, we want the home+workspace dirs to be the install location (NOT the user's home folder!):
[[ `dirname $0`!="$PWD" ]] && [[ -z "$GDBH" ]] && export GDBH=`dirname $0` && [[ -z "$GAIAN_WORKSPACE" ]] && export GAIAN_WORKSPACE=$GDBH

[[ -z $GDBH ]] && export GDBH=.

# This is a simple query against logical table: 'LT0', with provenance columns, therefore accessing the 'LT0_P' view.
# The provenance columns 'GDB_NODE' and 'GDB_LEAF' appear in the returned result set.
# These columns respectively hold the nodeid (usually just the hostname, but also the port if it is not the default 6414)
# and a description of the physical federated data source that each row of data was retrieved from.

$GDBH/queryDerby.sh $* "select * from LT0_P"
