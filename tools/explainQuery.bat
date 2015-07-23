@echo off
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2009
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================

rem >>> Notes:

rem >>> !!!!!!!!!!!
rem >>> This batch file must be run from the same directory as where the GaianDB server was started from because that
rem >>> is where the .dot file will be generated.
rem >>> !!!!!!!!!!!

rem >>> How this batch file works:
rem >>> It calls the stored procedure 'listexplain' which is a GaianDB API call that performs an 'explain' query on the given 
rem >>> logical table, here: 'DERBY_TABLES', it is equivalent to the query against GaianTable('DERBY_TABLES', 'explain in graph.dot').
rem >>> Note that a file 'graph.dot' is produced, which holds a simple text representation of a graph that shows
rem >>> the route of a query through the network of GaianDB nodes. This is transformed into a graph.gif file by the 'dot' program.
rem >>> The 'dot' program is available as part of 'Graphviz' which can be downloaded from: http://www.graphviz.org/Download..php

rem >>> The logical table 'DERBY_TABLES' is system generated and not visible in the config file. It provides a view of
rem >>> all physical tables and their columns in all the networked GaianDB nodes.


if not defined GDBH set GDBH=..

del %GDBH%\graph.dot graph.gif

rem set query="select * from new com.ibm.db2j.GaianTable('DERBY_TABLES', 'explain in graph.dot') GT"
rem set query="select * from new com.ibm.db2j.GaianQuery('select distinct tabname from new com.ibm.db2j.GaianTable(''DERBY_TABLES'', ''maxDepth=0'') T where tabtype=''T''', 'explain in graph.dot') Q"
rem set query="select * from new com.ibm.db2j.GaianQuery('select distinct tabname from new com.ibm.db2j.GaianTable(''DERBY_TABLES'', ''maxDepth=0'') T', 'explain in graph.dot') Q"
rem set query="select * from table( gexplain('select * from table( gtables() ) T') ) T"
rem set query="call list_explain('select * from table( gtables() ) T')"


rem set query="select * from new com.ibm.db2j.GaianTable('DERBY_TABLES', 'explain in graph.dot') GT"
set query="call listexplain('DERBY_TABLES')"

if NOT [%*] == [] set query=%*

call %GDBH%\queryDerby.bat %query%

dot -Tgif %GDBH%\graph.dot -o graph.gif


REM Set TDate=%date:~10,4%%date:~4,2%%date:~7,2%
echo %TIME%

SET FDT=%date:~6,4%%date:~3,2%%date:~0,2%%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%%TIME:~9,2%


SET FLNAME=%GDBH%\graph%FDT%.gif
echo filename=%FLNAME% 

move graph.gif %FLNAME%

dir %FLNAME%
 
start explorer %FLNAME%
del %GDBH%\graph.dot


rem start graph.gif
rem pause