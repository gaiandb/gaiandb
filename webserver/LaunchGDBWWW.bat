@echo off
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2012
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================

if not defined GDBH set GDBH=.
SET GDBL=%GDBH%\lib

SET SYSCP=""
if defined CLASSPATH SET SYSCP=%CLASSPATH%

SET CP=%GDBL%;%GDBL%\GAIANDB.jar;%GDBL%\db2jcutdown.jar;%GDBL%\derbyclient.jar;%GDBL%\derby.jar;%GDBL%\derbynet.jar;%GDBL%\derbytrimmed.jar

rem Do not move this script from the GaianDB home directory (where GAIANDB.jar is).
rem If running this script from another directory, GDBH  must be set to the GaianDB home directory.
rem Note that the workspace will be the execution directory: where the config file, database, log file, 
rem sysinfo file and other user data files are located.

TITLE GaianDB WWW Server 
echo Launching GDB WWW Server...
SET CP="%CP:"=%;.\lib\GDBWWWServer.jar;%SYSCP:"=%"

java -Xmx64m -cp %CP%  com.ibm.gaiandb.webserver.WebServer
