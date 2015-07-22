@echo off
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2007-2014
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================

rem Do not move this script from the GaianDB home directory (where GAIANDB.jar is).
rem If running this script from another directory, GDBH  must be set to the GaianDB home directory.

rem Pass in as 1st argument to this script the number of nodes to kick off, or set the following variable here:

SET startport=6414
SET numnodes=3

rem Pass in as 2nd arg the option '-sameconfig' to make all nodes use the same configuration file when starting up.


if NOT [%1] == [] set numnodes=%1

rem SET cidx=
SET cidx=1
if -sameconfig == %2 set cidx=


rem The following is equivalent to 
rem start launchGaianServer.bat -p 6414
rem start launchGaianServer.bat -p 6415 -c gaiandb_config2.properties
rem start launchGaianServer.bat -p 6416 -c gaiandb_config3.properties
rem ...


if not defined GDBH set GDBH=.


rem ENVIRONMENT VARIABLES CANNOT BE SET INSIDE A FOR LOOP !!!!!
rem So we call a function inside the loop to start the remaining servers

FOR /L %%G IN (1,1,%numnodes%) DO ( call :start_node %%G )
goto :eof

:start_node
	set /a port = %startport% + %1 - 1
	if DEFINED cidx ( set /a cidx = %1 )
	
	rem if %port%==6415 call sleep.exe 20
	rem if %port%==6424 call sleep.exe 30
	rem if %port%==6434 call sleep.exe 20
	rem if %port%==6444 call sleep.exe 20
	rem if %port%==6454 call sleep.exe 20
	rem if %port%==6464 call sleep.exe 20

	if %port% EQU %startport% ( 
		start %GDBH%\launchGaianServer.bat -p %startport%
	) else (
		start %GDBH%\launchGaianServer.bat -p %port% -c gaiandb_config%cidx%.properties
	)
goto :eof

