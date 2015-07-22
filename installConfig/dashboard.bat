@ECHO OFF
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2007-2014
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================

TITLE GaianDB Dashboard

if not defined GDBH set GDBH=.
set GDBL=%GDBH%\lib

SET THISCLASSPATH=""

REM -- Obsolete: SET CLASSPATH=%GDBL%\GAIANDB.jar;%GDBL%\GAIANDB-tools.jar;%GDBL%\prefusetrimmed.jar;%GDBL%\derby.jar;%GDBL%\derbyclient.jar

REM -- Add all jars in %GDBL%
call :findLibjars "%GDBL%"

START javaw -cp "%THISCLASSPATH:"=%" com.ibm.gaiandb.apps.dashboard.Dashboard

exit

REM ======================== automatic jar discovery ROUTINES ===============================
:findLibjars
for %%j in (%1\*.jar) do call :addlibjar "%%j"
exit /B

:addlibjar
set THISCLASSPATH=%THISCLASSPATH%;%1
