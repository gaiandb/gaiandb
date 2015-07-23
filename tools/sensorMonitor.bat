@ECHO OFF
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2009
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================

rem ============================================================================
rem  This batch file monitors Lenovo ThinkPad sensors (using SensorReader.dll)
rem  and stores readings in a Derby table - which are read by the GaianDB dashboard
rem ============================================================================

SETLOCAL
TITLE GaianDB Sensor Monitor

if not defined GDBH set GDBH=..
set GDBL=%GDBH%\lib

SET CLASSPATH=%GDBL%\GAIANDB.jar;%GDBL%\derby.jar;%GDBL%\derbyclient.jar

java -cp %CLASSPATH% com.ibm.gaiandb.apps.sensormonitor.SensorMonitor %*

PAUSE
