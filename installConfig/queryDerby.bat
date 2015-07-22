@echo off
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2007-2014
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================

TITLE Derby CLP

if not defined GDBH set GDBH=.
set GDBL=%GDBH%\lib

rem - We keep a local variable THISCLASSPATH so as not to modify the environment classpath.
SET THISCLASSPATH=""

REM -- Add all jars in %GDBL%
call :findLibjars "%GDBL%"

SET THISCLASSPATH=%THISCLASSPATH%;C:\Progra~1\IBM\SQLLIB\java\db2jcc.jar;C:\Progra~1\IBM\SQLLIB\java\db2jcc_license_cu.jar
SET THISCLASSPATH=%THISCLASSPATH%;C:\APPS\db282\java\db2jcc.jar;C:\APPS\db282\java\db2jcc_license_cu.jar


SET ARGS=%*

rem Java location
set JAVA_CMD=java
if defined JAVA_HOME set JAVA_CMD="%JAVA_HOME:"=%\bin\java"

rem Default JVM arguments, e.g. heap size
if not defined JAVA_OPTS set JAVA_OPTS=-Xmx128m

rem %JAVA_CMD% -version
%JAVA_CMD% %JAVA_OPTS% -cp "%THISCLASSPATH:"=%" com.ibm.gaiandb.tools.SQLDerbyRunner %ARGS%

exit /B

REM ======================== automatic jar discovery ROUTINES ===============================
:findLibjars
for %%j in (%1\*.jar) do call :addlibjar "%%j"
exit /B

:addlibjar
set THISCLASSPATH=%THISCLASSPATH%;"%1"