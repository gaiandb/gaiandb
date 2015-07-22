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
rem Note that the workspace will be the execution directory: where the config file, database, log file, 
rem sysinfo file and other user data files are located.

rem set node=MyNodeID

set args=
set nodedef=
if NOT [%1] == [] ( set args=%* ) else (
	if defined node (
		set args=-n %node%
		set nodedef= %node%
	)
)

set portdef=
FOR %%G IN (%args%) DO (
if defined portdef ( 
	set portdef= on port %%G
	goto gotport
)
if -p == %%G set portdef=-p
)
:gotport

TITLE GaianDB Server%nodedef%%portdef%
echo Launching Server...

if not defined GDBH set GDBH=.
SET GDBL=%GDBH%\lib

SET GAIANCLASSPATH=%GDBL%

SET SYSCP=""
if defined CLASSPATH SET SYSCP=%CLASSPATH%

REM ======================== automatic jar discovery - (add user jars to 'ext' folder) ========
call :findLibjars "%GDBL%"
call :findLibjars "%GDBL%\ext"

goto bypassstaticjars

REM ======================== static jar settings (obsolete)================================

SET GAIANCLASSPATH=%GDBL%;%GDBL%\GAIANDB.jar;%GDBL%\db2jcutdown.jar;%GDBL%\derbyclient.jar;%GDBL%\derby.jar;%GDBL%\derbynet.jar;%GDBL%\derbytrimmed.jar
rem Add GaianDB WPML plugin
SET GAIANCLASSPATH=%GAIANCLASSPATH%;%GDBL%\wpml-pfg.jar

rem Optional Jars for interfacing to Omnifind Enterprise Edition and the Micro-Broker (to be obtained externally - see readme)
SET GAIANCLASSPATH=%GAIANCLASSPATH%;%GDBL%\siapi.jar;%GDBL%\esapi.jar;%GDBL%\wmqtt.jar

rem Optional Jars for WPML Policy Management libraries (to be obtained externally - see readme)
SET WPML=C:\APPS\wpml-1.2\lib
SET GAIANCLASSPATH=%GAIANCLASSPATH%;%WPML%\wpml.jar;%WPML%\JSON4J.jar;%WPML%\arenatk.jar;%WPML%\antlr-2.7.7.jar

rem Optional Jars for JDBC access to DB2, MySQL, MS SQLServer, Oracle data sources.
SET GAIANCLASSPATH=%GAIANCLASSPATH%;%GDBL%\db2jcc.jar;%GDBL%\db2jcc_license_cu.jar
SET GAIANCLASSPATH=%GAIANCLASSPATH%;%GDBL%\ojdbc14.jar

rem SET GAIANCLASSPATH=%GAIANCLASSPATH%;C:\APPS\DB2-9.5\java\db2jcc.jar;C:\APPS\DB2-9.5\java\db2jcc_license_cu.jar
rem SET GAIANCLASSPATH=%GAIANCLASSPATH%;C:\APPS\MSSQLServer\JDBCDriver\driver2005\Micros~1\sqljdbc_1.0\enu\sqljdbc.jar
rem SET GAIANCLASSPATH=%GAIANCLASSPATH%;C:\APPS\MySQL5\jdbc\mysql-connector-java-5.1.7-bin.jar
rem SET GAIANCLASSPATH=%GAIANCLASSPATH%;C:\APPS\oraclexe\app\oracle\product\10.2.0\server\jdbc\lib\ojdbc14.jar

rem Apache - POI jars for spreadsheet federation
rem SET GAIANCLASSPATH=%GAIANCLASSPATH%;%GDBL%\geronimo-stax-api_1.0_spec-1.0.jar;%GDBL%\poi-ooxml-schemas-3.6-20091214.jar;%GDBL%\dom4j-1.6.1.jar;%GDBL%\poi-3.6-20091214.jar;%GDBL%\poi-ooxml-3.6-20091214.jar;%GDBL%\xmlbeans-2.3.0.jar

REM ======================== static jars setting ===============================
:bypassstaticjars

rem - Set final GAIANCLASSPATH. Remove nested double quotes and wrap whole expression in them. Note " is not a valid file name character in windows
SET GAIANCLASSPATH="%GAIANCLASSPATH:"=%;%SYSCP:"=%"

rem echo GAIANCLASSPATH=%GAIANCLASSPATH%
rem echo.

rem - Java command - may include path

set JAVA_CMD=java
if defined JAVA_HOME set JAVA_CMD="%JAVA_HOME:"=%\bin\java"
if defined JAVA_HOME echo Using JAVA_HOME, so JAVA_CMD=%JAVA_CMD%

rem - JAVA_OPTS: Default JVM arguments, e.g. heap size

if not defined JAVA_OPTS set JAVA_OPTS=-Xmx256m

rem - GAIAN_WORKSPACE: Gaian workspace folder 
rem - This folder MUST CONTAIN AT LEAST gaiandb_config.properties. Also include derby.properties to enforce authentication (optional).
rem - Log files and physical Derby database will also be created here.

if not defined GAIAN_WORKSPACE set GAIAN_WORKSPACE=.

rem - REMOTE DEBUGGING:                               set JAVA_OPTS=-Xmx128m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044
rem - GENERATE HEAPDUMP WHEN OutOfMemoryError OCCURS: set JAVA_OPTS=-Xmx128m -Xdump:heap:events=systhrow,filter=java/lang/OutOfMemoryError


:restart

rem *** START GAIAN ***

%JAVA_CMD% %JAVA_OPTS% -Dderby.system.home=%GAIAN_WORKSPACE% -cp %GAIANCLASSPATH% com.ibm.gaiandb.GaianNode %args%


set rc=%ERRORLEVEL%
echo GaianDB server node with args [%args%] has exited. ERRORLEVEL=%rc%

rem Recycle the node if ERRORLEVEL is 2 or more - i.e. if it exited in any other way than through an explict process kill (e.g. memory overflow).
if %rc% == 2 (
	echo Detected OutOfMemoryError exit code.. deleting heapdump, javacore and Snap files; and recycling GaianDB node
	del heapdump.*.phd javacore.*.txt Snap.*.trc
	goto restart
)
if %rc% == 3 (
	echo Recycling node as requested...
	goto restart
)

if %rc% == 0 exit /B
Pause
exit /B


REM ======================== automatic jar discovery ROUTINES ===============================
:findLibjars
for %%j in (%1\*.jar) do call :addlibjar "%%j"
exit /B

:addlibjar
set GAIANCLASSPATH=%GAIANCLASSPATH%;%1

