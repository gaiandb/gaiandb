@echo off
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2014
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================


rem Do not move this script from the GaianDB home directory (where GAIANDB.jar is).
rem If running this script from another directory, GDBH  must be set to the GaianDB home directory.
rem Note that the workspace will be the execution directory: where the config file, database, log file, 
rem sysinfo file and other user data files are located.


set DEFAULT_COMMAND=install
set DEFAULT_CONFIG=gaiandb.properties
set DEFAULT_PORT=6414

rem parse parameters


set COMMAND=%1
if "%COMMAND%" == "" set COMMAND=%DEFAULT_COMMAND%

rem get a port if specified in the parameters
set PORT=%DEFAULT_PORT%
set portdefined=
FOR %%G IN (%*) DO (
if defined portdefined ( 
	set PORT=%%G
	goto gotport
)
if -p == %%G set portdefined=yes
)
:gotport

rem echo PORT %PORT%


rem CURRENT_DIR will have a backslash delimiter at the end.
set CURRENT_DIR=%~dps0

rem Set default Service name
set SERVICE_NAME=GaianDB%PORT%
set DISPLAYNAME=IBM Gaian Database %PORT%

rem windows service jar location
set WINSERVJAR=%CURRENT_DIR%lib\WindowsService.jar


if /i %COMMAND% == install goto installService
if /i %COMMAND% == uninstall goto uninstallService
if /i %COMMAND% == manage goto manageService
if /i %COMMAND% == start goto startService
if /i %COMMAND% == stop goto stopService

echo Unrecognised Command
goto end

:installService
rem write the properties file with the gaian launch command.

rem install the service
commons-daemon\prunsrv //IS//%SERVICE_NAME% ^
	--Description="IBM Gaian Database" ^
	--DisplayName="%DISPLAYNAME%" ^
	--Install=%CURRENT_DIR%commons-daemon\prunsrv ^
	--Jvm auto ^
	--Startup auto ^
	--StartMode jvm ^
	--StartClass com.ibm.gaiandb.windowsservice.WindowsService ^
	--StartMethod start ^
	--StartParams gaianService%PORT%.properties ^
	--StartPath %CURRENT_DIR%^
	--StopMode jvm ^
	--StopClass com.ibm.gaiandb.windowsservice.WindowsService ^
	--StopMethod stop ^
	--Classpath %WINSERVJAR%;%CURRENT_DIR% ^
	--LogPath service.log ^
	--LogPrefix gaian ^
	--LogLevel Info ^
	--StdOutput auto ^
	--StdError auto

if not errorlevel 1 (echo %SERVICE_NAME% Service successfully installed.) else (echo %SERVICE_NAME% Service install failed. Error code: %errorlevel%
 goto error)

:manageService
commons-daemon\prunmgr //ES//%SERVICE_NAME%
if errorlevel 1	echo %SERVICE_NAME% Service not found %errorlevel%
goto end

:uninstallService
commons-daemon\prunsrv //DS//%SERVICE_NAME%
rem the uninstall reports its own failure so we only echo on success.
if not errorlevel 1	(echo %SERVICE_NAME% Service successfully uninstalled.) 
goto end

:startService
commons-daemon\prunsrv //ES//%SERVICE_NAME%
rem the start reports its own failure so we only echo on success.
if not errorlevel 1	(echo %SERVICE_NAME% Service successfully started, wait a few seconds before connecting to GaianDB.) 
goto end

:stopService
commons-daemon\prunsrv //SS//%SERVICE_NAME%

rem the start reports its own failure so we only echo on success.
if not errorlevel 1	(echo %SERVICE_NAME% Service successfully stopped.) 
goto end

:error
set /p input=Press Enter To Continue:%=%

:end