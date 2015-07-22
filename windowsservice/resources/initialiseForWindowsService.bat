@echo off
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2014
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================

rem Apache Commons Daemon looks in the registry to start a Java Service.
rem if no Oracle JVM exists, it is necessary to copy the IBM Registry settings
rem so that the Daemon uses the IBM JDK.

set OracleJavaRegFlag="HKLM\SOFTWARE\JavaSoft\Java Runtime Environment"
set IBMJavaRegFlag="HKLM\SOFTWARE\IBM\Java2 Runtime Environment"


rem Check to see if valid JVM registry flags are avialable for Apache Commons Daemon

reg query %OracleJavaRegFlag% /v CurrentVersion

if %ERRORLEVEL% EQU 0 goto OracleRegistryFlagFound
goto OracleRegistryFlagNotFound

:OracleRegistryFlagFound
echo "The correct Registry Flags for the Windows Service have been found"
goto End

:OracleRegistryFlagNotFound
reg query %IBMJavaRegFlag% /v CurrentVersion

if %ERRORLEVEL% NEQ 0 goto IBMRegistryFlagNotFound

rem Copy the IBM JVM flags into the place where Apache Commond Daemon looks.
echo Copying Registry Flags.
reg Copy %IBMJavaRegFlag% %OracleJavaRegFlag% /s

reg query %OracleJavaRegFlag% /v CurrentVersion
if %ERRORLEVEL% NEQ 0 echo "Copying the Registry Flags failed."

goto End

:IBMRegistryFlagNotFound
echo "A current Oracle or IBM JVM can not be found. Please Install one."
goto End

:End