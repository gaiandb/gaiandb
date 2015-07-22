@echo off
rem ============================================================================
rem  GaianDB
rem  Copyright IBM Corp. 2007-2014
rem  
rem  LICENSE: Eclipse Public License v1.0
rem  http://www.eclipse.org/legal/epl-v10.html
rem ============================================================================

TITLE Gaiandb Test

REM Note to users: You can specify [-h GaianNodeHost] [-p GaianNodePort] to send this sample query to any Gaian Node

if not defined GDBH set GDBH=.

rem >>> This is a simple query against logical table: 'LT0', with provenance columns, therefore accessing the 'LT0_P' view.
rem >>> The provenance columns 'GDB_NODE' and 'GDB_LEAF' appear in the returned result set.
rem >>> These columns respectively hold the nodeid (usually just the hostname, but also the port if it is not the default 6414)
rem >>> and a description of the physical federated data source that each row of data was retrieved from.

call %GDBH%\queryDerby.bat %* "select * from LT0_P"

pause
