@echo off
set TESTDIR=%CD%
cd ..\..\tests
call setenv.bat
cd %TESTDIR%
set TESTDIR=
