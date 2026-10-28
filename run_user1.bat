@echo off
title KTP User1 (Sender)
set FILE=%1
if "%FILE%"=="" set FILE=input.txt
echo Starting User1 (Sender) with file %FILE%...
java -cp . User1 %FILE% 9997 8887
pause
