@echo off
title APL Document Pipeline — Processing Inbox
cd /d "C:\Development\Document-Pipeline"

echo.
echo ============================================================
echo  APL Document Pipeline
echo  Processing all Inbox sources
echo ============================================================
echo.

python pipeline\ingest.py

echo.
echo ============================================================
echo  Done. Check above for any warnings or failures.
echo ============================================================
echo.
pause
