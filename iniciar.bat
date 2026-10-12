@echo off
title Kick Miner Go - Monitor de Pontos
echo ===================================================
echo     Iniciando Kick Miner Go...
echo ===================================================
if not exist "kick-miner.exe" (
    echo Compilando kick-miner.exe...
    go build -ldflags="-s -w" -o kick-miner.exe .
)
kick-miner.exe
pause
