@echo off
echo ===================================================
echo   Compilando Kick Miner Go para Linux (VPS)
echo ===================================================
cmd /c "set GOOS=linux&& set GOARCH=amd64&& go build -ldflags=\"-s -w\" -o kick-miner-linux ."
if %errorlevel% equ 0 (
    echo [SUCESSO] Binario kick-miner-linux gerado com sucesso!
) else (
    echo [ERRO] Falha ao compilar para Linux.
)
pause
