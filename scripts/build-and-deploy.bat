@echo off
echo ============================================================
echo   COMPILANDO KICK MINER GO PARA LINUX (AMD64 / VPS)
echo ============================================================

set GOOS=linux
set GOARCH=amd64
go build -ldflags="-s -w" -o kick-miner-linux .

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Sucesso! Binario gerado: kick-miner-linux
) else (
    echo.
    echo Erro durante a compilacao.
    pause
    exit /b 1
)

echo.
set /p VPS_IP="Digite o IP da VPS: "

set SSH_KEY=..\bot-template-go\ssh-key-2026-10-06.key
set SSH_USER=ubuntu
set REMOTE_DIR=/home/ubuntu/kick-miner

echo.
echo [1/4] Criando pasta remota...
ssh -i "%SSH_KEY%" -o StrictHostKeyChecking=no %SSH_USER%@%VPS_IP% "mkdir -p %REMOTE_DIR%"

echo [2/4] Enviando binario...
scp -i "%SSH_KEY%" -o StrictHostKeyChecking=no kick-miner-linux %SSH_USER%@%VPS_IP%:%REMOTE_DIR%/kick-miner-linux

echo [3/4] Enviando config.json...
scp -i "%SSH_KEY%" -o StrictHostKeyChecking=no config.json %SSH_USER%@%VPS_IP%:%REMOTE_DIR%/config.json

echo [4/4] Instalando e iniciando servico systemd...
scp -i "%SSH_KEY%" -o StrictHostKeyChecking=no scripts\kick-miner.service %SSH_USER%@%VPS_IP%:/tmp/kick-miner.service
ssh -i "%SSH_KEY%" -o StrictHostKeyChecking=no %SSH_USER%@%VPS_IP% "chmod +x %REMOTE_DIR%/kick-miner-linux && sudo mv /tmp/kick-miner.service /etc/systemd/system/kick-miner.service && sudo systemctl daemon-reload && sudo systemctl enable kick-miner && sudo systemctl restart kick-miner && sudo systemctl status kick-miner --no-pager"

echo.
echo ============================================================
echo   DEPLOY CONCLUIDO!
echo   Dashboard: http://%VPS_IP%:3009
echo   Logs: ssh -i %SSH_KEY% %SSH_USER%@%VPS_IP% "journalctl -u kick-miner -f"
echo ============================================================
pause
