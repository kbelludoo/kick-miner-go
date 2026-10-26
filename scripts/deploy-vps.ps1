# Deploy do Kick Miner Go para VPS
# Uso: .\scripts\deploy-vps.ps1 -VpsHost "ubuntu@SEU_IP" -SshKey ".\ssh-key.key"
param(
    [Parameter(Mandatory=$true)]
    [string]$VpsHost,
    
    [Parameter(Mandatory=$false)]
    [string]$SshKey = ".\ssh-key-2026-10-06.key",
    
    [Parameter(Mandatory=$false)]
    [string]$RemotePath = "/home/ubuntu/kick-miner-go"
)

Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  Kick Miner Go — Deploy para VPS" -ForegroundColor Cyan  
Write-Host "=================================================" -ForegroundColor Cyan

# 1. Compilar
Write-Host "[1/4] Compilando para Linux AMD64..." -ForegroundColor Yellow
$env:GOOS = "linux"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"
go build -ldflags="-s -w" -o kick-miner-linux .
if ($LASTEXITCODE -ne 0) { Write-Host "ERRO na compilacao!" -ForegroundColor Red; exit 1 }
Write-Host "    OK — kick-miner-linux gerado" -ForegroundColor Green

# Limpa vars de env
Remove-Item Env:GOOS; Remove-Item Env:GOARCH; Remove-Item Env:CGO_ENABLED

# 2. Enviar binário
Write-Host "[2/4] Enviando binario para $VpsHost..." -ForegroundColor Yellow
$sshArgs = if ($SshKey) { "-i `"$SshKey`"" } else { "" }
Invoke-Expression "scp $sshArgs kick-miner-linux `"${VpsHost}:${RemotePath}/kick-miner-linux`""
if ($LASTEXITCODE -ne 0) { Write-Host "ERRO no SCP do binario!" -ForegroundColor Red; exit 1 }
Write-Host "    OK — binario enviado" -ForegroundColor Green

# 3. Enviar config (opcional — só se config.json existir)
if (Test-Path "config.json") {
    Write-Host "[3/4] Enviando config.json para VPS..." -ForegroundColor Yellow
    Invoke-Expression "scp $sshArgs config.json `"${VpsHost}:${RemotePath}/config.json`""
    Write-Host "    OK — config.json enviado" -ForegroundColor Green
} else {
    Write-Host "[3/4] config.json nao encontrado — pulando" -ForegroundColor Gray
}

# 4. Reiniciar serviço na VPS
Write-Host "[4/4] Reiniciando kick-miner.service na VPS..." -ForegroundColor Yellow
$restartCmd = "chmod +x ${RemotePath}/kick-miner-linux && sudo systemctl restart kick-miner.service && sleep 2 && sudo systemctl status kick-miner.service --no-pager -l | head -20"
Invoke-Expression "ssh $sshArgs $VpsHost `"$restartCmd`""
if ($LASTEXITCODE -ne 0) { Write-Host "AVISO: Erro ao reiniciar servico (pode nao existir ainda)" -ForegroundColor Yellow }

Write-Host ""
Write-Host "=================================================" -ForegroundColor Green
Write-Host "  Deploy Concluido!" -ForegroundColor Green
Write-Host "  Dashboard: http://SEU_IP:3009" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
