# ⚡ Kick Miner Go — Standalone Channel Points Miner

Minerador autônomo e de altíssima performance de **Pontos de Canal da Kick** desenvolvido 100% em **Go (Golang)**. Computa presença de espectador no WebSocket oficial e acumula pontos 24/7 sem abrir navegadores e sem baixar fluxos pesados de vídeo.

🌐 **Documentação & Landing Page (GitHub Pages):**  
👉 **[https://kbelludoo.github.io/kick-miner-go/](https://kbelludoo.github.io/kick-miner-go/)**

---

## 🌟 Recursos Principais

- 🪶 **Consumo Insano de RAM (~2 MB):** Mais de 95% mais leve que qualquer alternativa em Python/Docker (que usam de 45 a 100 MB).
- 📶 **Zero Consumo de Vídeo:** Não baixa os gigabytes de streaming de vídeo. Comunica-se exclusivamente via mensagens leves de WebSocket (`websockets.kick.com`), consumindo apenas alguns Kilobytes de texto por hora.
- 🔄 **Reconexão Inteligente e Multi-Stream:**
  - Detecta quando os streamers entram ao vivo e conecta automaticamente.
  - Limita as conexões simultâneas (`maxConcurrentStreams`) para evitar rate-limits.
  - Reconecta sozinho quando a live encerra ou reinicia.
- 💰 **Monitor de Saldo em Tempo Real:** Consulta a API de pontos da Kick a cada 5 minutos e registra a evolução de saldo no console e no Web Dashboard.
- 🌐 **Web Dashboard Embutido (Porta 3007):** Interface escura/neon para monitorar streamers assistidos, uptime e logs diretamente no navegador (`http://localhost:3007`).
- 🖥️ **Pronto para VPS Linux e Windows:** Binário compilado sem dependências externas.

---

## 📁 Estrutura do Projeto

```text
kick-miner-go/
├── config.example.json         # Modelo seguro de configuração
├── config.json                 # Suas credenciais reais (ignorado pelo git via .gitignore)
├── main.go                     # Ponto de entrada do executável
├── go.mod / go.sum             # Dependências Go
├── iniciar.bat                 # Inicializador 1-clique para Windows
├── Dockerfile                  # Container mínimo Alpine (~12 MB)
├── docker-compose.yml          # Compose opcional
│
├── docs/                       # 🌐 GITHUB PAGES
│   └── index.html              # Landing page e guia
│
├── pkg/
│   ├── config/                 # Parser de configurações
│   ├── miner/                  # Motor WebSocket, handshakes e tracking
│   └── server/                 # Web Dashboard embutido na porta 3007
│
└── scripts/
    └── build-linux.bat         # Compilação para VPS Linux
```

---

## 🚀 Como Executar

### 1. Clonar o Repositório
```bash
git clone https://github.com/kbelludoo/kick-miner-go.git
cd kick-miner-go
```

### 2. Configurar o `config.json`
Copie `config.example.json` para `config.json`:
```bash
cp config.example.json config.json
```

Edite o `config.json` adicionando seu Bearer Token da Kick e a lista de streamers:
```json
{
  "accounts": [
    {
      "username": "SeuUsuarioKick",
      "bearerToken": "SEU_USER_ID|SEU_TOKEN_AQUI",
      "streamers": ["tftoddy", "faasii", "axtlol", "xdaren_", "cogu", "matta"],
      "maxConcurrentStreams": 2
    }
  ],
  "settings": {
    "checkIntervalSeconds": 45,
    "pingIntervalSeconds": 30,
    "trackingIntervalSeconds": 10,
    "pointsLogIntervalMinutes": 5,
    "webDashboardPort": 3007
  }
}
```

#### 🔑 Como obter seu Bearer Token da Kick:
1. Abra o [Kick.com](https://kick.com) logado na sua conta no navegador.
2. Pressione `F12` para abrir as Ferramentas do Desenvolvedor e vá para a aba **Rede (Network)**.
3. Filtre por `api` ou `viewer` e recarregue a página.
4. Clique em qualquer requisição e veja os **Cabeçalhos da Requisição (Request Headers)**.
5. Copie o valor do cabeçalho `Authorization` (ex: `Bearer SEU_USER_ID|SEU_TOKEN_AQUI`). O token é o trecho após `Bearer `.

---

### 3. Rodar no Windows
Execute o script em lote ou via terminal:
```cmd
iniciar.bat
```
Ou compile e rode diretamente:
```bash
go run .
```

Acesse o Dashboard local:  
👉 **http://localhost:3007**

---

### 4. Rodar na VPS Linux (Ubuntu / Debian)

Compilar para Linux:
```cmd
cmd /c "set GOOS=linux&& set GOARCH=amd64&& go build -ldflags=\"-s -w\" -o kick-miner-linux ."
```

Copie para a sua VPS:
```bash
scp kick-miner-linux ubuntu@sua-vps-ip:/home/ubuntu/kick-miner-go/
scp config.json ubuntu@sua-vps-ip:/home/ubuntu/kick-miner-go/
```

Na VPS:
```bash
chmod +x kick-miner-linux
./kick-miner-linux
```

Ou execute como serviço `systemd` para iniciar com o sistema e rodar 24/7.

---

## 🔒 Segurança e Privacidade

- **Proteção Total:** O arquivo `config.json` com suas credenciais está listado no `.gitignore` e **nunca** é enviado para o GitHub.
- **Transparência:** Todo o código é aberto e não envia dados para terceiros; a conexão é estritamente direta entre o seu IP e os servidores oficiais da Kick (`websockets.kick.com`).

---

## 📄 Licença
Distribuído sob a licença MIT. Criado por [kbelludoo](https://github.com/kbelludoo).
