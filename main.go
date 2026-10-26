package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/kbelludoo/kick-miner-go/pkg/chatbot"
	"github.com/kbelludoo/kick-miner-go/pkg/chatter"
	"github.com/kbelludoo/kick-miner-go/pkg/config"
	"github.com/kbelludoo/kick-miner-go/pkg/miner"
	"github.com/kbelludoo/kick-miner-go/pkg/server"
)

func main() {
	fmt.Println("================================================================================")
	fmt.Println("     ⚡ KICK CHANNEL POINTS MINER GO (ULTRA-LEVE & ALTA PERFORMANCE) ⚡        ")
	fmt.Println("                 Zero Navegadores • Zero Docker • ~2MB RAM                      ")
	fmt.Println("================================================================================")

	configFile := "config.json"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}

	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		log.Fatalf("[Kick Miner] Erro na configuração: %v\n", err)
	}

	if len(cfg.Accounts) == 0 {
		log.Fatalf("[Kick Miner] Nenhuma conta configurada em '%s'. Adicione ao menos uma conta com bearerToken.\n", configFile)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Intercepta sinais de encerramento do sistema
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\n[Kick Miner] Encerrando graciosamente...")
		cancel()
	}()

	// Instancia gerenciador e servidor web
	mgr := miner.NewManager(cfg)
	srv := server.NewServer(cfg.Settings.WebDashboardPort, mgr)
	srv.Start()

	go mgr.Start(ctx)

	// Inicia o Chatbot oficial para a live do canal (envia links periódicos e responde comandos)
	chatBot := chatbot.NewBot(cfg.Accounts[0].BearerToken, "kbelludoo")
	go chatBot.Start(ctx)

	// Inicia os Chatters humanizados se habilitados
	if cfg.Chatter.Enabled && len(cfg.Accounts) > 0 {
		startChatters(ctx, cfg)
	}

	// Ticker de console no terminal a cada 5 segundos
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			mgr.Close()
			return
		case <-ticker.C:
			t := mgr.GetTelemetry()

			var watchingList []string
			totalPoints := 0
			for _, acc := range t.Accounts {
				totalPoints += acc.TotalPoints
				for sName, s := range acc.Streamers {
					if s.Watching {
						watchingList = append(watchingList, fmt.Sprintf("%s (%d pts)", sName, s.Points))
					}
				}
			}

			watchDesc := "Nenhum canal ao vivo no momento"
			if len(watchingList) > 0 {
				watchDesc = strings.Join(watchingList, ", ")
			}

			fmt.Printf("[Ticker] RAM: %.2f MB | Uptime: %s | Transmissões: %d | Pontos: %d | Assistindo: [%s]\n",
				t.RamMb, t.Uptime, t.TotalStreams, totalPoints, watchDesc)
		}
	}
}

// startChatters inicia um Chatter para cada streamer configurado, usando a primeira conta
// do grupo "prioridade" (ou a primeira conta disponível).
func startChatters(ctx context.Context, cfg *config.Config) {
	if len(cfg.Accounts) == 0 {
		return
	}

	acc := cfg.Accounts[0]

	// Determina a lista de streamers do chatter. Prioriza cfg.Chatter.Streamers,
	// mas cai no grupo "prioridade" da conta se a lista estiver vazia.
	streamers := cfg.Chatter.Streamers
	if len(streamers) == 0 {
		for _, g := range acc.GetEffectiveGroups() {
			if g.Name == "prioridade" {
				streamers = g.Streamers
				break
			}
		}
	}

	if len(streamers) == 0 {
		log.Println("[Chatter] Nenhum streamer configurado para o chatter, pulando...")
		return
	}

	minInterval := time.Duration(cfg.Chatter.MinIntervalMinutes) * time.Minute
	maxInterval := time.Duration(cfg.Chatter.MaxIntervalMinutes) * time.Minute

	for _, streamer := range streamers {
		s := streamer // captura para goroutine
		c := chatter.NewChatter(chatter.ChatterConfig{
			Token:       acc.BearerToken,
			Streamer:    s,
			Username:    acc.Username,
			MinInterval: minInterval,
			MaxInterval: maxInterval,
			LLMAPIKey:   cfg.Chatter.LLMAPIKey,
			LLMBaseURL:  cfg.Chatter.LLMBaseURL,
			LLMModel:    cfg.Chatter.LLMModel,
		})
		log.Printf("[Chatter] Iniciando chatter para streamer '%s'\n", s)
		go c.Start(ctx)
	}
}

func init() {
	// Garante que o diretório de trabalho seja a raiz do executável
	if exe, err := os.Executable(); err == nil {
		_ = os.Chdir(filepath.Dir(exe))
	}
	_ = runtime.GOMAXPROCS(runtime.NumCPU())
}
