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

func init() {
	// Garante que o diretório de trabalho seja a raiz do executável
	if exe, err := os.Executable(); err == nil {
		_ = os.Chdir(filepath.Dir(exe))
	}
	_ = runtime.GOMAXPROCS(runtime.NumCPU())
}
