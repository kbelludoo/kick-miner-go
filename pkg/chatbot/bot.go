package chatbot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Bot gerencia envio de links e respostas de comandos no chat da Kick
type Bot struct {
	token      string
	streamer   string
	chatroomID int64
	client     *http.Client
	mu         sync.Mutex
	stop       chan struct{}
}

// Respostas para comandos de chat
var botResponses = map[string]string{
	"!bots":      "🤖 [BOT FARM 24/7] Central Hub ao vivo: https://kbelludoo.github.io/bot-template-go/ | Miner: !miner | Jogos: !huntera, !sigma, !leveling, !baiak, !tibidle | Repo: !github",
	"!hub":       "⚡ Central VPS: https://kbelludoo.github.io/bot-template-go/",
	"!miner":     "🟢 Kick Miner Go: https://kbelludoo.github.io/kick-miner-go/ (Mineração de pontos e drops da Kick 24/7 em Go nativo)",
	"!kickminer": "🟢 Kick Miner Go: https://kbelludoo.github.io/kick-miner-go/ (Mineração de pontos e drops da Kick 24/7 em Go nativo)",
	"!pontos":    "⭐ Pontos Kick Miner: Mineração automática em até 12 lives simultâneas! Dashboard: https://kbelludoo.github.io/kick-miner-go/",
	"!huntera":   "⚔️ Huntera Idle Dashboard: https://kbelludoo.github.io/huntera-bot/",
	"!tibidle":   "🛡️ Tibidle Multi-Bot Dashboard: https://kbelludoo.github.io/tibidle-bot/",
	"!sigma":     "⚡ Pokemon Sigma Dashboard: https://kbelludoo.github.io/sigma-bot/",
	"!leveling":  "🏹 Leveling Idle 2D Dashboard: https://kbelludoo.github.io/leveling-bot/",
	"!baiak":     "🏰 Baiak Idle Fleet Dashboard: https://kbelludoo.github.io/baiak-idle-bot/",
	"!github":    "📦 Repositório GitHub do Projeto: https://github.com/kbelludoo/bot-template-go",
	"!vps":       "🖥️ VPS Oracle Cloud (sa-saopaulo-1) • Zero OBS • Restream nativo em Go + Xvfb",
}

func NewBot(token, streamer string) *Bot {
	return &Bot{
		token:    token,
		streamer: strings.ToLower(strings.TrimSpace(streamer)),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		stop: make(chan struct{}),
	}
}

// Start inicia o bot em background (timer periódico e listener de comandos)
func (b *Bot) Start(ctx context.Context) {
	if b.token == "" || b.streamer == "" {
		return
	}

	// 1. Resolve o ID da sala de chat
	for {
		chID, err := b.resolveChatroomID()
		if err == nil && chID > 0 {
			b.chatroomID = chID
			log.Printf("[Kick Chatbot] Conectado ao chat de '%s' (Chatroom ID: %d)\n", b.streamer, b.chatroomID)
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}

	// 2. Timer periódico para enviar mensagem com os links a cada 12 minutos
	go b.startPeriodicAnnouncements(ctx)

	// 3. Listener de comandos no chat via WebSocket Pusher
	go b.startChatListener(ctx)
}

func (b *Bot) resolveChatroomID() (int64, error) {
	url := fmt.Sprintf("https://kick.com/api/v2/channels/%s", b.streamer)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+b.token)

	resp, err := b.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return 0, err
	}

	if chatroom, ok := data["chatroom"].(map[string]any); ok {
		if idFloat, ok := chatroom["id"].(float64); ok {
			return int64(idFloat), nil
		}
	}
	return 0, fmt.Errorf("chatroom id não encontrado")
}

// SendMessage envia uma mensagem no chat da live
func (b *Bot) SendMessage(content string) error {
	b.mu.Lock()
	chID := b.chatroomID
	b.mu.Unlock()

	if chID == 0 {
		return fmt.Errorf("chatroom ainda não inicializado")
	}

	url := fmt.Sprintf("https://kick.com/api/v2/messages/send/%d", chID)
	payload, _ := json.Marshal(map[string]any{
		"content": content,
		"type":    "message",
	})

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+b.token)
	req.Header.Set("Origin", "https://kick.com")
	req.Header.Set("Referer", fmt.Sprintf("https://kick.com/%s", b.streamer))

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (b *Bot) startPeriodicAnnouncements(ctx context.Context) {
	// Primeira mensagem 1 minuto após iniciar
	select {
	case <-ctx.Done():
		return
	case <-time.After(1 * time.Minute):
		_ = b.SendMessage("🤖 [BOT FARM 24/7] Dashboards ao vivo de todos os bots: https://kbelludoo.github.io/bot-template-go/ | Use !bots para links individuais!")
	}

	ticker := time.NewTicker(12 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = b.SendMessage("🤖 [BOT FARM 24/7] Acompanhe telemetria, ouro e XP em tempo real: https://kbelludoo.github.io/bot-template-go/ | Miner: https://kbelludoo.github.io/kick-miner-go/ | Comandos: !bots, !miner, !huntera, !sigma, !baiak")
		}
	}
}

func (b *Bot) startChatListener(ctx context.Context) {
	wsURL := "wss://ws-us2.pusher.com/app/eb1d5f283081a78b932c?protocol=7&client=js&version=7.6.0&flash=false"
	dialer := websocket.DefaultDialer

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, _, err := dialer.DialContext(ctx, wsURL, nil)
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}

		// Inscreve no canal do chat da sala
		subMsg := map[string]any{
			"event": "pusher:subscribe",
			"data": map[string]any{
				"auth":    "",
				"channel": fmt.Sprintf("chatrooms.%d.v2", b.chatroomID),
			},
		}
		_ = conn.WriteJSON(subMsg)

		for {
			_, msgBytes, err := conn.ReadMessage()
			if err != nil {
				_ = conn.Close()
				break
			}

			var event struct {
				Event   string `json:"event"`
				Channel string `json:"channel"`
				Data    string `json:"data"`
			}
			if err := json.Unmarshal(msgBytes, &event); err != nil {
				continue
			}

			if event.Event == "App\\Events\\ChatMessageEvent" {
				var chatData struct {
					Content string `json:"content"`
					Sender  struct {
						Username string `json:"username"`
					} `json:"sender"`
				}
				if err := json.Unmarshal([]byte(event.Data), &chatData); err == nil {
					cmd := strings.ToLower(strings.TrimSpace(chatData.Content))
					if reply, exists := botResponses[cmd]; exists {
						log.Printf("[Kick Chatbot] Resposta acionada por '%s' para comando '%s'\n", chatData.Sender.Username, cmd)
						_ = b.SendMessage(fmt.Sprintf("@%s %s", chatData.Sender.Username, reply))
					}
				}
			}
		}

		time.Sleep(5 * time.Second)
	}
}
