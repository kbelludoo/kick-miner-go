package miner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type StreamWatcher struct {
	manager   *Manager
	username  string
	token     string
	streamer  string
	channelID int64
	userID    int64
	streamID  int64

	mu        sync.RWMutex
	conn      *websocket.Conn
	alive     bool
	autoFill  bool
	startedAt time.Time
	stop      chan struct{}
	stopOnce  sync.Once
}

func NewStreamWatcher(mgr *Manager, username, token, streamer string, chID, uID, sID int64, autoFill bool) *StreamWatcher {
	return &StreamWatcher{
		manager:   mgr,
		username:  username,
		token:     token,
		streamer:  streamer,
		channelID: chID,
		userID:    uID,
		streamID:  sID,
		alive:     true,
		autoFill:  autoFill,
		startedAt: time.Now(),
		stop:      make(chan struct{}),
	}
}

func (w *StreamWatcher) IsAlive() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.alive
}

// IsAutoFill indica se o watcher ocupa um slot genérico (pode ser substituído por um favorito).
func (w *StreamWatcher) IsAutoFill() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.autoFill
}

func (w *StreamWatcher) setAutoFill(v bool) {
	w.mu.Lock()
	w.autoFill = v
	w.mu.Unlock()
}

func (w *StreamWatcher) Duration() time.Duration {
	return time.Since(w.startedAt)
}

func (w *StreamWatcher) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.alive = false
	w.stopOnce.Do(func() { close(w.stop) })
	if w.conn != nil {
		_ = w.conn.Close()
		w.conn = nil
	}
}

// Run conecta ao WebSocket e reconecta automaticamente com backoff exponencial.
func (w *StreamWatcher) Run(ctx context.Context) {
	defer func() {
		w.mu.Lock()
		w.alive = false
		if w.conn != nil {
			_ = w.conn.Close()
			w.conn = nil
		}
		w.mu.Unlock()

		w.manager.watcherEnded(w)
		w.manager.AddLog(fmt.Sprintf("[%s] Sessão de visualização na live de '%s' finalizada.", w.username, w.streamer))
	}()

	backoff := 5 * time.Second
	const maxBackoff = 120 * time.Second

	for {
		// Checa se contexto foi cancelado antes de tentar reconectar
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !w.IsAlive() {
			return
		}

		err := w.runSession(ctx)
		if err == nil {
			// Sessão terminou limpa (ctx cancelado)
			return
		}

		if !w.IsAlive() {
			return
		}

		// Sesão caiu por erro — reconectar com backoff
		w.manager.setWatching(w, false)
		w.manager.AddLog(fmt.Sprintf("[%s] ⚠ WebSocket de '%s' caiu: %v. Reconectando em %s...", w.username, w.streamer, err, backoff))

		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// runSession executa uma única sessão WebSocket. Retorna nil se ctx foi cancelado, erro caso contrário.
func (w *StreamWatcher) runSession(ctx context.Context) error {
	// 1. Obter Viewer WebSocket Token
	wsToken, err := w.fetchViewerToken()
	if err != nil || wsToken == "" {
		return fmt.Errorf("token WS: %w", err)
	}

	// 2. Conectar ao WebSocket
	wsURL := fmt.Sprintf("wss://websockets.kick.com/viewer/v1/connect?token=%s", wsToken)
	headers := make(http.Header)
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	headers.Set("Origin", "https://kick.com")

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, wsURL, headers)
	if err != nil {
		select {
		case <-ctx.Done():
			return nil
		default:
			return fmt.Errorf("dial WS: %w", err)
		}
	}
	defer conn.Close()

	w.mu.Lock()
	w.conn = conn
	w.mu.Unlock()

	w.manager.setWatching(w, true)

	// Consulta e loga saldo inicial de pontos
	if pts, err := w.manager.FetchPoints(w.token, w.streamer); err == nil {
		w.manager.updateStreamerPoints(w.username, w.streamer, pts)
		w.manager.AddLog(fmt.Sprintf("[%s] ⭐ Conectado à live de '%s'! Saldo atual: %d pontos (+10 a cada 5m).", w.username, w.streamer, pts))
	} else {
		w.manager.AddLog(fmt.Sprintf("[%s] ⭐ Conectado à live de '%s' (ganhando pontos a cada 5m)!", w.username, w.streamer))
	}

	// 3. Handshake inicial + ping
	_ = conn.WriteJSON(map[string]any{
		"type": "channel_handshake",
		"data": map[string]any{
			"message": map[string]any{
				"channelId": w.channelID,
			},
		},
	})
	_ = conn.WriteJSON(map[string]any{"type": "ping"})

	handshakeInterval := time.Duration(w.manager.cfg.Settings.PingIntervalSeconds) * time.Second
	trackingInterval := time.Duration(w.manager.cfg.Settings.TrackingIntervalSeconds) * time.Second
	pointsInterval := time.Duration(w.manager.cfg.Settings.PointsLogIntervalMinutes) * time.Minute

	handshakeTicker := time.NewTicker(handshakeInterval)
	defer handshakeTicker.Stop()

	trackTicker := time.NewTicker(trackingInterval)
	defer trackTicker.Stop()

	pointsTicker := time.NewTicker(pointsInterval)
	defer pointsTicker.Stop()

	disconnectChan := make(chan error, 1)
	sessionDone := make(chan struct{})
	defer close(sessionDone)

	// Goroutine de envio: handshakes periódicos e tracking
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sessionDone:
				return
			case <-handshakeTicker.C:
				w.mu.Lock()
				if w.conn != nil {
					_ = w.conn.WriteJSON(map[string]any{
						"type": "channel_handshake",
						"data": map[string]any{
							"message": map[string]any{
								"channelId": w.channelID,
							},
						},
					})
					_ = w.conn.WriteJSON(map[string]any{"type": "ping"})
				}
				w.mu.Unlock()

			case <-trackTicker.C:
				w.mu.Lock()
				if w.conn != nil {
					err := w.conn.WriteJSON(map[string]any{
						"type": "user_event",
						"data": map[string]any{
							"message": map[string]any{
								"name":          "tracking.user.watch.livestream",
								"channel_id":    w.channelID,
								"livestream_id": w.streamID,
							},
						},
					})
					if err != nil {
						w.mu.Unlock()
						select {
						case disconnectChan <- err:
						default:
						}
						return
					}
				}
				w.mu.Unlock()

			case <-pointsTicker.C:
				if pts, err := w.manager.FetchPoints(w.token, w.streamer); err == nil {
					w.manager.updateStreamerPoints(w.username, w.streamer, pts)
					w.manager.AddLog(fmt.Sprintf("[%s] 💰 Live de '%s': saldo atualizado para %d pontos (+10 creditados)!", w.username, w.streamer, pts))
				}
			}
		}
	}()

	// Loop de recebimento de mensagens — encerra rápido ao ctx.Done()
	readErr := make(chan error, 1)
	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			var parsed map[string]any
			if err := json.Unmarshal(msg, &parsed); err == nil {
				if parsed["type"] == "ping" {
					w.mu.Lock()
					if w.conn != nil {
						_ = w.conn.WriteJSON(map[string]any{"type": "pong"})
					}
					w.mu.Unlock()
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case <-w.stop:
		return nil
	case err := <-disconnectChan:
		return err
	case err := <-readErr:
		return err
	}
}

func (w *StreamWatcher) fetchViewerToken() (string, error) {
	// Reusa o httpClient do Manager para aproveitar o pool de conexões TCP
	req, err := http.NewRequest("GET", "https://websockets.kick.com/viewer/v1/token", nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Authorization", "Bearer "+w.token)
	req.Header.Set("Origin", "https://kick.com")
	req.Header.Set("Referer", fmt.Sprintf("https://kick.com/%s/", w.streamer))
	req.Header.Set("X-Chatroom", fmt.Sprintf("%d", w.channelID))
	req.Header.Set("X-User-Id", fmt.Sprintf("%d", w.userID))
	req.Header.Set("X-Client-Token", "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823")

	resp, err := w.manager.httpCli.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var parsed struct {
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
		Token string `json:"token"`
	}
	_ = json.Unmarshal(body, &parsed)

	if parsed.Data.Token != "" {
		return parsed.Data.Token, nil
	}
	if parsed.Token != "" {
		return parsed.Token, nil
	}
	return "", fmt.Errorf("token não encontrado na resposta")
}
