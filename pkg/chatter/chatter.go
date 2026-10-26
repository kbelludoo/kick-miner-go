package chatter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Tamanho máximo do buffer circular de mensagens.
	maxBufferSize = 20
	// Janela de análise de mood (últimos N segundos).
	moodWindowSecs = 30
	// Limiar mínimo de mensagens para detectar um mood.
	moodThreshold = 3
	// URL base do Pusher WebSocket da Kick.
	pusherWSURL = "wss://ws-us2.pusher.com/app/eb1d5f283081a78b932c?protocol=7&client=js&version=7.6.0&flash=false"
	// User-Agent compatível com navegador para não ser bloqueado.
	browserUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

// laughTokens são tokens que indicam risada no chat.
var laughTokens = []string{"kkk", "haha", "lol", "hauha", "kkkk", "rsrs", "kek"}

// hypeTokens são tokens que indicam hype no chat.
var hypeTokens = []string{"vamo", "vai", "go", "bora", "chegou", "fogo", "🔥", "gooo", "lets"}

// ggTokens são tokens que indicam fim de partida.
var ggTokens = []string{"gg", "gg wp", "gg ez", "foi fácil", "proximo", "próximo"}

// griefTokens são tokens que indicam perda pesada.
var griefTokens = []string{"perdeu", "caiu", "morreu", "eliminado", "acabou", "derrotado"}

// Chatter é o agente que interage humanamente no chat de uma live Kick.
type Chatter struct {
	cfg        ChatterConfig
	chatroomID int64
	llm        *llmClient
	httpClient *http.Client

	mu      sync.Mutex
	buffer  []ChatMessage // buffer circular, máx maxBufferSize entradas
	lastSent time.Time
}

// NewChatter cria um novo Chatter com as configurações fornecidas.
// Aplica defaults seguros para intervalos não configurados.
func NewChatter(cfg ChatterConfig) *Chatter {
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = 13 * time.Minute
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = 19 * time.Minute
	}
	return &Chatter{
		cfg:        cfg,
		llm:        newLLMClient(cfg),
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Start inicia o Chatter: resolve o chatroomID, abre o WebSocket e dispara goroutines.
// Bloqueia até ctx ser cancelado.
func (c *Chatter) Start(ctx context.Context) {
	if c.cfg.Token == "" || c.cfg.Streamer == "" {
		slog.Warn("[chatter] token ou streamer vazio, chatter desativado",
			"streamer", c.cfg.Streamer)
		return
	}

	slog.Info("[chatter] iniciando", "streamer", c.cfg.Streamer, "username", c.cfg.Username)

	// Resolve o ID da sala de chat com retry infinito.
	for {
		id, err := c.resolveChatroomID()
		if err == nil && id > 0 {
			c.mu.Lock()
			c.chatroomID = id
			c.mu.Unlock()
			slog.Info("[chatter] chatroom resolvido", "streamer", c.cfg.Streamer, "chatroomID", id)
			break
		}
		slog.Warn("[chatter] falha ao resolver chatroomID, retentando em 10s",
			"streamer", c.cfg.Streamer, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}

	// Goroutine de envio periódico com jitter.
	go c.periodicSender(ctx)

	// Goroutine de leitura do chat via WebSocket Pusher.
	c.listenChat(ctx)
}

// periodicSender envia uma mensagem a cada intervalo aleatório entre MinInterval e MaxInterval.
func (c *Chatter) periodicSender(ctx context.Context) {
	// Primeira espera antes de enviar qualquer coisa.
	wait := c.randomInterval()
	slog.Info("[chatter] primeira mensagem agendada",
		"streamer", c.cfg.Streamer, "em", wait.Round(time.Second))

	select {
	case <-ctx.Done():
		return
	case <-time.After(wait):
	}

	for {
		c.mu.Lock()
		mood := c.detectMood()
		msgs := make([]ChatMessage, len(c.buffer))
		copy(msgs, c.buffer)
		c.mu.Unlock()

		phrase := PickPhrase(c.cfg.Streamer, mood)
		if phrase != "" {
			if err := c.sendWithHumanDelay(ctx, phrase); err != nil {
				slog.Warn("[chatter] erro ao enviar mensagem periódica",
					"streamer", c.cfg.Streamer, "err", err)
			} else {
				slog.Info("[chatter] mensagem periódica enviada",
					"streamer", c.cfg.Streamer, "mood", mood.String(), "msg", phrase)
				c.mu.Lock()
				c.lastSent = time.Now()
				c.mu.Unlock()
			}
		}

		next := c.randomInterval()
		slog.Info("[chatter] próxima mensagem agendada",
			"streamer", c.cfg.Streamer, "em", next.Round(time.Second))
		select {
		case <-ctx.Done():
			return
		case <-time.After(next):
		}
	}
}

// listenChat abre o WebSocket Pusher, lê mensagens e aciona reações a menções.
func (c *Chatter) listenChat(ctx context.Context) {
	dialer := websocket.DefaultDialer

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, _, err := dialer.DialContext(ctx, pusherWSURL, nil)
		if err != nil {
			slog.Warn("[chatter] falha no WebSocket, reconectando em 10s",
				"streamer", c.cfg.Streamer, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}

		c.mu.Lock()
		roomID := c.chatroomID
		c.mu.Unlock()

		// Subscrição no canal da sala de chat.
		subMsg := map[string]any{
			"event": "pusher:subscribe",
			"data": map[string]any{
				"auth":    "",
				"channel": fmt.Sprintf("chatrooms.%d.v2", roomID),
			},
		}
		if err := conn.WriteJSON(subMsg); err != nil {
			_ = conn.Close()
			continue
		}

		c.readLoop(ctx, conn)
		_ = conn.Close()

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// readLoop processa mensagens do WebSocket até o contexto ser cancelado ou a conexão fechar.
func (c *Chatter) readLoop(ctx context.Context, conn *websocket.Conn) {
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			c.handleRawMessage(ctx, raw)
		}
	}()

	select {
	case <-ctx.Done():
		_ = conn.Close()
		<-done
	case <-done:
	}
}

// handleRawMessage decodifica uma mensagem bruta do Pusher e processa evento de chat.
func (c *Chatter) handleRawMessage(ctx context.Context, raw []byte) {
	var envelope struct {
		Event string `json:"event"`
		Data  string `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return
	}
	if envelope.Event != `App\Events\ChatMessageEvent` {
		return
	}

	var chatData struct {
		Content string `json:"content"`
		Sender  struct {
			Username string `json:"username"`
		} `json:"sender"`
	}
	if err := json.Unmarshal([]byte(envelope.Data), &chatData); err != nil {
		return
	}

	msg := ChatMessage{
		Username:  chatData.Sender.Username,
		Content:   chatData.Content,
		Timestamp: time.Now(),
	}

	// Ignora mensagens enviadas pelo próprio chatter.
	if strings.EqualFold(msg.Username, c.cfg.Username) {
		return
	}

	// Adiciona ao buffer circular.
	c.mu.Lock()
	c.buffer = append(c.buffer, msg)
	if len(c.buffer) > maxBufferSize {
		c.buffer = c.buffer[len(c.buffer)-maxBufferSize:]
	}
	snapshot := make([]ChatMessage, len(c.buffer))
	copy(snapshot, c.buffer)
	c.mu.Unlock()

	// Verifica se foi uma menção ao username.
	if IsMention(msg.Content, c.cfg.Username) {
		go c.handleMention(ctx, snapshot, msg)
	}
}

// handleMention processa uma menção: aguarda delay humano, chama LLM e responde.
func (c *Chatter) handleMention(ctx context.Context, snapshot []ChatMessage, mention ChatMessage) {
	// Delay humano: 4-8 segundos antes de responder.
	delay := 4*time.Second + time.Duration(rand.Intn(4000))*time.Millisecond //nolint:gosec
	slog.Info("[chatter] menção detectada, respondendo em",
		"streamer", c.cfg.Streamer, "de", mention.Username, "delay", delay.Round(time.Second))

	select {
	case <-ctx.Done():
		return
	case <-time.After(delay):
	}

	prompt := BuildMentionPrompt(snapshot, mention, c.cfg.Streamer)
	reply, err := c.llm.Complete(ctx, prompt)
	if err != nil {
		slog.Warn("[chatter] LLM falhou para menção, usando frase do dicionário",
			"streamer", c.cfg.Streamer, "err", err)
		// Fallback para frase de agree/idle do dicionário.
		reply = PickPhrase(c.cfg.Streamer, ModeAgree)
	}

	if reply == "" {
		return
	}

	// Prefixar resposta com @username do remetente.
	fullReply := fmt.Sprintf("@%s %s", mention.Username, reply)
	if err := c.sendWithHumanDelay(ctx, fullReply); err != nil {
		slog.Warn("[chatter] erro ao enviar resposta de menção",
			"streamer", c.cfg.Streamer, "err", err)
	} else {
		slog.Info("[chatter] menção respondida",
			"streamer", c.cfg.Streamer, "para", mention.Username, "reply", fullReply)
	}
}

// sendWithHumanDelay envia a mensagem simulando tempo de digitação (~40 chars/segundo).
func (c *Chatter) sendWithHumanDelay(ctx context.Context, content string) error {
	// Simula tempo de digitação: 40 chars/seg com pequeno jitter.
	typingMs := len(content) * 25 // ~40 chars/s → 25ms por char
	jitterMs := rand.Intn(500)    //nolint:gosec
	typingDelay := time.Duration(typingMs+jitterMs) * time.Millisecond
	if typingDelay > 5*time.Second {
		typingDelay = 5 * time.Second
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(typingDelay):
	}

	return c.sendMessage(content)
}

// sendMessage envia uma mensagem via API REST da Kick.
func (c *Chatter) sendMessage(content string) error {
	c.mu.Lock()
	roomID := c.chatroomID
	c.mu.Unlock()

	if roomID == 0 {
		return fmt.Errorf("chatroom ainda não inicializado para '%s'", c.cfg.Streamer)
	}

	url := fmt.Sprintf("https://kick.com/api/v2/messages/send/%d", roomID)
	payload, err := json.Marshal(map[string]any{
		"content": content,
		"type":    "message",
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("criar request: %w", err)
	}
	req.Header.Set("User-Agent", browserUserAgent)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("Origin", "https://kick.com")
	req.Header.Set("Referer", fmt.Sprintf("https://kick.com/%s", c.cfg.Streamer))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// resolveChatroomID consulta a API da Kick para obter o ID da sala de chat.
func (c *Chatter) resolveChatroomID() (int64, error) {
	url := fmt.Sprintf("https://kick.com/api/v2/channels/%s", c.cfg.Streamer)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("criar request: %w", err)
	}
	req.Header.Set("User-Agent", browserUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("ler body: %w", err)
	}

	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return 0, fmt.Errorf("parse json: %w", err)
	}

	chatroom, ok := data["chatroom"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("campo 'chatroom' ausente na resposta")
	}
	idFloat, ok := chatroom["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("campo 'chatroom.id' inválido")
	}
	return int64(idFloat), nil
}

// detectMood analisa o buffer circular e retorna o mood predominante.
// Deve ser chamado com c.mu mantido.
func (c *Chatter) detectMood() ReactionMode {
	cutoff := time.Now().Add(-moodWindowSecs * time.Second)

	var (
		laughCount int
		hypeCount  int
		ggCount    int
		griefCount int
	)

	for _, m := range c.buffer {
		if m.Timestamp.Before(cutoff) {
			continue
		}
		lower := strings.ToLower(m.Content)
		for _, t := range laughTokens {
			if strings.Contains(lower, t) {
				laughCount++
				break
			}
		}
		for _, t := range hypeTokens {
			if strings.Contains(lower, t) {
				hypeCount++
				break
			}
		}
		for _, t := range ggTokens {
			if strings.Contains(lower, t) {
				ggCount++
				break
			}
		}
		for _, t := range griefTokens {
			if strings.Contains(lower, t) {
				griefCount++
				break
			}
		}
	}

	// Retorna o mood com maior contagem acima do limiar.
	best := moodThreshold - 1 // abaixo do limiar → idle
	mode := ModeIdle

	if laughCount > best {
		best = laughCount
		mode = ModeLaugh
	}
	if hypeCount > best {
		best = hypeCount
		mode = ModeHype
	}
	if ggCount > best {
		best = ggCount
		mode = ModeGG
	}
	if griefCount > best {
		mode = ModeGrief
	}

	return mode
}

// randomInterval retorna uma duração aleatória entre MinInterval e MaxInterval.
func (c *Chatter) randomInterval() time.Duration {
	spread := c.cfg.MaxInterval - c.cfg.MinInterval
	if spread <= 0 {
		return c.cfg.MinInterval
	}
	jitter := time.Duration(rand.Int63n(int64(spread))) //nolint:gosec
	return c.cfg.MinInterval + jitter
}
