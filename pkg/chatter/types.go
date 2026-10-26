package chatter

import "time"

// ReactionMode define qual comportamento de reação está ativo.
type ReactionMode int

const (
	ModeIdle      ReactionMode = iota // Nenhum mood detectado — usa frases idle
	ModeLaugh                         // Detectou risadas (kkk, lol, etc.)
	ModeHype                          // Detectou hype (VAMO, GOO, etc.)
	ModePlayGood                      // Jogada boa detectada
	ModePlayBad                       // Jogada ruim detectada
	ModeGG                            // Fim de partida
	ModeGrief                         // Perda pesada
	ModeAgree                         // Concordância com o streamer
)

// String retorna o nome legível do modo.
func (m ReactionMode) String() string {
	switch m {
	case ModeLaugh:
		return "laugh"
	case ModeHype:
		return "hype"
	case ModePlayGood:
		return "play_good"
	case ModePlayBad:
		return "play_bad"
	case ModeGG:
		return "gg"
	case ModeGrief:
		return "grief"
	case ModeAgree:
		return "agree"
	default:
		return "idle"
	}
}

// ChatterConfig reúne toda a configuração necessária para um Chatter.
type ChatterConfig struct {
	// Token Bearer da conta Kick.
	Token string
	// Slug do streamer (ex.: "baianotv").
	Streamer string
	// Username da conta que será usada para enviar mensagens (ex.: "KbElLuDoO").
	Username string
	// Intervalo mínimo entre mensagens periódicas (13 min por padrão).
	MinInterval time.Duration
	// Intervalo máximo entre mensagens periódicas (19 min por padrão).
	MaxInterval time.Duration
	// Chave de API OpenAI-compat para o LLM (pode ser vazia — tenta env).
	LLMAPIKey string
	// URL base do provedor LLM (ex.: "https://apinex.io/v1").
	LLMBaseURL string
	// Modelo a usar (ex.: "free/gemini-3.8-flash").
	LLMModel string
}

// ChatMessage representa uma mensagem recebida do chat Kick.
type ChatMessage struct {
	Username  string
	Content   string
	Timestamp time.Time
}

// ChannelContext mantém estado do canal: buffer circular e mood atual.
type ChannelContext struct {
	// Últimas N mensagens para análise de mood.
	RecentMessages []ChatMessage
	// Mood predominante nos últimos 30s.
	CurrentMood ReactionMode
	// Timestamp da última mensagem periódica enviada.
	LastSent time.Time
}
