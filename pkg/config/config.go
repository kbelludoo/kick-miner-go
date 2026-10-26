package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Accounts []AccountConfig `json:"accounts"`
	Settings SettingsConfig  `json:"settings"`
	Chatter  ChatterConfig   `json:"chatter"`
}

// ChatterConfig configura o modulo de interacao humanizada no chat.
type ChatterConfig struct {
	// Enabled ativa ou desativa o chatter globalmente.
	Enabled bool `json:"enabled"`
	// LLMAPIKey e a chave de API para o provedor LLM (opcional — le de .env se vazio).
	LLMAPIKey string `json:"llmApiKey"`
	// LLMModel e o modelo a usar (ex.: "free/gemini-3.8-flash").
	LLMModel string `json:"llmModel"`
	// LLMBaseURL e a URL base do provedor OpenAI-compat.
	LLMBaseURL string `json:"llmBaseURL"`
	// MinIntervalMinutes e o intervalo minimo entre mensagens periodicas (padrao: 13).
	MinIntervalMinutes int `json:"minIntervalMinutes"`
	// MaxIntervalMinutes e o intervalo maximo entre mensagens periodicas (padrao: 19).
	MaxIntervalMinutes int `json:"maxIntervalMinutes"`
	// Streamers lista os slugs dos streamers onde o chatter deve atuar.
	Streamers []string `json:"streamers"`
}

// StreamerGroup define um grupo independente de streamers com seu proprio limite de slots.
type StreamerGroup struct {
	Name          string   `json:"name"`
	Streamers     []string `json:"streamers"`
	MaxConcurrent int      `json:"maxConcurrent"`
}

type AccountConfig struct {
	Username            string          `json:"username"`
	BearerToken         string          `json:"bearerToken"`
	Streamers           []string        `json:"streamers"`           // legado: lista plana
	MaxConcurrentStreams int             `json:"maxConcurrentStreams"` // legado
	Groups              []StreamerGroup `json:"groups"`               // novo: grupos independentes
}

// GetEffectiveGroups retorna os grupos a usar.
// Se "groups" estiver definido, usa eles. Caso contrario, encapsula a lista legada num grupo unico.
func (a AccountConfig) GetEffectiveGroups() []StreamerGroup {
	if len(a.Groups) > 0 {
		for i := range a.Groups {
			if a.Groups[i].MaxConcurrent <= 0 {
				a.Groups[i].MaxConcurrent = 2
			}
		}
		return a.Groups
	}
	max := a.MaxConcurrentStreams
	if max <= 0 {
		max = 2
	}
	return []StreamerGroup{{
		Name:          "default",
		Streamers:     a.Streamers,
		MaxConcurrent: max,
	}}
}

type SettingsConfig struct {
	CheckIntervalSeconds     int   `json:"checkIntervalSeconds"`
	PingIntervalSeconds      int   `json:"pingIntervalSeconds"`
	TrackingIntervalSeconds  int   `json:"trackingIntervalSeconds"`
	PointsLogIntervalMinutes int   `json:"pointsLogIntervalMinutes"`
	WebDashboardPort         int   `json:"webDashboardPort"`
	AutoFillEnabled          bool  `json:"autoFillEnabled"`
	AutoFillTarget           int   `json:"autoFillTarget"`
	DailyClaimEnabled        *bool `json:"dailyClaimEnabled"`      // padrao: true
	DailyClaimCheckMinutes   int   `json:"dailyClaimCheckMinutes"` // padrao: 15
}

// IsDailyClaimEnabled retorna se o resgate automatico de recompensas diarias esta ativo.
func (s SettingsConfig) IsDailyClaimEnabled() bool {
	return s.DailyClaimEnabled == nil || *s.DailyClaimEnabled
}

func LoadConfig(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			exampleData, errEx := os.ReadFile("config.example.json")
			if errEx == nil {
				_ = os.WriteFile(filePath, exampleData, 0644)
				return nil, fmt.Errorf("arquivo '%s' nao existia e foi criado a partir de 'config.example.json'. Configure seus tokens nele", filePath)
			}
		}
		return nil, fmt.Errorf("falha ao ler '%s': %w", filePath, err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("falha ao parsear JSON de '%s': %w", filePath, err)
	}

	if cfg.Settings.CheckIntervalSeconds <= 0 {
		cfg.Settings.CheckIntervalSeconds = 45
	}
	if cfg.Settings.PingIntervalSeconds <= 0 {
		cfg.Settings.PingIntervalSeconds = 30
	}
	if cfg.Settings.TrackingIntervalSeconds <= 0 {
		cfg.Settings.TrackingIntervalSeconds = 10
	}
	if cfg.Settings.PointsLogIntervalMinutes <= 0 {
		cfg.Settings.PointsLogIntervalMinutes = 5
	}
	if cfg.Settings.WebDashboardPort <= 0 {
		cfg.Settings.WebDashboardPort = 3007
	}
	if cfg.Settings.DailyClaimCheckMinutes <= 0 {
		cfg.Settings.DailyClaimCheckMinutes = 15
	}
	// Defaults do chatter
	if cfg.Chatter.MinIntervalMinutes <= 0 {
		cfg.Chatter.MinIntervalMinutes = 13
	}
	if cfg.Chatter.MaxIntervalMinutes <= 0 {
		cfg.Chatter.MaxIntervalMinutes = 19
	}

	for i := range cfg.Accounts {
		if cfg.Accounts[i].MaxConcurrentStreams <= 0 {
			cfg.Accounts[i].MaxConcurrentStreams = 2
		}
	}

	return &cfg, nil
}
