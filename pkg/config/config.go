package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Accounts []AccountConfig `json:"accounts"`
	Settings SettingsConfig  `json:"settings"`
}

type AccountConfig struct {
	Username             string   `json:"username"`
	BearerToken          string   `json:"bearerToken"`
	Streamers            []string `json:"streamers"`
	MaxConcurrentStreams int      `json:"maxConcurrentStreams"`
}

type SettingsConfig struct {
	CheckIntervalSeconds     int `json:"checkIntervalSeconds"`
	PingIntervalSeconds      int `json:"pingIntervalSeconds"`
	TrackingIntervalSeconds  int `json:"trackingIntervalSeconds"`
	PointsLogIntervalMinutes int `json:"pointsLogIntervalMinutes"`
	WebDashboardPort         int `json:"webDashboardPort"`
}

func LoadConfig(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			exampleData, errEx := os.ReadFile("config.example.json")
			if errEx == nil {
				_ = os.WriteFile(filePath, exampleData, 0644)
				return nil, fmt.Errorf("arquivo '%s' não existia e foi criado a partir de 'config.example.json'. Configure seus tokens nele", filePath)
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

	for i := range cfg.Accounts {
		if cfg.Accounts[i].MaxConcurrentStreams <= 0 {
			cfg.Accounts[i].MaxConcurrentStreams = 2
		}
	}

	return &cfg, nil
}
