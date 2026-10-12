package miner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/kbelludoo/kick-miner-go/pkg/config"
)

type Manager struct {
	cfg       *config.Config
	mu        sync.RWMutex
	httpCli   *http.Client
	watchers  map[string]map[string]*StreamWatcher // username -> streamer -> watcher
	statuses  map[string]*AccountStatus
	logs      []string
	startedAt time.Time
}

func NewManager(cfg *config.Config) *Manager {
	m := &Manager{
		cfg: cfg,
		httpCli: &http.Client{
			Timeout: 15 * time.Second,
		},
		watchers:  make(map[string]map[string]*StreamWatcher),
		statuses:  make(map[string]*AccountStatus),
		logs:      make([]string, 0, 50),
		startedAt: time.Now(),
	}

	for _, acc := range cfg.Accounts {
		m.watchers[acc.Username] = make(map[string]*StreamWatcher)
		st := &AccountStatus{
			Username:  acc.Username,
			Streamers: make(map[string]StreamerStatus),
		}
		for _, s := range acc.Streamers {
			clean := strings.ToLower(strings.TrimSpace(s))
			if clean != "" {
				st.Streamers[clean] = StreamerStatus{
					Streamer: clean,
				}
			}
		}
		m.statuses[acc.Username] = st
	}

	return m
}

func (m *Manager) Start(ctx context.Context) {
	m.AddLog(fmt.Sprintf("Iniciando Kick Miner Go para %d conta(s)...", len(m.cfg.Accounts)))

	// Checagem imediata inicial
	m.checkAllAccounts(ctx)

	checkInterval := time.Duration(m.cfg.Settings.CheckIntervalSeconds) * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.Close()
			return
		case <-ticker.C:
			m.checkAllAccounts(ctx)
		}
	}
}

func (m *Manager) checkAllAccounts(ctx context.Context) {
	for _, acc := range m.cfg.Accounts {
		go m.checkAccount(ctx, acc)
	}
}

func (m *Manager) checkAccount(ctx context.Context, acc config.AccountConfig) {
	if acc.BearerToken == "" {
		return
	}

	m.mu.RLock()
	activeMap := m.watchers[acc.Username]
	activeCount := 0
	for _, w := range activeMap {
		if w.IsAlive() {
			activeCount++
		}
	}
	m.mu.RUnlock()

	maxStreams := acc.MaxConcurrentStreams
	if maxStreams <= 0 {
		maxStreams = 2
	}

	for _, streamer := range acc.Streamers {
		clean := strings.ToLower(strings.TrimSpace(streamer))
		if clean == "" {
			continue
		}

		m.mu.RLock()
		w, exists := activeMap[clean]
		m.mu.RUnlock()

		if exists && w.IsAlive() {
			continue
		}

		// Se já atingiu o limite de transmissões simultâneas, para de abrir novas
		if activeCount >= maxStreams {
			break
		}

		// Checa se o canal está ao vivo na Kick
		info, err := m.GetChannelInfo(acc.BearerToken, clean)
		if err != nil || info == nil {
			continue
		}

		m.mu.Lock()
		st := m.statuses[acc.Username]
		if sStatus, ok := st.Streamers[clean]; ok {
			sStatus.IsLive = info.IsLive
			st.Streamers[clean] = sStatus
		}
		m.mu.Unlock()

		if !info.IsLive {
			continue
		}

		// Canal está ao vivo e temos vaga!
		m.mu.Lock()
		activeMap = m.watchers[acc.Username]
		if existing, ok := activeMap[clean]; ok && existing.IsAlive() {
			m.mu.Unlock()
			continue
		}

		watcher := NewStreamWatcher(m, acc.Username, acc.BearerToken, clean, info.ChannelID, info.UserID, info.StreamID)
		activeMap[clean] = watcher
		activeCount++
		m.mu.Unlock()

		go watcher.Run(ctx)
	}
}

func (m *Manager) GetChannelInfo(token, streamer string) (*ChannelInfo, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://kick.com/api/v2/channels/%s", streamer), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Origin", "https://kick.com")
	req.Header.Set("Referer", fmt.Sprintf("https://kick.com/%s/", streamer))

	resp, err := m.httpCli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}

	chIDFloat, ok := data["id"].(float64)
	if !ok {
		return nil, fmt.Errorf("sem id de canal")
	}

	userIDFloat, _ := data["user_id"].(float64)
	livestreamMap, isLiveMap := data["livestream"].(map[string]any)

	info := &ChannelInfo{
		Slug:      streamer,
		ChannelID: int64(chIDFloat),
		UserID:    int64(userIDFloat),
		IsLive:    isLiveMap && livestreamMap != nil,
	}

	if info.IsLive {
		if streamIDFloat, ok := livestreamMap["id"].(float64); ok {
			info.StreamID = int64(streamIDFloat)
		}
	}

	return info, nil
}

func (m *Manager) FetchPoints(token, streamer string) (int, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://kick.com/api/v2/channels/%s/points", streamer), nil)
	if err != nil {
		return 0, err
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Origin", "https://kick.com")
	req.Header.Set("Referer", fmt.Sprintf("https://kick.com/%s/", streamer))

	resp, err := m.httpCli.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var res struct {
		Data struct {
			Points int `json:"points"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return 0, err
	}

	return res.Data.Points, nil
}

func (m *Manager) updateStreamerWatching(username, streamer string, watching bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if st, ok := m.statuses[username]; ok {
		if sStatus, ok2 := st.Streamers[streamer]; ok2 {
			sStatus.Watching = watching
			st.Streamers[streamer] = sStatus
		}
		// Recalcula ativas
		active := 0
		for _, s := range st.Streamers {
			if s.Watching {
				active++
			}
		}
		st.ActiveStreams = active
	}
}

func (m *Manager) updateStreamerPoints(username, streamer string, points int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if st, ok := m.statuses[username]; ok {
		if sStatus, ok2 := st.Streamers[streamer]; ok2 {
			sStatus.Points = points
			st.Streamers[streamer] = sStatus
		}
		// Soma total
		tot := 0
		for _, s := range st.Streamers {
			tot += s.Points
		}
		st.TotalPoints = tot
	}
}

func (m *Manager) AddLog(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Println(msg)
	entry := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
	m.logs = append(m.logs, entry)
	if len(m.logs) > 30 {
		m.logs = m.logs[1:]
	}
}

func (m *Manager) GetTelemetry() GlobalTelemetry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	ramMb := float64(mem.Alloc) / (1024 * 1024)

	accCopies := make(map[string]AccountStatus)
	totalStreams := 0
	for k, v := range m.statuses {
		cp := *v
		cp.Streamers = make(map[string]StreamerStatus)
		for sk, sv := range v.Streamers {
			// Adiciona tempo se estiver assistindo
			if sv.Watching {
				if w, ok := m.watchers[k][sk]; ok && w.IsAlive() {
					sv.WatchTime = w.Duration().Round(time.Second).String()
				}
			}
			cp.Streamers[sk] = sv
		}
		accCopies[k] = cp
		totalStreams += cp.ActiveStreams
	}

	logCopies := make([]string, len(m.logs))
	copy(logCopies, m.logs)

	return GlobalTelemetry{
		Uptime:       time.Since(m.startedAt).Round(time.Second).String(),
		RamMb:        ramMb,
		Goroutines:   runtime.NumGoroutine(),
		TotalStreams: totalStreams,
		Accounts:     accCopies,
		Logs:         logCopies,
	}
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, accMap := range m.watchers {
		for _, w := range accMap {
			w.Close()
		}
	}
}
