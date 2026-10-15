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
	favorites map[string]map[string]bool // username -> streamers configurados nos grupos
	checking  map[string]bool            // username -> checagem em andamento
	logs      []string

	discoveryWarnAt time.Time
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
		favorites: make(map[string]map[string]bool),
		checking:  make(map[string]bool),
		logs:      make([]string, 0, 100),
		startedAt: time.Now(),
	}

	for _, acc := range cfg.Accounts {
		m.watchers[acc.Username] = make(map[string]*StreamWatcher)
		favs := make(map[string]bool)
		st := &AccountStatus{
			Username:  acc.Username,
			Streamers: make(map[string]StreamerStatus),
		}
		// Inicializa status de todos os streamers de todos os grupos
		for _, group := range acc.GetEffectiveGroups() {
			for _, s := range group.Streamers {
				clean := strings.ToLower(strings.TrimSpace(s))
				if clean != "" {
					favs[clean] = true
					st.Streamers[clean] = StreamerStatus{
						Streamer: clean,
					}
				}
			}
		}
		m.favorites[acc.Username] = favs
		m.statuses[acc.Username] = st
	}

	return m
}

func (m *Manager) Start(ctx context.Context) {
	m.AddLog(fmt.Sprintf("Iniciando Kick Miner Go para %d conta(s)...", len(m.cfg.Accounts)))

	// Resgate automático da recompensa diária (API de gamificação)
	if m.cfg.Settings.IsDailyClaimEnabled() {
		go m.runDailyClaims(ctx)
	}

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

	m.mu.Lock()
	if m.checking[acc.Username] {
		m.mu.Unlock()
		return
	}
	m.checking[acc.Username] = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.checking, acc.Username)
		m.mu.Unlock()
	}()

	// Libera slots de lives que já terminaram
	m.pruneOfflineWatchers(acc)

	// Cada grupo roda de forma independente (slots separados)
	for _, group := range acc.GetEffectiveGroups() {
		m.checkGroup(ctx, acc, group)
	}

	// Se AutoFill estiver ativo, completa os slots restantes até o alvo (ex: 12)
	if m.cfg.Settings.AutoFillEnabled {
		m.checkAutoFill(ctx, acc)
	}
}

// checkGroup gerencia os slots de um grupo de streamers de forma independente.
func (m *Manager) checkGroup(ctx context.Context, acc config.AccountConfig, group config.StreamerGroup) {
	m.mu.RLock()
	activeMap := m.watchers[acc.Username]
	// Conta apenas watchers que pertencem a ESTE grupo
	activeCount := 0
	groupSet := make(map[string]bool, len(group.Streamers))
	for _, s := range group.Streamers {
		clean := strings.ToLower(strings.TrimSpace(s))
		if clean != "" {
			groupSet[clean] = true
		}
	}
	for streamer, w := range activeMap {
		if groupSet[streamer] && w.IsAlive() {
			activeCount++
		}
	}
	m.mu.RUnlock()

	maxStreams := group.MaxConcurrent
	if maxStreams <= 0 {
		maxStreams = 2
	}

	for _, streamer := range group.Streamers {
		clean := strings.ToLower(strings.TrimSpace(streamer))
		if clean == "" {
			continue
		}

		m.mu.RLock()
		w, exists := activeMap[clean]
		m.mu.RUnlock()

		if exists && w.IsAlive() {
			if w.IsAutoFill() {
				m.promoteToFavorite(acc.Username, w)
			}
			continue
		}

		if activeCount >= maxStreams {
			break
		}

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

		m.mu.Lock()
		activeMap = m.watchers[acc.Username]
		if existing, ok := activeMap[clean]; ok && existing.IsAlive() {
			m.mu.Unlock()
			continue
		}

		evicted := m.evictAutoFillLocked(acc.Username)
		watcher := NewStreamWatcher(m, acc.Username, acc.BearerToken, clean, info.ChannelID, info.UserID, info.StreamID, false)
		activeMap[clean] = watcher
		activeCount++
		m.mu.Unlock()

		if evicted != "" {
			m.AddLog(fmt.Sprintf("[%s] 🔁 Favorito '%s' entrou ao vivo: substituindo auto-fill '%s'.", acc.Username, clean, evicted))
		}
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

// setWatching atualiza o status apenas se o watcher ainda for o atual do streamer
// (um watcher antigo encerrando não pode sobrescrever o novo).
func (m *Manager) setWatching(w *StreamWatcher, watching bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.watchers[w.username][w.streamer] != w {
		return
	}
	if st, ok := m.statuses[w.username]; ok {
		if sStatus, ok2 := st.Streamers[w.streamer]; ok2 {
			sStatus.Watching = watching
			st.Streamers[w.streamer] = sStatus
		}
		recalcLocked(st)
	}
}

// watcherEnded remove o watcher finalizado e descarta o status de streamers do auto-fill.
func (m *Manager) watcherEnded(w *StreamWatcher) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.watchers[w.username][w.streamer] != w {
		return
	}
	delete(m.watchers[w.username], w.streamer)

	st, ok := m.statuses[w.username]
	if !ok {
		return
	}
	if m.favorites[w.username][w.streamer] {
		if sStatus, ok2 := st.Streamers[w.streamer]; ok2 {
			sStatus.Watching = false
			st.Streamers[w.streamer] = sStatus
		}
	} else {
		delete(st.Streamers, w.streamer)
	}
	recalcLocked(st)
}

func recalcLocked(st *AccountStatus) {
	active, tot := 0, 0
	for _, s := range st.Streamers {
		if s.Watching {
			active++
		}
		tot += s.Points
	}
	st.ActiveStreams = active
	st.TotalPoints = tot
}

func (m *Manager) updateStreamerPoints(username, streamer string, points int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if st, ok := m.statuses[username]; ok {
		if sStatus, ok2 := st.Streamers[streamer]; ok2 {
			sStatus.Points = points
			st.Streamers[streamer] = sStatus
		}
		recalcLocked(st)
	}
}

func (m *Manager) AddLog(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Println(msg)
	entry := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
	m.logs = append(m.logs, entry)
	if len(m.logs) > 100 {
		m.logs = m.logs[1:]
	}
}

func (m *Manager) GetTelemetry() GlobalTelemetry {
	// ReadMemStats faz stop-the-world; chamar fora do lock para não bloquear writers
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	ramMb := float64(mem.Alloc) / (1024 * 1024)

	m.mu.RLock()
	defer m.mu.RUnlock()

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
