package miner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kbelludoo/kick-miner-go/pkg/config"
)

func (m *Manager) autoFillTarget() int {
	if m.cfg.Settings.AutoFillTarget > 0 {
		return m.cfg.Settings.AutoFillTarget
	}
	return 12
}

func (m *Manager) aliveCountLocked(username string) int {
	n := 0
	for _, w := range m.watchers[username] {
		if w.IsAlive() {
			n++
		}
	}
	return n
}

// evictAutoFillLocked derruba o watcher de auto-fill mais recente quando o alvo já foi
// atingido, abrindo espaço para um favorito. Exige m.mu travado para escrita.
func (m *Manager) evictAutoFillLocked(username string) string {
	if !m.cfg.Settings.AutoFillEnabled || m.aliveCountLocked(username) < m.autoFillTarget() {
		return ""
	}

	var victim *StreamWatcher
	for _, w := range m.watchers[username] {
		if !w.IsAlive() || !w.IsAutoFill() {
			continue
		}
		if victim == nil || w.Duration() < victim.Duration() {
			victim = w
		}
	}
	if victim == nil {
		return ""
	}
	victim.Close()
	return victim.streamer
}

// promoteToFavorite transforma um watcher iniciado pelo auto-fill em favorito (não substituível).
func (m *Manager) promoteToFavorite(username string, w *StreamWatcher) {
	w.setAutoFill(false)
	m.mu.Lock()
	if st, ok := m.statuses[username]; ok {
		if sStatus, ok2 := st.Streamers[w.streamer]; ok2 {
			sStatus.AutoFill = false
			st.Streamers[w.streamer] = sStatus
		}
	}
	m.mu.Unlock()
}

// pruneOfflineWatchers encerra watchers cujas lives terminaram (ou reiniciaram com outro ID),
// liberando o slot para um favorito ou para o auto-fill.
func (m *Manager) pruneOfflineWatchers(acc config.AccountConfig) {
	m.mu.RLock()
	active := make([]*StreamWatcher, 0, len(m.watchers[acc.Username]))
	for _, w := range m.watchers[acc.Username] {
		if w.IsAlive() {
			active = append(active, w)
		}
	}
	m.mu.RUnlock()

	for _, w := range active {
		info, err := m.GetChannelInfo(acc.BearerToken, w.streamer)
		if err != nil || info == nil {
			continue
		}
		restarted := info.IsLive && info.StreamID != 0 && w.streamID != 0 && info.StreamID != w.streamID
		if info.IsLive && !restarted {
			continue
		}

		m.mu.Lock()
		if st, ok := m.statuses[acc.Username]; ok {
			if sStatus, ok2 := st.Streamers[w.streamer]; ok2 {
				sStatus.IsLive = info.IsLive
				st.Streamers[w.streamer] = sStatus
			}
		}
		m.mu.Unlock()

		w.Close()
		if restarted {
			m.AddLog(fmt.Sprintf("[%s] 🔄 Live de '%s' reiniciou; reconectando na nova transmissão.", acc.Username, w.streamer))
		} else {
			m.AddLog(fmt.Sprintf("[%s] 📴 Live de '%s' terminou; slot liberado.", acc.Username, w.streamer))
		}
	}
}

// checkAutoFill completa os slots livres até o alvo com lives descobertas na Kick.
func (m *Manager) checkAutoFill(ctx context.Context, acc config.AccountConfig) {
	target := m.autoFillTarget()

	m.mu.RLock()
	slotsNeeded := target - m.aliveCountLocked(acc.Username)
	m.mu.RUnlock()
	if slotsNeeded <= 0 {
		return
	}

	discovered := m.DiscoverLiveStreamers()
	if len(discovered) == 0 {
		m.mu.Lock()
		warn := time.Since(m.discoveryWarnAt) > 10*time.Minute
		if warn {
			m.discoveryWarnAt = time.Now()
		}
		m.mu.Unlock()
		if warn {
			m.AddLog(fmt.Sprintf("[%s] ⚠ [Auto-Fill] Nenhuma live descoberta na Kick; %d slot(s) ociosos.", acc.Username, slotsNeeded))
		}
		return
	}

	for _, clean := range discovered {
		if slotsNeeded <= 0 {
			break
		}

		m.mu.RLock()
		w, exists := m.watchers[acc.Username][clean]
		m.mu.RUnlock()
		if exists && w.IsAlive() {
			continue
		}

		info, err := m.GetChannelInfo(acc.BearerToken, clean)
		if err != nil || info == nil || !info.IsLive {
			continue
		}

		m.mu.Lock()
		activeMap := m.watchers[acc.Username]
		if existing, ok := activeMap[clean]; ok && existing.IsAlive() {
			m.mu.Unlock()
			continue
		}
		if m.aliveCountLocked(acc.Username) >= target {
			m.mu.Unlock()
			return
		}
		st := m.statuses[acc.Username]
		sStatus, ok := st.Streamers[clean]
		if !ok {
			sStatus = StreamerStatus{Streamer: clean, AutoFill: true}
		}
		sStatus.IsLive = true
		st.Streamers[clean] = sStatus

		watcher := NewStreamWatcher(m, acc.Username, acc.BearerToken, clean, info.ChannelID, info.UserID, info.StreamID, !m.favorites[acc.Username][clean])
		activeMap[clean] = watcher
		slotsNeeded--
		m.mu.Unlock()

		m.AddLog(fmt.Sprintf("[%s] ⚡ [Auto-Fill] Conectando live de '%s' para manter %d ativos!", acc.Username, clean, target))
		go watcher.Run(ctx)
	}
}

// DiscoverLiveStreamers busca na API pública da Kick os canais ao vivo no momento (PT e EN).
func (m *Manager) DiscoverLiveStreamers() []string {
	endpoints := []string{
		"https://kick.com/stream/featured-livestreams/pt",
		"https://kick.com/stream/featured-livestreams/en",
		"https://kick.com/stream/livestreams/pt?page=1&limit=30",
		"https://kick.com/stream/livestreams/en?page=1&limit=30",
	}

	seen := make(map[string]bool)
	var result []string
	add := func(slug string) {
		s := strings.ToLower(strings.TrimSpace(slug))
		if s != "" && !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}

	for _, url := range endpoints {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "application/json, text/plain, */*")
		req.Header.Set("Origin", "https://kick.com")

		resp, err := m.httpCli.Do(req)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			continue
		}

		// Formato paginado: { data: [ { channel: { slug } } ] }
		var paginated struct {
			Data []struct {
				Channel struct {
					Slug string `json:"slug"`
				} `json:"channel"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &paginated); err == nil && len(paginated.Data) > 0 {
			for _, item := range paginated.Data {
				add(item.Channel.Slug)
			}
			continue
		}

		// Formato array direto: [ { channel: { slug } } | { slug } ]
		var rawList []map[string]any
		if err := json.Unmarshal(body, &rawList); err == nil {
			for _, item := range rawList {
				if ch, ok := item["channel"].(map[string]any); ok {
					if slug, ok2 := ch["slug"].(string); ok2 {
						add(slug)
					}
				} else if slug, ok := item["slug"].(string); ok {
					add(slug)
				}
			}
		}
	}

	return result
}
