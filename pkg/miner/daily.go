package miner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/kbelludoo/kick-miner-go/pkg/config"
)

const (
	gamificationChallengesURL = "https://web.kick.com/api/v1/gamification/challenges"
	dailyErrorBackoff         = 30 * time.Minute
	dailyMaxWait              = 6 * time.Hour
)

type dailyChallenge struct {
	ID         string `json:"id"`
	Recurrence string `json:"recurrence"`
	Status     string `json:"status"` // in_progress | claimable | claimed
	Window     struct {
		StartsAt string `json:"starts_at"`
		EndsAt   string `json:"ends_at"`
	} `json:"window"`
	Condition struct {
		Threshold float64 `json:"threshold"` // minutos de assistência necessários
		Progress  float64 `json:"progress"`
	} `json:"condition"`
	Winner *struct {
		Name   string `json:"name"`
		Rarity string `json:"rarity"`
	} `json:"winner"`
}

func (m *Manager) runDailyClaims(ctx context.Context) {
	m.AddLog("Resgate automático de recompensas diárias ativado.")
	for _, acc := range m.cfg.Accounts {
		if acc.BearerToken == "" {
			continue
		}
		go m.dailyClaimLoop(ctx, acc)
	}
}

func (m *Manager) dailyClaimLoop(ctx context.Context, acc config.AccountConfig) {
	fallback := time.Duration(m.cfg.Settings.DailyClaimCheckMinutes) * time.Minute
	for {
		wait := m.checkDailyClaim(acc, fallback)
		if wait < time.Minute {
			wait = time.Minute
		}
		if wait > dailyMaxWait {
			wait = dailyMaxWait
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (m *Manager) checkDailyClaim(acc config.AccountConfig, fallback time.Duration) time.Duration {
	ch, err := m.fetchDailyChallenge(acc.BearerToken)
	if err != nil {
		m.AddLog(fmt.Sprintf("[%s] ⚠️ Falha ao consultar recompensa diária: %v (nova tentativa em 30 min)", acc.Username, err))
		return dailyErrorBackoff
	}
	if ch == nil {
		m.AddLog(fmt.Sprintf("[%s] ⚠️ Recompensa diária não encontrada na API (nova tentativa em %s)", acc.Username, fallback))
		return fallback
	}

	switch ch.Status {
	case "claimed":
		reset := dailyResetTime(ch)
		m.AddLog(fmt.Sprintf("[%s] ✅ Recompensa diária já resgatada. Próximo resgate: %s", acc.Username, reset.Format("02/01 15:04")))
		return untilNextCheck(reset, fallback)

	case "claimable":
		if ch.ID == "" {
			return fallback
		}
		if err := m.claimDailyChallenge(acc.BearerToken, ch.ID); err != nil {
			m.AddLog(fmt.Sprintf("[%s] ⚠️ Falha ao resgatar recompensa diária: %v (nova tentativa em 30 min)", acc.Username, err))
			return dailyErrorBackoff
		}

		reward := ""
		if ch2, err2 := m.fetchDailyChallenge(acc.BearerToken); err2 == nil && ch2 != nil && ch2.Winner != nil && ch2.Winner.Name != "" {
			reward = ch2.Winner.Name
			if ch2.Winner.Rarity != "" {
				reward += " (" + ch2.Winner.Rarity + ")"
			}
		}

		reset := dailyResetTime(ch)
		if reward != "" {
			m.AddLog(fmt.Sprintf("[%s] 🎁 Recompensa diária resgatada: %s! Próximo resgate: %s", acc.Username, reward, reset.Format("02/01 15:04")))
		} else {
			m.AddLog(fmt.Sprintf("[%s] 🎁 Recompensa diária resgatada! Próximo resgate: %s", acc.Username, reset.Format("02/01 15:04")))
		}
		return untilNextCheck(reset, fallback)

	case "in_progress":
		remaining := ch.Condition.Threshold - ch.Condition.Progress
		if remaining < 1 {
			remaining = 1
		}
		next := fallback
		if rm := time.Duration(remaining * float64(time.Minute)); rm < next {
			next = rm
		}
		return next

	default:
		return fallback
	}
}

func (m *Manager) dailyRequest(method, token, endpoint string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		return 0, nil, err
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Origin", "https://kick.com")
	req.Header.Set("Referer", "https://kick.com/")
	req.Header.Set("x-app-platform", "web")
	req.Header.Set("Cookie", "session_token="+token)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := m.httpCli.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func (m *Manager) fetchDailyChallenge(token string) (*dailyChallenge, error) {
	status, body, err := m.dailyRequest(http.MethodGet, token, gamificationChallengesURL, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("status %d", status)
	}

	var list []dailyChallenge
	var wrap struct {
		Data []dailyChallenge `json:"data"`
	}
	if err := json.Unmarshal(body, &wrap); err == nil && len(wrap.Data) > 0 {
		list = wrap.Data
	} else if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("formato inesperado da API de challenges")
	}

	for i := range list {
		if list[i].Recurrence == "daily" {
			return &list[i], nil
		}
	}
	return nil, nil
}

func (m *Manager) claimDailyChallenge(token, id string) error {
	endpoint := fmt.Sprintf("%s/%s/claim", gamificationChallengesURL, url.PathEscape(id))
	status, body, err := m.dailyRequest(http.MethodPost, token, endpoint, []byte("{}"))
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		snippet := string(body)
		if len(snippet) > 120 {
			snippet = snippet[:120]
		}
		return fmt.Errorf("status %d: %s", status, snippet)
	}
	return nil
}

// dailyResetTime usa o fim da janela do desafio (horário do servidor).
// Fallback: 00:02 UTC do dia seguinte, quando a API não informa ends_at.
func dailyResetTime(ch *dailyChallenge) time.Time {
	if ch.Window.EndsAt != "" {
		if t, err := time.Parse(time.RFC3339, ch.Window.EndsAt); err == nil {
			return t.Add(time.Minute)
		}
	}
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 2, 0, 0, time.UTC)
}

func untilNextCheck(reset time.Time, fallback time.Duration) time.Duration {
	d := time.Until(reset) + time.Minute // margem para o servidor abrir a nova janela
	if d < time.Minute {
		return fallback
	}
	return d
}
