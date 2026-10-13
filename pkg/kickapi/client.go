package kickapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// AppConfig armazena as credenciais oficiais da Kick Developer
type AppConfig struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	RedirectURI  string `json:"redirectUri"`
}

// Client gerencia a comunicação com a API oficial da Kick (api.kick.com)
type Client struct {
	cfg        AppConfig
	httpClient *http.Client
}

// TokenResponse representa o retorno OAuth2 da Kick
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
}

// ChatMessage representa o payload para enviar mensagem no chat oficial
type ChatMessage struct {
	Content string `json:"content"`
	Type    string `json:"type"`
}

func NewClient(cfg AppConfig) *Client {
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// GetAuthorizationURL gera o link para autorização OAuth 2.1
func (c *Client) GetAuthorizationURL(state string) string {
	u, _ := url.Parse("https://id.kick.com/oauth/authorize")
	q := u.Query()
	q.Set("client_id", c.cfg.ClientID)
	q.Set("redirect_uri", c.cfg.RedirectURI)
	q.Set("response_type", "code")
	q.Set("scope", "user:read chat:write channel:read channel:write events:subscribe")
	q.Set("state", state)
	u.RawQuery = q.Encode()
	return u.String()
}

// ExchangeCode troca o authorization code pelo access token
func (c *Client) ExchangeCode(ctx context.Context, code string) (*TokenResponse, error) {
	data := url.Values{}
	data.Set("grant_type", "authorization_code")
	data.Set("client_id", c.cfg.ClientID)
	data.Set("client_secret", c.cfg.ClientSecret)
	data.Set("redirect_uri", c.cfg.RedirectURI)
	data.Set("code", code)

	req, err := http.NewRequestWithContext(ctx, "POST", "https://id.kick.com/oauth/token", bytes.NewBufferString(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("falha na requisição OAuth: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("erro OAuth (%d): %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("erro ao decodificar token: %w", err)
	}
	return &tokenResp, nil
}

// SendChatMessage envia uma mensagem oficial no chat de um canal
func (c *Client) SendChatMessage(ctx context.Context, broadcasterID, token, message string) error {
	payload, _ := json.Marshal(ChatMessage{
		Content: message,
		Type:    "user",
	})

	url := fmt.Sprintf("https://api.kick.com/public/v1/chat/messages?broadcaster_user_id=%s", broadcasterID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("erro ao enviar mensagem (%d): %s", resp.StatusCode, string(b))
	}
	return nil
}
