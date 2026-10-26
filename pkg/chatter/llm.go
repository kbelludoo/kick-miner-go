package chatter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultLLMBaseURL = "https://apinex.io/v1"
	defaultLLMModel   = "free/gemini-3.8-flash"
	llmTimeout        = 8 * time.Second

	// System prompt de persona do chatter humanizado.
	llmSystemPrompt = `Você é um viewer brasileiro comum no chat da Kick chamado KbElLuDoO.
Você assiste lives de streamers brasileiros e responde de forma casual, curta e natural.
Nunca escreva mais de 2 frases. Use gírias BR quando fizer sentido.
Não comece com "Olá" ou "Oi". Seja direto e humano.
Não mencione que você é um bot ou IA.`
)

// openAIRequest é o payload enviado para a API OpenAI-compat.
type openAIRequest struct {
	Model    string          `json:"model"`
	Messages []openAIMessage `json:"messages"`
}

// openAIMessage representa uma mensagem no histórico do LLM.
type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// openAIResponse é a resposta da API OpenAI-compat.
type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// llmClient é o cliente HTTP leve para APIs OpenAI-compat.
type llmClient struct {
	apiKey  string
	baseURL string
	model   string
	http    *http.Client
}

// newLLMClient cria um cliente LLM resolvendo credenciais da config, .env e variáveis de ambiente.
func newLLMClient(cfg ChatterConfig) *llmClient {
	dotenvLoad()

	apiKey := cfg.LLMAPIKey
	if apiKey == "" {
		apiKey = os.Getenv("APINEX_API_KEY")
	}

	baseURL := cfg.LLMBaseURL
	if baseURL == "" {
		baseURL = defaultLLMBaseURL
	}

	model := cfg.LLMModel
	if model == "" {
		envModel := os.Getenv("APINEX_MODEL")
		if envModel != "" {
			model = envModel
		} else {
			model = defaultLLMModel
		}
	}

	return &llmClient{
		apiKey:  apiKey,
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		http:    &http.Client{Timeout: llmTimeout},
	}
}

// dotenvLoad lê o arquivo .env do diretório atual e seta variáveis de ambiente
// apenas se ainda não estiverem definidas.
func dotenvLoad() {
	f, err := os.Open(".env")
	if err != nil {
		return // .env é opcional
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		if os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
}

// Complete envia um prompt ao LLM e retorna a resposta textual.
// Faz fallback para TOKENHARBOR_API_KEY se a chave primária falhar.
func (c *llmClient) Complete(ctx context.Context, userPrompt string) (string, error) {
	resp, err := c.doRequest(ctx, c.apiKey, userPrompt)
	if err != nil {
		// Fallback: tenta com TOKENHARBOR_API_KEY
		fallbackKey := os.Getenv("TOKENHARBOR_API_KEY")
		if fallbackKey == "" || fallbackKey == c.apiKey {
			return "", fmt.Errorf("llm request falhou: %w", err)
		}
		slog.Warn("[chatter/llm] chave primária falhou, tentando fallback", "err", err)
		resp, err = c.doRequest(ctx, fallbackKey, userPrompt)
		if err != nil {
			return "", fmt.Errorf("llm fallback falhou: %w", err)
		}
	}
	return resp, nil
}

// doRequest executa a chamada HTTP para a API OpenAI-compat.
func (c *llmClient) doRequest(ctx context.Context, apiKey, userPrompt string) (string, error) {
	if apiKey == "" {
		return "", fmt.Errorf("api key vazia — configure APINEX_API_KEY ou llmApiKey no config.json")
	}

	payload := openAIRequest{
		Model: c.model,
		Messages: []openAIMessage{
			{Role: "system", Content: llmSystemPrompt},
			{Role: "user", Content: userPrompt},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("criar request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	res, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("http do: %w", err)
	}
	defer res.Body.Close()

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("ler body: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d: %s", res.StatusCode, string(resBody))
	}

	var parsed openAIResponse
	if err := json.Unmarshal(resBody, &parsed); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}

	if len(parsed.Choices) == 0 || parsed.Choices[0].Message.Content == "" {
		return "", fmt.Errorf("resposta vazia do LLM")
	}

	return strings.TrimSpace(parsed.Choices[0].Message.Content), nil
}
