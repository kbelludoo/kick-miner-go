package chatter

import (
	"strings"
)

// mentionTargets lista todas as variações do username que devem acionar resposta via LLM.
var mentionTargets = []string{
	"@kbelludoo",
	"kbelludoo",
	"@kbelludoo,",
	"@kbelludoo:",
}

// IsMention retorna true se o conteúdo da mensagem mencionar o username do chatter.
func IsMention(content, username string) bool {
	lower := strings.ToLower(content)
	lowerUser := strings.ToLower(username)

	// Verifica variações diretas do username
	for _, t := range mentionTargets {
		if strings.Contains(lower, t) {
			return true
		}
	}

	// Verifica @<username> genérico para suportar qualquer username configurado
	if strings.Contains(lower, "@"+lowerUser) {
		return true
	}

	return false
}

// BuildMentionPrompt monta o prompt contextual enviado ao LLM quando o chatter é mencionado.
// Inclui as últimas mensagens do chat + a pergunta do usuário.
func BuildMentionPrompt(recentMsgs []ChatMessage, mentionMsg ChatMessage, streamer string) string {
	var sb strings.Builder

	sb.WriteString("Você está assistindo a live de ")
	sb.WriteString(streamer)
	sb.WriteString(" na Kick. ")
	sb.WriteString("Contexto recente do chat (últimas mensagens):\n")

	// Inclui até as últimas 5 mensagens como contexto
	start := 0
	if len(recentMsgs) > 5 {
		start = len(recentMsgs) - 5
	}
	for _, m := range recentMsgs[start:] {
		sb.WriteString(m.Username)
		sb.WriteString(": ")
		sb.WriteString(m.Content)
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	sb.WriteString(mentionMsg.Username)
	sb.WriteString(" disse: ")
	sb.WriteString(mentionMsg.Content)
	sb.WriteString("\n\nResponda de forma curta e casual em português BR.")

	return sb.String()
}

// ExtractQuestion extrai a parte relevante da mensagem de menção,
// removendo o @username para deixar apenas a pergunta/conteúdo.
func ExtractQuestion(content, username string) string {
	lower := strings.ToLower(content)
	lowerUser := "@" + strings.ToLower(username)

	idx := strings.Index(lower, lowerUser)
	if idx >= 0 {
		// Remove o @username e retorna o restante
		after := strings.TrimSpace(content[idx+len(lowerUser):])
		if after != "" {
			return after
		}
	}
	return content
}
