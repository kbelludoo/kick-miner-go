package chatter

import "math/rand"

// @mbvm:dict mood=idle streamer=baianotv
var idleBaiano = []string{
	"assistindo quieto aqui no fundão",
	"baião on",
	"caçando os baicoins 🪙",
	"boa live baião",
	"sempre aqui",
	"de olho no jogo",
	"aqui firmão baião",
}

// @mbvm:dict mood=laugh streamer=baianotv
var laughBaiano = []string{
	"KKKKKK caiu",
	"mano que isso kkkk",
	"nao tankei kkkk",
	"morri kkk",
	"kkkkkkk cara",
	"KAKAKAKA",
	"caiu geral kkkkk",
	"que cena mano kkkk",
}

// @mbvm:dict mood=hype streamer=baianotv
var hypeBaiano = []string{
	"CHEGA",
	"VAMOOOOO",
	"isso aí baião",
	"AGORA SIM",
	"bora bora bora",
	"CHEGOOOU",
	"VAI BAIÃO",
	"ISSO",
}

// @mbvm:dict mood=play_good streamer=baianotv
var playGoodBaiano = []string{
	"que mecânica",
	"essa jogada foi bruta",
	"🔥🔥",
	"muito bom isso",
	"jogou demais",
	"brabo demais",
	"caramba que jogada",
	"mão de ouro",
}

// @mbvm:dict mood=play_bad streamer=baianotv
var playBadBaiano = []string{
	"aff",
	"mano...",
	"que feio isso",
	"não era pra fazer isso não",
	"ih",
	"nossa...",
	"vacilou hein",
	"podia ter evitado",
}

// @mbvm:dict mood=gg streamer=baianotv
var ggBaiano = []string{
	"gg",
	"gg wp",
	"foi fácil",
	"próximo",
	"tava dominado",
	"gg ez",
	"beleza demais",
	"passou fácil",
}

// @mbvm:dict mood=grief streamer=baianotv
var griefBaiano = []string{
	"q time esse vei",
	"doeu",
	"cruzes",
	"perdeu de um jeito feio hein",
	"tava indo bem...",
	"ai mano",
	"que tragédia isso",
	"miserável",
}

// @mbvm:dict mood=agree streamer=baianotv
var agreeBaiano = []string{
	"verdade",
	"faz sentido",
	"isso mesmo",
	"concordo",
	"exatamente",
	"é isso aí baião",
	"100%",
	"real",
}

// ─── Gaules ────────────────────────────────────────────────────────────────

// @mbvm:dict mood=idle streamer=gaules
var idleGaules = []string{
	"firme na tribo",
	"aqui na live do gaulão",
	"de olho",
	"tribo presente",
	"assistindo o classico",
	"tribo on",
	"de boa aqui",
	"sempre na tribo",
}

// @mbvm:dict mood=laugh streamer=gaules
var laughGaules = []string{
	"KKKKK que cena",
	"mano kkkk",
	"nao segurou kkk",
	"HAUAHAUHA",
	"caiu por água abaixo kkk",
	"geral kkkk",
	"que cena hein kkk",
	"não consegui segurar kkk",
}

// @mbvm:dict mood=hype streamer=gaules
var hypeGaules = []string{
	"VAMOOOO TRIBO",
	"isso aí gaulão",
	"FOGO 🔥",
	"A TRIBO NÃO PARA",
	"CHEGOOOU",
	"BORA TRIBO",
	"VAI GAULÃO",
	"A TRIBO É FORTE",
}

// @mbvm:dict mood=play_good streamer=gaules
var playGoodGaules = []string{
	"mira afiada 🎯",
	"que clutch",
	"jogou demais",
	"boa demais essa aí",
	"esse cara é bom",
	"perfeito",
	"que rifado",
	"clutchzão",
}

// @mbvm:dict mood=play_bad streamer=gaules
var playBadGaules = []string{
	"vixi",
	"passou batido",
	"oof",
	"capotou",
	"ai ai",
	"vacilou",
	"podia ter pego",
	"perdeu a mira ali",
}

// @mbvm:dict mood=gg streamer=gaules
var ggGaules = []string{
	"gg tribo",
	"dominou",
	"gg fácil",
	"próxima",
	"lavou",
	"gg wp tribo",
	"foi clean",
	"varreu",
}

// @mbvm:dict mood=grief streamer=gaules
var griefGaules = []string{
	"foi roubado",
	"complicado esse mapa",
	"podia ter ganhado",
	"aí dói",
	"tribo chora",
	"que mapa ruim",
	"doeu na alma",
	"próxima tribo",
}

// @mbvm:dict mood=agree streamer=gaules
var agreeGaules = []string{
	"verdade gaulão",
	"isso aí",
	"certeza",
	"de fato",
	"é exatamente isso",
	"100% gaulão",
	"a tribo concorda",
	"real demais",
}

// ─── Default ────────────────────────────────────────────────────────────────

// @mbvm:dict mood=idle streamer=default
var idleDefault = []string{
	"assistindo aqui",
	"boa live",
	"de olho no chat",
	"quieto aqui",
	"presente",
	"on aqui",
	"sempre aqui ó",
	"de boa",
}

// @mbvm:dict mood=laugh streamer=default
var laughDefault = []string{
	"kkkkk",
	"mano kkkk",
	"que cena kkk",
	"KKKKK",
	"morri kkkk",
	"HAUAHAUHA",
	"não aguentei kkkk",
	"socorro kkk",
}

// @mbvm:dict mood=hype streamer=default
var hypeDefault = []string{
	"VAI!",
	"bora!",
	"isso aí!",
	"VAMOS",
	"🔥",
	"CHEGOU",
	"ISSO MESMO",
	"VAI VAI VAI",
}

// @mbvm:dict mood=play_good streamer=default
var playGoodDefault = []string{
	"muito bom",
	"que jogada",
	"brabo",
	"foi bonito isso",
	"👏",
	"perfeito",
	"demais demais",
	"top jogada",
}

// @mbvm:dict mood=play_bad streamer=default
var playBadDefault = []string{
	"aff",
	"ih",
	"que pena",
	"oof",
	"mano...",
	"nossa...",
	"vacilou ali",
	"complicou",
}

// @mbvm:dict mood=gg streamer=default
var ggDefault = []string{
	"gg",
	"gg wp",
	"próximo",
	"foi",
	"boa",
	"gg fácil",
	"passou limpo",
	"gg ez",
}

// @mbvm:dict mood=grief streamer=default
var griefDefault = []string{
	"doeu",
	"foi feio",
	"que perda",
	"ai",
	"tava ganhando",
	"cruzes",
	"que perda hein",
	"dói demais",
}

// @mbvm:dict mood=agree streamer=default
var agreeDefault = []string{
	"verdade",
	"isso",
	"faz sentido",
	"concordo",
	"exato",
	"real",
	"é isso",
	"100%",
}

// dictIndex mapeia "streamer:mood" → slice de frases.
var dictIndex = map[string][]string{
	"baianotv:idle":      idleBaiano,
	"baianotv:laugh":     laughBaiano,
	"baianotv:hype":      hypeBaiano,
	"baianotv:play_good": playGoodBaiano,
	"baianotv:play_bad":  playBadBaiano,
	"baianotv:gg":        ggBaiano,
	"baianotv:grief":     griefBaiano,
	"baianotv:agree":     agreeBaiano,

	"gaules:idle":      idleGaules,
	"gaules:laugh":     laughGaules,
	"gaules:hype":      hypeGaules,
	"gaules:play_good": playGoodGaules,
	"gaules:play_bad":  playBadGaules,
	"gaules:gg":        ggGaules,
	"gaules:grief":     griefGaules,
	"gaules:agree":     agreeGaules,

	"default:idle":      idleDefault,
	"default:laugh":     laughDefault,
	"default:hype":      hypeDefault,
	"default:play_good": playGoodDefault,
	"default:play_bad":  playBadDefault,
	"default:gg":        ggDefault,
	"default:grief":     griefDefault,
	"default:agree":     agreeDefault,
}

// PickPhrase retorna uma frase aleatória para o streamer e mood fornecidos.
// Cai em "default" caso o streamer não exista no dicionário.
func PickPhrase(streamer string, mode ReactionMode) string {
	mood := mode.String()
	key := streamer + ":" + mood
	phrases, ok := dictIndex[key]
	if !ok || len(phrases) == 0 {
		key = "default:" + mood
		phrases = dictIndex[key]
	}
	if len(phrases) == 0 {
		return ""
	}
	return phrases[rand.Intn(len(phrases))] //nolint:gosec
}
