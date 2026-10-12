# Etapa de compilação
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o kick-miner .

# Imagem final mínima em Scratch / Alpine (apenas ~12 MB)
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app

COPY --from=builder /app/kick-miner .
COPY config.example.json .

EXPOSE 3007

ENTRYPOINT ["./kick-miner"]
