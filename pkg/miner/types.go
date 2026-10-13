package miner

type ChannelInfo struct {
	ChannelID int64  `json:"channelId"`
	UserID    int64  `json:"userId"`
	StreamID  int64  `json:"streamId"`
	Slug      string `json:"slug"`
	IsLive    bool   `json:"isLive"`
}

type StreamerStatus struct {
	Streamer  string `json:"streamer"`
	IsLive    bool   `json:"isLive"`
	Watching  bool   `json:"watching"`
	AutoFill  bool   `json:"autoFill"`
	Points    int    `json:"points"`
	WatchTime string `json:"watchTime"`
	LastError string `json:"lastError,omitempty"`
}

type AccountStatus struct {
	Username      string                    `json:"username"`
	ActiveStreams int                       `json:"activeStreams"`
	TotalPoints   int                       `json:"totalPoints"`
	Streamers     map[string]StreamerStatus `json:"streamers"`
}

type GlobalTelemetry struct {
	Uptime       string                   `json:"uptime"`
	RamMb        float64                  `json:"ramMb"`
	Goroutines   int                      `json:"goroutines"`
	TotalStreams int                      `json:"totalStreams"`
	Accounts     map[string]AccountStatus `json:"accounts"`
	Logs         []string                 `json:"logs"`
}
