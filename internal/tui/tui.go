package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"minipulsar/internal/broker"
)

const (
	colorPink   = "\x1b[38;5;205m"
	colorPurple = "\x1b[38;5;135m"
	colorBlue   = "\x1b[38;5;45m"
	colorCyan   = "\x1b[38;5;87m"
	colorYellow = "\x1b[38;5;220m"
	colorDim    = "\x1b[38;5;244m"
	colorReset  = "\x1b[0m"
)

// Run starts the synthwave-themed TUI and blocks until the program exits.
func Run(b *broker.Broker, logCh <-chan string) error {
	model := &uiModel{
		broker:     b,
		logCh:      logCh,
		maxLogs:    200,
		lastUpdate: time.Now(),
	}
	return tea.NewProgram(model).Start()
}

type uiModel struct {
	broker     *broker.Broker
	logCh      <-chan string
	logs       []string
	maxLogs    int
	lastUpdate time.Time
	lastStats  broker.Stats
	lastBytes  uint64
	lastMsgs   uint64
	throughput float64
	msgRate    float64
}

func (m *uiModel) Init() tea.Cmd {
	return nil
}

func (m *uiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch typed := msg.(type) {
	case tea.KeyMsg:
		r := typed.Key()
		if r == 'q' || r == 'Q' {
			return m, func() tea.Msg { return tea.Quit() }
		}
	case tea.TickMsg:
		m.readLogs()
		m.updateStats()
	}
	return m, nil
}

func (m *uiModel) View() string {
	stats := m.lastStats

	header := fmt.Sprintf("%sMINIPULSAR%s  %sSynthwave TUI%s\n", colorPink, colorReset, colorPurple, colorReset)
	sub := fmt.Sprintf("%sPress Q to quit%s\n", colorDim, colorReset)

	statsLine := fmt.Sprintf(
		"%sTopics%s: %d  %sProducers%s: %d  %sConsumers%s: %d",
		colorCyan, colorReset, stats.Topics,
		colorCyan, colorReset, stats.Producers,
		colorCyan, colorReset, stats.Consumers,
	)

	throughput := fmt.Sprintf(
		"%sMsgs/s%s: %.2f  %sKB/s%s: %.2f  %sTotal msgs%s: %d",
		colorYellow, colorReset, m.msgRate,
		colorYellow, colorReset, m.throughput/1024.0,
		colorYellow, colorReset, stats.TotalMessages,
	)

	logHeader := fmt.Sprintf("%sLogs%s\n", colorPurple, colorReset)
	logBody := strings.Join(m.logs, "")
	if logBody == "" {
		logBody = fmt.Sprintf("%s(no log output yet)%s\n", colorDim, colorReset)
	}

	return strings.Join([]string{
		header,
		sub,
		statsLine,
		"\n",
		throughput,
		"\n\n",
		logHeader,
		logBody,
	}, "")
}

func (m *uiModel) readLogs() {
	for {
		select {
		case line := <-m.logCh:
			m.appendLog(line)
		default:
			return
		}
	}
}

func (m *uiModel) appendLog(line string) {
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	colored := fmt.Sprintf("%s%s%s", colorBlue, strings.TrimRight(line, "\n"), colorReset)
	m.logs = append(m.logs, colored+"\n")
	if len(m.logs) > m.maxLogs {
		m.logs = m.logs[len(m.logs)-m.maxLogs:]
	}
}

func (m *uiModel) updateStats() {
	now := time.Now()
	stats := m.broker.Snapshot()

	delta := now.Sub(m.lastUpdate).Seconds()
	if delta <= 0 {
		delta = 1
	}

	bytesDelta := float64(stats.TotalBytes - m.lastBytes)
	msgsDelta := float64(stats.TotalMessages - m.lastMsgs)
	m.throughput = bytesDelta / delta
	m.msgRate = msgsDelta / delta

	m.lastBytes = stats.TotalBytes
	m.lastMsgs = stats.TotalMessages
	m.lastStats = stats
	m.lastUpdate = now
}
