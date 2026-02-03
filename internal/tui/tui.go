package tui

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"minipulsar/internal/broker"
)

const (
	defaultTopLimit = 10
	logBufferMax    = 1000
)

type statsMsg struct {
	stats broker.StatsSnapshot
	err   error
}

type logMsg struct {
	line string
	ok   bool
}

type tickMsg time.Time

type model struct {
	broker *broker.Broker
	logCh  <-chan string
	level  *slog.LevelVar

	width   int
	height  int
	ready   bool
	layout  layout
	padding int

	stats broker.StatsSnapshot
	err   error

	viewport viewport.Model
	logs     []string
	logLevel slog.Level

	delayLevel int
	paused     bool
}

// NewProgram builds a Bubble Tea program that renders broker stats and logs.
func NewProgram(b *broker.Broker, logCh <-chan string, levelVar *slog.LevelVar, level slog.Level) *tea.Program {
	m := model{
		broker:   b,
		logCh:    logCh,
		level:    levelVar,
		logLevel: level,
		paused:   b.ThrottlePaused(),
	}
	m.delayLevel = b.ThrottleLevel()
	return tea.NewProgram(m, tea.WithAltScreen())
}

func (m model) Init() tea.Cmd {
	return tea.Batch(tick(), waitForLog(m.logCh))
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func waitForLog(logCh <-chan string) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-logCh
		return logMsg{line: line, ok: ok}
	}
}

func fetchStats(b *broker.Broker) tea.Cmd {
	return func() tea.Msg {
		stats, err := b.StatsSnapshot(defaultTopLimit)
		return statsMsg{stats: stats, err: err}
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.resize()
		return m, nil
	case tickMsg:
		return m, tea.Batch(fetchStats(m.broker), tick())
	case statsMsg:
		m.stats = msg.stats
		m.err = msg.err
		return m, nil
	case logMsg:
		if !msg.ok {
			return m, nil
		}
		m.logs = append(m.logs, msg.line)
		if len(m.logs) > logBufferMax {
			m.logs = m.logs[len(m.logs)-logBufferMax:]
		}
		m.viewport.SetContent(strings.Join(m.logs, "\n"))
		m.viewport.GotoBottom()
		return m, waitForLog(m.logCh)
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "l":
			m.rotateLogLevel()
		case "d":
			m.rotateDelayLevel()
		case " ":
			m.togglePause()
		case "up", "k":
			m.viewport.LineUp(1)
		case "down", "j":
			m.viewport.LineDown(1)
		case "pgup":
			m.viewport.HalfViewUp()
		case "pgdown":
			m.viewport.HalfViewDown()
		}
	}
	return m, nil
}

func (m *model) resize() {
	if m.width == 0 || m.height == 0 {
		return
	}
	m.ready = true

	outerPadding := 1
	usableWidth := m.width - outerPadding*2 - 2
	if usableWidth < 40 {
		outerPadding = 1
		usableWidth = m.width - outerPadding*2 - 2
	}
	m.padding = outerPadding

	headerHeight := 6
	helpHeight := 1
	verticalPadding := outerPadding * 2
	availableHeight := m.height - headerHeight - helpHeight - verticalPadding
	if availableHeight < 6 {
		availableHeight = 6
	}

	topHeight := int(math.Round(float64(availableHeight) * 0.45))
	if topHeight < 8 {
		topHeight = 8
	}
	logHeight := availableHeight - topHeight
	if logHeight < 6 {
		logHeight = 6
	}

	statsWidth := int(math.Round(float64(m.width) * 0.35))
	if statsWidth < 28 {
		statsWidth = 28
	}
	if statsWidth > usableWidth-30 {
		statsWidth = usableWidth - 30
	}
	topicsWidth := usableWidth - statsWidth - 3
	if topicsWidth < 30 {
		topicsWidth = 30
		statsWidth = usableWidth - topicsWidth - 3
	}

	m.viewport = viewport.New(usableWidth-2, logHeight)
	m.viewport.SetContent(strings.Join(m.logs, "\n"))
	m.viewport.GotoBottom()
	m.layout = layout{
		topHeight:    topHeight,
		logHeight:    logHeight,
		statsWidth:   statsWidth,
		topicsWidth:  topicsWidth,
		totalWidth:   usableWidth + 2,
		headerHeight: headerHeight,
	}
}

func (m model) View() string {
	if !m.ready {
		return "loading..."
	}

	styles := newStyles()

	headerLine := styles.headerLine.Render(strings.Repeat("─", m.layout.totalWidth))
	headerText := lipgloss.Place(
		m.layout.totalWidth,
		1,
		lipgloss.Center,
		lipgloss.Center,
		styles.headerText.Render("Minipulsar"),
	)
	header := lipgloss.JoinVertical(lipgloss.Left, headerLine, headerText, headerLine)
	stats := styles.box.Width(m.layout.statsWidth).Height(m.layout.topHeight).Render(renderOverview(m.stats, m.err))
	topics := styles.box.Width(m.layout.topicsWidth).Height(m.layout.topHeight).Render(renderTopTopics(m.stats, m.layout.topicsWidth-2))
	row := lipgloss.JoinHorizontal(lipgloss.Top, stats, " ", topics)
	logs := styles.box.Width(m.layout.totalWidth - 2).Height(m.layout.logHeight).Render(m.viewport.View())
	help := styles.help.Render(fmt.Sprintf(
		"q: quit • l: log level (%s) • d: delay (%ds) • space: pause (%s) • ↑/↓/pgup/pgdown scroll logs",
		logLevelLabel(m.logLevel),
		m.delayLevel,
		pauseLabel(m.paused),
	))

	content := lipgloss.JoinVertical(lipgloss.Left, header, row, logs, help)
	return lipgloss.NewStyle().Padding(m.padding, m.padding).Render(content)
}

type layout struct {
	topHeight    int
	logHeight    int
	statsWidth   int
	topicsWidth  int
	totalWidth   int
	headerHeight int
}

type styleSet struct {
	headerBox  lipgloss.Style
	headerLine lipgloss.Style
	headerText lipgloss.Style
	box        lipgloss.Style
	help       lipgloss.Style
	accent     lipgloss.Style
	label      lipgloss.Style
	value      lipgloss.Style
	bar        lipgloss.Style
}

func newStyles() styleSet {
	bg := lipgloss.Color("#1B1035")
	pink := lipgloss.Color("#F72585")
	purple := lipgloss.Color("#7209B7")
	cyan := lipgloss.Color("#4CC9F0")
	text := lipgloss.Color("#F8F7FF")

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(purple).
		Foreground(text).
		Background(bg).
		Padding(0, 1)

	return styleSet{
		headerBox: lipgloss.NewStyle().
			Background(bg),
		headerLine: lipgloss.NewStyle().
			Foreground(pink),
		headerText: lipgloss.NewStyle().
			Foreground(text).
			Background(bg).
			Bold(true).
			Align(lipgloss.Center),
		box:    box,
		help:   lipgloss.NewStyle().Foreground(cyan).Padding(0, 1),
		accent: lipgloss.NewStyle().Foreground(pink).Bold(true),
		label:  lipgloss.NewStyle().Foreground(cyan),
		value:  lipgloss.NewStyle().Foreground(text).Bold(true),
		bar:    lipgloss.NewStyle().Foreground(pink),
	}
}

func renderOverview(stats broker.StatsSnapshot, err error) string {
	styles := newStyles()
	lines := []string{
		styles.accent.Render("Overview"),
		"",
		fmt.Sprintf("%s %s", styles.label.Render("Producers"), styles.value.Render(fmt.Sprintf("%d", stats.Producers))),
		fmt.Sprintf("%s %s", styles.label.Render("Consumers"), styles.value.Render(fmt.Sprintf("%d", stats.Consumers))),
		fmt.Sprintf("%s %s", styles.label.Render("Topics"), styles.value.Render(fmt.Sprintf("%d", stats.Topics))),
		fmt.Sprintf("%s %s", styles.label.Render("Subscriptions"), styles.value.Render(fmt.Sprintf("%d", stats.Subscriptions))),
		fmt.Sprintf("%s %s", styles.label.Render("Pending"), styles.value.Render(fmt.Sprintf("%d", stats.Pending))),
		fmt.Sprintf("%s %s", styles.label.Render("Alloc Mem"), styles.value.Render(formatBytes(stats.MemoryAlloc))),
		fmt.Sprintf("%s %s", styles.label.Render("Msgs/sec"), styles.value.Render(fmt.Sprintf("%.1f", stats.ThroughputPS))),
	}
	if err != nil {
		lines = append(lines, "", styles.label.Render("Stats error: ")+err.Error())
	}
	return strings.Join(lines, "\n")
}

func renderTopTopics(stats broker.StatsSnapshot, width int) string {
	styles := newStyles()
	lines := []string{styles.accent.Render("Top Topics"), ""}

	if len(stats.TopTopics) == 0 {
		lines = append(lines, styles.label.Render("No topic activity yet."))
		return strings.Join(lines, "\n")
	}

	metricWidth := 6
	topicWidth := width - metricWidth*2 - 2
	if topicWidth < 10 {
		topicWidth = 10
	}

	head := fmt.Sprintf("%-*s %*s %*s", topicWidth, "Topic", metricWidth, "Msgs", metricWidth, "Pend")
	lines = append(lines, styles.label.Render(head))

	for _, topic := range stats.TopTopics {
		lines = append(lines, fmt.Sprintf(
			"%-*s %*d %*d",
			topicWidth,
			truncate(topic.Topic, topicWidth),
			metricWidth,
			topic.MessageCount,
			metricWidth,
			topic.PendingCount,
		))
	}

	return strings.Join(lines, "\n")
}

func truncate(s string, width int) string {
	if width <= 0 || len(s) <= width {
		return s
	}
	if width <= 1 {
		return s[:width]
	}
	return s[:width-1] + "…"
}

func formatBytes(value uint64) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	div, exp := uint64(unit), 0
	for n := value / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(value)/float64(div), "KMGTPE"[exp])
}

func (m *model) rotateLogLevel() {
	if m.level == nil {
		return
	}
	levels := []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}
	current := normalizeLevel(m.logLevel)
	next := levels[0]
	for i, lvl := range levels {
		if current == lvl {
			next = levels[(i+1)%len(levels)]
			break
		}
	}
	m.logLevel = next
	m.level.Set(next)
}

func normalizeLevel(level slog.Level) slog.Level {
	if level <= slog.LevelDebug {
		return slog.LevelDebug
	}
	if level >= slog.LevelError {
		return slog.LevelError
	}
	switch level {
	case slog.LevelInfo:
		return slog.LevelInfo
	case slog.LevelWarn:
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}

func logLevelLabel(level slog.Level) string {
	switch normalizeLevel(level) {
	case slog.LevelDebug:
		return "DEBUG"
	case slog.LevelInfo:
		return "INFO"
	case slog.LevelWarn:
		return "WARN"
	case slog.LevelError:
		return "ERROR"
	default:
		return "INFO"
	}
}

func (m *model) rotateDelayLevel() {
	next := m.delayLevel + 1
	if next > broker.MaxThrottleLevel {
		next = 0
	}
	m.delayLevel = m.broker.SetThrottleLevel(next)
}

func (m *model) togglePause() {
	m.paused = !m.paused
	m.broker.SetThrottlePaused(m.paused)
}

func pauseLabel(paused bool) string {
	if paused {
		return "on"
	}
	return "off"
}
