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
	defaultTopLimit  = 10
	logBufferMax     = 1000
	historyMax       = 30
	canvasBackground = "\x1b[48;2;27;16;53m"
	ansiReset        = "\x1b[0m"
)

type view int

const (
	overviewView view = iota
	topicsView
	backlogView
	logsView
)

var viewNames = [...]string{"Overview", "Topics", "Backlog", "Logs"}

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
	followLogs bool
	startedAt  time.Time
	updatedAt  time.Time
	activeView view
	selected   int
	throughput []float64
	pending    []int
}

// NewProgram builds a Bubble Tea program that renders broker stats and logs.
func NewProgram(b *broker.Broker, logCh <-chan string, levelVar *slog.LevelVar, level slog.Level) *tea.Program {
	m := model{
		broker:     b,
		logCh:      logCh,
		level:      levelVar,
		logLevel:   level,
		paused:     b.ThrottlePaused(),
		followLogs: true,
		startedAt:  time.Now(),
	}
	m.delayLevel = b.ThrottleLevel()
	return tea.NewProgram(m, tea.WithAltScreen())
}

// Init kicks off periodic ticks and log streaming for the TUI.
func (m model) Init() tea.Cmd {
	return tea.Batch(fetchStats(m.broker), tick(), waitForLog(m.logCh))
}

// tick emits a periodic message used to refresh stats.
func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// waitForLog blocks until the next log line arrives from the broker logger.
func waitForLog(logCh <-chan string) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-logCh
		return logMsg{line: line, ok: ok}
	}
}

// fetchStats pulls a snapshot from the broker for the dashboard panels.
func fetchStats(b *broker.Broker) tea.Cmd {
	return func() tea.Msg {
		stats, err := b.StatsSnapshot(defaultTopLimit)
		return statsMsg{stats: stats, err: err}
	}
}

// Update handles user input, timer ticks, and log messages.
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
		m.updatedAt = time.Now()
		if msg.err == nil {
			m.throughput = appendHistory(m.throughput, msg.stats.ThroughputPS, historyMax)
			m.pending = appendHistory(m.pending, msg.stats.Pending, historyMax)
			m.clampSelection()
		}
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
		if m.followLogs {
			m.viewport.GotoBottom()
		}
		return m, waitForLog(m.logCh)
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab", "right":
			m.activeView = nextView(m.activeView, 1)
			m.selected = 0
			m.clampSelection()
		case "shift+tab", "left":
			m.activeView = nextView(m.activeView, -1)
			m.selected = 0
			m.clampSelection()
		case "1":
			m.activeView = overviewView
		case "2":
			m.activeView = topicsView
			m.selected = 0
			m.clampSelection()
		case "3":
			m.activeView = backlogView
			m.selected = 0
			m.clampSelection()
		case "4":
			m.activeView = logsView
		case "r":
			return m, fetchStats(m.broker)
		case "l":
			m.rotateLogLevel()
		case "d":
			m.rotateDelayLevel()
		case " ":
			m.togglePause()
		case "f":
			m.followLogs = !m.followLogs
			if m.followLogs {
				m.viewport.GotoBottom()
			}
		case "c":
			m.logs = nil
			m.viewport.SetContent("")
		case "up", "k":
			m.moveSelection(-1)
		case "down", "j":
			m.moveSelection(1)
		case "pgup":
			if m.activeView == logsView {
				m.viewport.HalfViewUp()
			}
		case "pgdown":
			if m.activeView == logsView {
				m.viewport.HalfViewDown()
			}
		}
	}
	return m, nil
}

// resize recalculates layout metrics whenever the terminal size changes.
func (m *model) resize() {
	if m.width == 0 || m.height == 0 {
		return
	}
	m.ready = true

	const outerX = 1
	const outerY = 1
	contentWidth := max(1, m.width-outerX*2)
	contentHeight := max(1, m.height-outerY*2)
	// Header, navigation, and footer each consume one terminal row.
	panelHeight := max(3, contentHeight-3)
	panelInnerHeight := max(1, panelHeight-2) // Rounded border consumes two rows.

	m.padding = outerX
	m.viewport = viewport.New(max(1, contentWidth-4), max(1, panelInnerHeight-3))
	m.viewport.SetContent(strings.Join(m.logs, "\n"))
	if m.followLogs {
		m.viewport.GotoBottom()
	}
	m.layout = layout{
		contentWidth: contentWidth,
		panelHeight:  panelHeight,
	}
}

// View renders the current TUI frame.
func (m model) View() string {
	if !m.ready {
		return "loading..."
	}

	styles := newStyles()

	status := "LIVE"
	if m.paused {
		status = "PAUSED"
	}
	header := renderHeader(status, m.stats.ThroughputPS, m.layout.contentWidth, styles)
	navigation := renderNavigation(m.activeView, m.layout.contentWidth, styles)
	panel := styles.box.Width(max(1, m.layout.contentWidth-2)).Height(max(1, m.layout.panelHeight-2)).Render(
		m.renderActiveView(max(1, m.layout.contentWidth-4), max(1, m.layout.panelHeight-4)),
	)
	help := lipgloss.Place(m.layout.contentWidth, 1, lipgloss.Center, lipgloss.Center, styles.help.Render(fmt.Sprintf(
		"1-4/tab view  j/k navigate  r refresh  d:%ds  space:%s  l:%s  f:%s  c clear  q quit",
		m.delayLevel,
		pauseLabel(m.paused),
		logLevelLabel(m.logLevel),
		pauseLabel(m.followLogs),
	)))

	content := lipgloss.JoinVertical(lipgloss.Left, header, navigation, panel, help)
	canvas := styles.screen.Width(m.width).Height(m.height).Render(
		lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, content),
	)
	// Lipgloss resets styles after nested labels. Reapply the canvas background
	// after every reset so blank space beside labels never falls back to black.
	return canvasBackground + strings.ReplaceAll(canvas, ansiReset, ansiReset+canvasBackground) + ansiReset
}

type layout struct {
	contentWidth int
	panelHeight  int
}

type styleSet struct {
	headerBox  lipgloss.Style
	headerLine lipgloss.Style
	headerText lipgloss.Style
	screen     lipgloss.Style
	header     lipgloss.Style
	box        lipgloss.Style
	help       lipgloss.Style
	accent     lipgloss.Style
	label      lipgloss.Style
	value      lipgloss.Style
	bar        lipgloss.Style
	warning    lipgloss.Style
	success    lipgloss.Style
	selected   lipgloss.Style
	muted      lipgloss.Style
}

// newStyles defines the synthwave color palette and component styles.
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
		screen: lipgloss.NewStyle().
			Background(bg),
		header: lipgloss.NewStyle().
			Background(bg).
			Foreground(text),
		headerLine: lipgloss.NewStyle().
			Foreground(pink),
		headerText: lipgloss.NewStyle().
			Foreground(text).
			Background(bg).
			Bold(true).
			Align(lipgloss.Center),
		box:      box,
		help:     lipgloss.NewStyle().Foreground(cyan).Background(bg),
		accent:   lipgloss.NewStyle().Foreground(pink).Bold(true),
		label:    lipgloss.NewStyle().Foreground(cyan),
		value:    lipgloss.NewStyle().Foreground(text).Bold(true),
		bar:      lipgloss.NewStyle().Foreground(pink),
		warning:  lipgloss.NewStyle().Foreground(lipgloss.Color("#FFB703")).Bold(true),
		success:  lipgloss.NewStyle().Foreground(lipgloss.Color("#80ED99")).Bold(true),
		selected: lipgloss.NewStyle().Foreground(bg).Background(cyan).Bold(true),
		muted:    lipgloss.NewStyle().Foreground(lipgloss.Color("#A89CC8")),
	}
}

// renderActiveView renders the selected operational panel from the latest snapshot.
func (m model) renderActiveView(width, height int) string {
	switch m.activeView {
	case topicsView:
		return renderTopicsView(m.stats, m.selected, width, height)
	case backlogView:
		return renderBacklogView(m.stats, m.selected, width, height)
	case logsView:
		return renderLogsView(m.viewport, m.followLogs, len(m.logs), width)
	default:
		return renderDashboard(m.stats, m.err, time.Since(m.startedAt), m.throughput, m.pending, width)
	}
}

func renderNavigation(active view, width int, styles styleSet) string {
	items := make([]string, len(viewNames))
	for i, name := range viewNames {
		if width < 54 {
			name = string(name[0])
		}
		label := fmt.Sprintf(" %d %s ", i+1, name)
		if view(i) == active {
			items[i] = styles.selected.Render(label)
		} else {
			items[i] = styles.muted.Render(label)
		}
	}
	return lipgloss.NewStyle().Background(lipgloss.Color("#1B1035")).Render(
		lipgloss.Place(width, 1, lipgloss.Center, lipgloss.Center, strings.Join(items, " ")),
	)
}

func renderHeader(status string, throughput float64, width int, styles styleSet) string {
	brand := styles.accent.Render("MINIPULSAR")
	stateStyle := styles.success
	if status == "PAUSED" {
		stateStyle = styles.warning
	}
	state := stateStyle.Render(" " + status + " ")
	if width < 48 {
		return styles.header.Width(width).Render(lipgloss.JoinHorizontal(lipgloss.Left, brand, " ", state))
	}
	rate := styles.muted.Render(fmt.Sprintf("%.1f msg/s", throughput))
	return styles.header.Width(width).Render(lipgloss.JoinHorizontal(lipgloss.Left, brand, "  ", state, "  ", rate))
}

func renderDashboard(stats broker.StatsSnapshot, err error, uptime time.Duration, throughput []float64, pending []int, width int) string {
	styles := newStyles()
	leftWidth := max(28, width/2-1)
	rightWidth := max(24, width-leftWidth-2)
	if leftWidth+rightWidth+2 > width {
		leftWidth = max(1, width/2-1)
		rightWidth = max(1, width-leftWidth-2)
	}
	health := "CLEAR"
	if stats.Pending > 0 {
		health = "BACKLOG"
	}
	if err != nil {
		health = "DEGRADED"
	}
	pressure := percent(stats.Pending, stats.Messages)
	left := []string{
		styles.accent.Render("Broker Overview"),
		"",
		fmt.Sprintf("%s %s", styles.label.Render("Health"), styles.value.Render(health)),
		fmt.Sprintf("%s %s", styles.label.Render("Uptime"), styles.value.Render(formatDuration(uptime))),
		fmt.Sprintf("%s %s", styles.label.Render("Connections"), styles.value.Render(fmt.Sprintf("%d producers / %d consumers", stats.Producers, stats.Consumers))),
		fmt.Sprintf("%s %s", styles.label.Render("Topology"), styles.value.Render(fmt.Sprintf("%d namespaces / %d topics", stats.Namespaces, stats.Topics))),
		fmt.Sprintf("%s %s", styles.label.Render("Stored"), styles.value.Render(fmt.Sprintf("%d messages / %d subscriptions", stats.Messages, stats.Subscriptions))),
		fmt.Sprintf("%s %s", styles.label.Render("Pending"), styles.warning.Render(fmt.Sprintf("%d (%.1f%% of stored)", stats.Pending, pressure))),
		fmt.Sprintf("%s %s", styles.label.Render("Memory"), styles.value.Render(formatBytes(stats.MemoryAlloc))),
	}
	if err != nil {
		left = append(left, "", styles.warning.Render("Stats error: "+err.Error()))
	}
	right := []string{
		styles.accent.Render("Live Signals"),
		"",
		styles.label.Render("Throughput (messages/sec)"),
		styles.bar.Render(sparklineFloat(throughput, rightWidth-2)),
		styles.value.Render(fmt.Sprintf("%.1f msg/s", stats.ThroughputPS)),
		"",
		styles.label.Render("Pending message trend"),
		styles.bar.Render(sparklineInt(pending, rightWidth-2)),
		styles.value.Render(fmt.Sprintf("%d pending", stats.Pending)),
		"",
		styles.label.Render("Hottest topic"),
	}
	if len(stats.TopTopics) == 0 {
		right = append(right, styles.muted.Render("No topic activity yet."))
	} else {
		topic := stats.TopTopics[0]
		right = append(right, styles.value.Render(truncate(topic.Topic, rightWidth-2)))
		right = append(right, styles.warning.Render(fmt.Sprintf("%d pending / %d stored", topic.PendingCount, topic.MessageCount)))
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, lipgloss.NewStyle().Width(leftWidth).Render(strings.Join(left, "\n")), "  ", lipgloss.NewStyle().Width(rightWidth).Render(strings.Join(right, "\n")))
}

func renderTopicsView(stats broker.StatsSnapshot, selected, width, height int) string {
	styles := newStyles()
	lines := []string{styles.accent.Render("Topic Pressure"), styles.muted.Render("j/k selects a topic. Pending is the active delivery queue across subscriptions."), ""}
	if len(stats.TopTopics) == 0 {
		return strings.Join(append(lines, styles.label.Render("No topics have been recorded.")), "\n")
	}
	topicWidth := max(12, width-31)
	lines = append(lines, styles.label.Render(fmt.Sprintf("  %-*s %8s %8s  %s", topicWidth, "Topic", "Stored", "Pending", "Pressure")))
	for i, topic := range stats.TopTopics {
		prefix := " "
		if i == selected {
			prefix = ">"
		}
		line := fmt.Sprintf("%s %-*s %8d %8d  %s", prefix, topicWidth, truncate(topic.Topic, topicWidth), topic.MessageCount, topic.PendingCount, backlogBar(topic.PendingCount, topic.MessageCount, 10))
		if i == selected {
			line = styles.selected.Render(line)
		}
		lines = append(lines, line)
	}
	selectedTopic := stats.TopTopics[min(selected, len(stats.TopTopics)-1)]
	lines = append(lines, "", styles.label.Render("Selected"), styles.value.Render(fmt.Sprintf("%s  |  %d stored  |  %d pending  |  %.1f%% pressure", selectedTopic.Topic, selectedTopic.MessageCount, selectedTopic.PendingCount, percent(selectedTopic.PendingCount, selectedTopic.MessageCount))))
	return strings.Join(lines, "\n")
}

func renderBacklogView(stats broker.StatsSnapshot, selected, width, height int) string {
	styles := newStyles()
	lines := []string{styles.accent.Render("Subscription Backlog"), styles.muted.Render("Backlog is available for namespaces with configured retention policies."), ""}
	if len(stats.TopSubscriptionsBacklog) == 0 {
		return strings.Join(append(lines, styles.label.Render("No subscription backlog data is available.")), "\n")
	}
	topicWidth := max(12, width-31)
	lines = append(lines, styles.label.Render(fmt.Sprintf("  %-*s %-12s %10s", topicWidth, "Topic", "Subscription", "Backlog")))
	for i, sub := range stats.TopSubscriptionsBacklog {
		prefix := " "
		if i == selected {
			prefix = ">"
		}
		line := fmt.Sprintf("%s %-*s %-12s %10d", prefix, topicWidth, truncate(sub.Topic, topicWidth), truncate(sub.Subscription, 12), sub.BacklogCount)
		if i == selected {
			line = styles.selected.Render(line)
		}
		lines = append(lines, line)
	}
	sub := stats.TopSubscriptionsBacklog[min(selected, len(stats.TopSubscriptionsBacklog)-1)]
	lines = append(lines, "", styles.label.Render("Selected"), styles.value.Render(fmt.Sprintf("%s / %s has %d messages awaiting delivery", sub.Topic, sub.Subscription, sub.BacklogCount)))
	return strings.Join(lines, "\n")
}

func renderLogsView(logs viewport.Model, following bool, count, width int) string {
	styles := newStyles()
	state := "paused"
	if following {
		state = "following"
	}
	return strings.Join([]string{styles.accent.Render("Broker Logs"), styles.muted.Render(fmt.Sprintf("%d buffered lines, %s. j/k and pgup/pgdown scroll.", count, state)), "", logs.View()}, "\n")
}

func nextView(current view, direction int) view {
	count := len(viewNames)
	return view((int(current) + direction + count) % count)
}

func appendHistory[T any](values []T, value T, limit int) []T {
	values = append(values, value)
	if len(values) > limit {
		return values[len(values)-limit:]
	}
	return values
}

func (m *model) moveSelection(direction int) {
	if m.activeView == logsView {
		if direction < 0 {
			m.viewport.LineUp(1)
		} else {
			m.viewport.LineDown(1)
		}
		return
	}
	if m.activeView != topicsView && m.activeView != backlogView {
		return
	}
	m.selected += direction
	m.clampSelection()
}

func (m *model) clampSelection() {
	count := 0
	switch m.activeView {
	case topicsView:
		count = len(m.stats.TopTopics)
	case backlogView:
		count = len(m.stats.TopSubscriptionsBacklog)
	}
	if count == 0 {
		m.selected = 0
		return
	}
	if m.selected < 0 {
		m.selected = 0
	}
	if m.selected >= count {
		m.selected = count - 1
	}
}

func percent(numerator, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) * 100 / float64(denominator)
}

func sparklineFloat(values []float64, width int) string {
	if len(values) == 0 || width <= 0 {
		return "-"
	}
	values = compactFloat(values, width)
	maxValue := 0.0
	for _, value := range values {
		if value > maxValue {
			maxValue = value
		}
	}
	if maxValue == 0 {
		return strings.Repeat("_", len(values))
	}
	levels := []rune("▁▂▃▄▅▆▇█")
	var out strings.Builder
	for _, value := range values {
		index := int(math.Round(value / maxValue * float64(len(levels)-1)))
		out.WriteRune(levels[index])
	}
	return out.String()
}

func sparklineInt(values []int, width int) string {
	floatValues := make([]float64, len(values))
	for i, value := range values {
		floatValues[i] = float64(value)
	}
	return sparklineFloat(floatValues, width)
}

func compactFloat(values []float64, width int) []float64 {
	if len(values) <= width {
		return values
	}
	result := make([]float64, width)
	for i := range result {
		result[i] = values[i*len(values)/width]
	}
	return result
}

func backlogBar(pending, messages, width int) string {
	if width <= 0 {
		return ""
	}
	ratio := 0.0
	if messages > 0 {
		ratio = float64(pending) / float64(messages)
	}
	filled := int(math.Round(ratio * float64(width)))
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
}

// truncate shortens strings to fit in fixed-width columns.
func truncate(s string, width int) string {
	if width <= 0 || len(s) <= width {
		return s
	}
	if width <= 1 {
		return s[:width]
	}
	return s[:width-1] + "…"
}

// formatBytes formats byte counts with binary suffixes.
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

// rotateLogLevel cycles the UI log level and updates the shared slog level.
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

// normalizeLevel clamps arbitrary levels to the nearest supported label.
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

// logLevelLabel formats the log level for the help footer.
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

// rotateDelayLevel cycles broker throttling delay from the UI.
func (m *model) rotateDelayLevel() {
	next := m.delayLevel + 1
	if next > broker.MaxThrottleLevel {
		next = 0
	}
	m.delayLevel = m.broker.SetThrottleLevel(next)
}

// togglePause toggles the broker-wide pause flag from the UI.
func (m *model) togglePause() {
	m.paused = !m.paused
	m.broker.SetThrottlePaused(m.paused)
}

// pauseLabel formats the pause state for the footer.
func pauseLabel(paused bool) string {
	if paused {
		return "on"
	}
	return "off"
}
