package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
	"minipulsar/internal/broker"
	"minipulsar/internal/storage"
)

func TestViewNavigationWraps(t *testing.T) {
	if got := nextView(logsView, 1); got != overviewView {
		t.Fatalf("nextView(logs, 1) = %v, want overview", got)
	}
	if got := nextView(overviewView, -1); got != logsView {
		t.Fatalf("nextView(overview, -1) = %v, want logs", got)
	}
}

func TestViewFillsTerminalDimensionsAfterPanelSwitch(t *testing.T) {
	m := model{
		width:      100,
		height:     32,
		startedAt:  time.Now(),
		followLogs: true,
		stats: broker.StatsSnapshot{TopTopics: []storage.TopicStat{{
			Topic: "persistent://public/default/orders", MessageCount: 10,
		}}, TopSubscriptionsBacklog: []storage.SubscriptionBacklogStat{{
			Topic: "persistent://public/default/orders", Subscription: "billing",
		}}},
	}
	m.resize()
	for _, active := range []view{overviewView, topicsView, backlogView, logsView} {
		m.activeView = active
		output := m.View()
		if got := lipgloss.Width(output); got != m.width {
			t.Fatalf("view %v width = %d, want %d", active, got, m.width)
		}
		if got := lipgloss.Height(output); got != m.height {
			t.Fatalf("view %v height = %d, want %d", active, got, m.height)
		}
	}
}

func TestSelectionIsClampedToActivePanel(t *testing.T) {
	m := model{
		activeView: topicsView,
		selected:   4,
		stats: broker.StatsSnapshot{TopTopics: []storage.TopicStat{
			{Topic: "persistent://public/default/orders"},
			{Topic: "persistent://public/default/events"},
		}},
	}
	m.clampSelection()
	if m.selected != 1 {
		t.Fatalf("selected = %d, want 1", m.selected)
	}
	m.moveSelection(-10)
	if m.selected != 0 {
		t.Fatalf("selected = %d, want 0", m.selected)
	}
}

func TestOperationalViewsRenderSnapshotData(t *testing.T) {
	stats := broker.StatsSnapshot{
		Messages: 10,
		Pending:  4,
		TopTopics: []storage.TopicStat{{
			Topic:        "persistent://public/default/orders",
			MessageCount: 10,
			PendingCount: 4,
		}},
		TopSubscriptionsBacklog: []storage.SubscriptionBacklogStat{{
			Topic:        "persistent://public/default/orders",
			Subscription: "billing",
			BacklogCount: 3,
		}},
	}

	for _, test := range []struct {
		name string
		view view
		want string
	}{
		{name: "overview", view: overviewView, want: "40.0%"},
		{name: "topics", view: topicsView, want: "orders"},
		{name: "backlog", view: backlogView, want: "billing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := model{activeView: test.view, stats: stats, throughput: []float64{1, 4}, pending: []int{1, 4}}
			if got := m.renderActiveView(100, 20); !strings.Contains(got, test.want) {
				t.Fatalf("view does not contain %q:\n%s", test.want, got)
			}
		})
	}
}

func TestAppendHistoryKeepsMostRecentValues(t *testing.T) {
	got := appendHistory([]int{1, 2, 3}, 4, 3)
	want := []int{2, 3, 4}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("history = %v, want %v", got, want)
		}
	}
}
