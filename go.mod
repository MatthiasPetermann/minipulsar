module minipulsar

go 1.21

require (
	github.com/charmbracelet/bubbles v0.16.1
	github.com/charmbracelet/bubbletea v0.25.0
	github.com/charmbracelet/lipgloss v0.9.1
	github.com/sirupsen/logrus v0.0.0
	github.com/mattn/go-sqlite3 v1.14.22
	google.golang.org/protobuf v1.33.0
)

replace github.com/sirupsen/logrus => ./internal/third_party/logrus
