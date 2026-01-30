module minipulsar

go 1.21

require (
	github.com/charmbracelet/bubbletea v0.0.0
	github.com/sirupsen/logrus v0.0.0
	github.com/mattn/go-sqlite3 v1.14.22
	google.golang.org/protobuf v1.33.0
)

replace github.com/sirupsen/logrus => ./internal/third_party/logrus
replace github.com/charmbracelet/bubbletea => ./internal/third_party/bubbletea
