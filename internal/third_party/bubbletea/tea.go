package tea

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Msg represents a message passed to the update loop.
// It mirrors bubbletea's Msg type and keeps the API compatible.
type Msg interface{}

// Cmd is an async command that returns a message.
type Cmd func() Msg

// Model is the core TUI interface.
type Model interface {
	Init() Cmd
	Update(Msg) (Model, Cmd)
	View() string
}

// QuitMsg signals the program to stop.
type QuitMsg struct{}

// Quit returns a command that quits the program.
func Quit() Msg {
	return QuitMsg{}
}

// Program drives a model with a simple render loop.
// It is a lightweight, local implementation of bubbletea's Program API.
type Program struct {
	model   Model
	input   io.Reader
	output  io.Writer
	mu      sync.Mutex
	stopped bool
}

// NewProgram creates a new program for the given model.
func NewProgram(model Model) *Program {
	return &Program{model: model, input: os.Stdin, output: os.Stdout}
}

// Start runs the program until Quit is received.
func (p *Program) Start() error {
	if cmd := p.model.Init(); cmd != nil {
		p.dispatch(cmd)
	}

	msgCh := make(chan Msg, 64)

	go p.readInput(msgCh)
	go p.handleSignals(msgCh)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case msg := <-msgCh:
			if _, ok := msg.(QuitMsg); ok {
				return nil
			}
			p.model, _ = p.model.Update(msg)
			p.render()
		case <-ticker.C:
			p.model, _ = p.model.Update(TickMsg{})
			p.render()
		}
	}
}

func (p *Program) dispatch(cmd Cmd) {
	if cmd == nil {
		return
	}
	go func() {
		msg := cmd()
		if msg == nil {
			return
		}
		p.model, _ = p.model.Update(msg)
		p.render()
	}()
}

func (p *Program) render() {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, _ = fmt.Fprint(p.output, "\x1b[H\x1b[2J")
	_, _ = fmt.Fprint(p.output, p.model.View())
}

func (p *Program) readInput(msgCh chan<- Msg) {
	reader := bufio.NewReader(p.input)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return
		}
		msgCh <- KeyMsg{Rune: rune(b)}
	}
}

func (p *Program) handleSignals(msgCh chan<- Msg) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	for range sigCh {
		msgCh <- QuitMsg{}
		return
	}
}

// TickMsg is emitted on a fixed interval to update the UI.
type TickMsg struct{}

// KeyMsg represents a single byte input.
type KeyMsg struct {
	Rune rune
}

// Key returns the rune for downstream handlers.
func (k KeyMsg) Key() rune {
	return k.Rune
}
