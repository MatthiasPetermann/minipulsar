package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	lua "github.com/yuin/gopher-lua"

	"minipulsar/internal/logging"
)

// FunctionContext provides metadata for Lua functions.
type FunctionContext struct {
	FunctionID  string
	SourceTopic string
	TargetTopic string
}

type functionTask struct {
	functionID string
	payload    []byte
	ctx        FunctionContext
	resultCh   chan functionResult
}

type functionResult struct {
	payload []byte
	err     error
}

// FunctionPool executes Lua functions using a bounded worker pool.
type FunctionPool struct {
	logger *logging.Logger
	tasks  chan functionTask
}

// NewFunctionPool creates a pool with the requested number of Lua workers.
func NewFunctionPool(registry *FunctionRegistry, workers int, logger *logging.Logger) (*FunctionPool, error) {
	if registry == nil {
		return nil, fmt.Errorf("function registry is required")
	}
	if workers <= 0 {
		return nil, fmt.Errorf("worker count must be positive")
	}
	if logger == nil {
		defaultLogger, err := logging.New(logging.Options{
			Format:        "text",
			WithTimestamp: true,
			Level:         slog.LevelInfo,
			Writer:        os.Stdout,
		})
		if err == nil {
			logger = defaultLogger.With("component", "lua-pool")
		}
	}
	pool := &FunctionPool{
		logger: logger,
		tasks:  make(chan functionTask),
	}

	for i := 0; i < workers; i++ {
		worker, err := newLuaWorker(registry)
		if err != nil {
			return nil, err
		}
		go worker.loop(pool.tasks, pool.logger.With("worker", i))
	}
	return pool, nil
}

// Execute runs a Lua function and returns the transformed payload.
func (p *FunctionPool) Execute(functionID string, payload []byte, ctx FunctionContext) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("function pool is nil")
	}
	resultCh := make(chan functionResult, 1)
	p.tasks <- functionTask{
		functionID: functionID,
		payload:    payload,
		ctx:        ctx,
		resultCh:   resultCh,
	}
	result := <-resultCh
	return result.payload, result.err
}

type luaWorker struct {
	state     *lua.LState
	functions map[string]luaFunction
}

type luaFunction struct {
	fn         *lua.LFunction
	maxRuntime time.Duration
}

// newLuaWorker loads Lua scripts once per worker to avoid per-call setup cost.
func newLuaWorker(registry *FunctionRegistry) (*luaWorker, error) {
	state := lua.NewState()
	openSafeLibs(state)

	functions := make(map[string]luaFunction, len(registry.Functions))
	for id, fn := range registry.Functions {
		if err := state.DoFile(fn.Path); err != nil {
			state.Close()
			return nil, fmt.Errorf("load function %q: %w", id, err)
		}
		value := state.GetGlobal("handle")
		lfn, ok := value.(*lua.LFunction)
		if !ok {
			state.Close()
			return nil, fmt.Errorf("function %q missing handle entrypoint", id)
		}
		functions[id] = luaFunction{fn: lfn, maxRuntime: fn.MaxRuntime}
		state.SetGlobal("handle", lua.LNil)
	}

	return &luaWorker{state: state, functions: functions}, nil
}

// loop processes function tasks sequentially on a single Lua state.
func (w *luaWorker) loop(tasks <-chan functionTask, logger *logging.Logger) {
	for task := range tasks {
		payload, err := w.execute(task.functionID, task.payload, task.ctx)
		if err != nil {
			logger.Warn("lua execution failed", "err", err, "function_id", task.functionID)
		}
		task.resultCh <- functionResult{payload: payload, err: err}
	}
}

// execute invokes the Lua handle function with payload and context metadata.
func (w *luaWorker) execute(functionID string, payload []byte, ctx FunctionContext) ([]byte, error) {
	fn := w.functions[functionID]
	if fn.fn == nil {
		return nil, fmt.Errorf("unknown function %q", functionID)
	}

	ctxTable := w.state.NewTable()
	ctxTable.RawSetString("function_id", lua.LString(ctx.FunctionID))
	ctxTable.RawSetString("source_topic", lua.LString(ctx.SourceTopic))
	ctxTable.RawSetString("target_topic", lua.LString(ctx.TargetTopic))

	if fn.maxRuntime > 0 {
		runCtx, cancel := context.WithTimeout(context.Background(), fn.maxRuntime)
		w.state.SetContext(runCtx)
		defer func() {
			cancel()
			w.state.RemoveContext()
		}()
	}

	if err := w.state.CallByParam(lua.P{
		Fn:      fn.fn,
		NRet:    1,
		Protect: true,
	}, lua.LString(string(payload)), ctxTable); err != nil {
		return nil, err
	}

	ret := w.state.Get(-1)
	w.state.Pop(1)
	value, ok := ret.(lua.LString)
	if !ok {
		return nil, fmt.Errorf("function %q returned non-string payload", functionID)
	}
	return []byte(value), nil
}

// openSafeLibs exposes a minimal set of Lua stdlib modules for safety.
func openSafeLibs(state *lua.LState) {
	lua.OpenBase(state)
	lua.OpenTable(state)
	lua.OpenString(state)
	lua.OpenMath(state)
}

// validateLuaFunction ensures a Lua script defines the expected handle entrypoint.
func validateLuaFunction(path string) error {
	state := lua.NewState()
	defer state.Close()
	openSafeLibs(state)
	if err := state.DoFile(path); err != nil {
		return err
	}
	value := state.GetGlobal("handle")
	if _, ok := value.(*lua.LFunction); !ok {
		return fmt.Errorf("handle entrypoint not found")
	}
	return nil
}
