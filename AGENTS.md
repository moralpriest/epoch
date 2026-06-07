# AGENTS.md — EPOCH

## Project Overview

EPOCH (Event-driven Propagation of Crowd Hashing) is a Go package (`github.com/civilware/epoch`) enabling DERO blockchain crowd mining. Applications connect to a DERO node's GetWork server to attempt or submit proof-of-work hashes for mining rewards.

Key files:
- `epoch.go` — Core logic: connection management, POW hashing, block submission
- `methods.go` — JRPC2 handler definitions and API method wrappers
- `epoch_test.go` — Integration tests (require DERO simulator)

**Architecture note**: The package uses a singleton `var epoch EPOCH` for all state. Tests share global state — the test suite handles this by running everything within `TestEPOCH`, but adding new top-level test functions risks interference.

**Dependency note**: `go.mod` has a `replace` directive that forks `github.com/deroproject/derohe` → `github.com/civilware/derohe`. Do not change this unless you understand the fork's differences.

## Build & Test Commands

```bash
# Build
go build ./...

# Run all tests (requires DERO simulator at 127.0.0.1:20000)
go test ./... -v

# Run a single test
go test -run TestEPOCH -v

# Run a specific subtest
go test -run TestEPOCH/SetPort -v
go test -run TestEPOCH/AttemptEPOCH -v

# Run benchmark (also requires simulator)
go test -bench BenchmarkAttemptHashes -v

# Race detection
go test -race ./...

# Static analysis
go vet ./...

# Format code
gofmt -s -w .

# Mainnet test (opt-in, connects to external node)
RUN_MAINNET_TEST=true go test -run TestMainnet -v
```

Tests require a running DERO simulator. The `TestEPOCH` function sets up its own simulator wallet and connects to `127.0.0.1:20000`. The `epoch_tests/` directory is created during tests and cleaned up automatically (also in `.gitignore`).

## Branch Workflow

- PRs target the `dev` branch
- Changes are compiled and merged into a `release/` branch
- Final releases are pushed to `main`

## Code Style

### Imports

Group imports with stdlib first, then a blank line, then third-party packages:

```go
import (
    "context"
    "fmt"
    "sync"

    "github.com/civilware/tela/logger"
    "github.com/deroproject/derohe/rpc"
)
```

### Naming Conventions

- **Package**: lowercase single word (`epoch`)
- **Exported types/functions**: PascalCase (`EPOCH`, `AttemptHashes`, `SetPort`)
- **Unexported**: camelCase (`connection`, `jobs`, `powHash`)
- **Result types**: suffix with `_Result` (`EPOCH_Result`, `GetSessionEPOCH_Result`)
- **Param types**: suffix with `_Params` (`Attempt_Params`, `Submit_Params`)
- **JSON tags**: snake_case (`json:"epochHashes"`)
- **Constants**: UPPER_SNAKE_CASE (`DEFAULT_MAX_THREADS`, `LIMIT_MAX_HASHES`)

### Error Handling

Use named return values with early returns. Check errors immediately:

```go
func SetPort(port int) (err error) {
    if port < 1 || port > 65535 {
        err = fmt.Errorf("invalid EPOCH port")
        return
    }
    // ...
    return
}
```

Wrap errors with context: `fmt.Errorf("could not get host: %s", err)`

### Concurrency

- Embed `sync.RWMutex` in structs for field-level locking
- Use `RLock`/`RUnlock` for reads, `Lock`/`Unlock` for writes
- Use buffered channels as semaphores for worker limiting
- Use `sync.WaitGroup` for goroutine coordination

### Comments

Add doc comments to all exported types and functions. Keep them concise:

```go
// Set the EPOCH reward address, must be a registered DERO address
func SetAddress(address string) (err error) {
```

### Testing

- Use `github.com/stretchr/testify/assert` for assertions
- Use `t.Run()` for subtests
- Use `t.Cleanup()` for resource cleanup
- Use `t.Fatalf()` for setup failures, `t.Errorf()` or assertions for test failures
- Use `t.Logf()` for diagnostic output
- Tests call `globals.InitNetwork()` — this is a one-time initialization; do not add separate test functions that also call it without understanding the conflict
