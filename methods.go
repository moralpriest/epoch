package epoch

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/civilware/tela/logger"
	"github.com/creachadair/jrpc2/handler"
	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/rpc"
)

// Address-aware connection reuse for AttemptEPOCHWithAddr:
// When dApps (e.g. DeroBeats) call AttemptEPOCHWithAddr every ~5s for the
// same artist, we reuse the existing connection instead of restarting.
// A 60s keepalive timer shuts down cleanly when the dApp stops calling.

var (
	lastEpochAddress    string          // last address used by AttemptEPOCHWithAddr
	epochKeepaliveTimer *time.Timer     // fires 60s after last AttemptEPOCHWithAddr call
)

const EPOCH_KEEPAWAY_DURATION = 60 * time.Second

var epochHandler = map[string]handler.Func{
	"AttemptEPOCH":      handler.New(AttemptEPOCH),
	"SubmitEPOCH":       handler.New(SubmitEPOCH),
	"GetMaxHashesEPOCH": handler.New(GetMaxHashesEPOCH),
	"GetAddressEPOCH":   handler.New(GetAddressEPOCH),
	"GetSessionEPOCH":   handler.New(GetSessionEPOCH),
	"StopEPOCH":         handler.New(StopEPOCH),
}

// Returns methods in epochHandler
func GetHandler() map[string]handler.Func {
	handler := map[string]handler.Func{}
	for n, f := range epochHandler {
		handler[n] = f
	}

	return handler
}

// resetEpochKeepalive resets the 60s silence timer.
// Called after each AttemptEPOCHWithAddr call to extend the connection.
func resetEpochKeepalive() {
	if epochKeepaliveTimer != nil {
		epochKeepaliveTimer.Stop()
	}
	epochKeepaliveTimer = time.AfterFunc(EPOCH_KEEPAWAY_DURATION, func() {
		logger.Printf("[EPOCH] Keepalive timeout — no calls for %s, stopping\n", EPOCH_KEEPAWAY_DURATION)
		StopGetWork()
		lastEpochAddress = ""
	})
}

// StopEPOCH is an explicit shutdown handler for dApps or user actions.
// It stops the keepalive timer and closes the EPOCH connection.
func StopEPOCH(ctx context.Context) (err error) {
	logger.Printf("[EPOCH] StopEPOCH called\n")
	if epochKeepaliveTimer != nil {
		epochKeepaliveTimer.Stop()
		epochKeepaliveTimer = nil
	}
	StopGetWork()
	lastEpochAddress = ""
	return nil
}

// GetLastEpochAddress returns the last address used by AttemptEPOCHWithAddr.
func GetLastEpochAddress() string {
	return lastEpochAddress
}

// SetLastEpochAddress stores the address for connection reuse tracking.
func SetLastEpochAddress(addr string) {
	lastEpochAddress = addr
}

// ResetKeepaliveTimer resets the silence timer after each AttemptEPOCHWithAddr call.
func ResetKeepaliveTimer() {
	resetEpochKeepalive()
}

// StopKeepaliveTimer stops the silence timer without closing the connection.
func StopKeepaliveTimer() {
	if epochKeepaliveTimer != nil {
		epochKeepaliveTimer.Stop()
		epochKeepaliveTimer = nil
	}
}

// EPOCH call structures
type (
	// EPOCH attempt params
	Attempt_Params struct {
		Hashes int `json:"hashes"`
	}

	// EPOCH submit params
	Submit_Params struct {
		Job        rpc.GetBlockTemplate_Result `json:"jobTemplate"`
		PowHash    [32]byte                    `json:"powHash"`
		EpochWork  [block.MINIBLOCK_SIZE]byte  `json:"epochWork"`
		Difficulty big.Int                     `json:"epochDifficulty"`
	}

	// EPOCH attempt/submit result
	EPOCH_Result struct {
		Hashes     uint64  `json:"epochHashes"`
		Submitted  int     `json:"epochSubmitted"`
		Duration   int64   `json:"epochDuration"`
		HashPerSec float64 `json:"epochHashPerSecond,omitempty"`
		Error      error   `json:"epochError,omitempty"`
	}
)

// AttemptEPOCH performs the POW and submits its results to the connected node
func AttemptEPOCH(ctx context.Context, p Attempt_Params) (result EPOCH_Result, err error) {
	return AttemptHashes(p.Hashes)
}

// SubmitEPOCH submits pre computed block data to the connected node
func SubmitEPOCH(ctx context.Context, params []Submit_Params) (result EPOCH_Result, err error) {
	return SubmitHashes(params)
}

// EPOCH GetMaxHashes result
type GetMaxHashes_Result struct {
	MaxHashes int `json:"maxHashes"`
}

// GetMaxHashesEPOCH returns the current max hash per request setting if EPOCH is active
func GetMaxHashesEPOCH(ctx context.Context) (result GetMaxHashes_Result, err error) {
	if !IsActive() {
		err = fmt.Errorf("epoch is not active")
		return
	}

	result.MaxHashes = GetMaxHashes()

	return
}

// EPOCH GetAddressEPOCH result
type GetAddressEPOCH_Result struct {
	Address string `json:"epochAddress"`
}

// GetAddressEPOCH returns the current address EPOCH has set if active
func GetAddressEPOCH(ctx context.Context) (result GetAddressEPOCH_Result, err error) {
	if !IsActive() {
		err = fmt.Errorf("epoch is not active")
		return
	}

	result.Address = GetAddress()

	return
}

// EPOCH GetSessionEPOCH result
type GetSessionEPOCH_Result struct {
	Hashes     uint64 `json:"sessionHashes"`
	MiniBlocks int    `json:"sessionMinis"`
	Version    string `json:"sessionVersion"`
}

// GetSessionEPOCH returns the statistics for the current EPOCH session if active. There may be multiple applications connected to
// a EPOCH session, the result values will be the sum of all the connections
func GetSessionEPOCH(ctx context.Context) (result GetSessionEPOCH_Result, err error) {
	if !IsActive() {
		err = fmt.Errorf("epoch is not active")
		return
	}

	return GetSession(time.Second * 15)
}
