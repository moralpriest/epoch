package epoch

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/civilware/tela/logger"
	"github.com/deroproject/derohe/astrobwt/astrobwtv3"
	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/blockchain"
	"github.com/deroproject/derohe/globals"
	"github.com/deroproject/derohe/rpc"
	"github.com/gorilla/websocket"
)

// Web socket connection and synchronization state.
type connection struct {
	sync.Mutex
	ws   *websocket.Conn
	done chan struct{}
}

func (c *connection) active() bool {
	c.Lock()
	defer c.Unlock()

	return c.ws != nil
}

func (c *connection) install(ws *websocket.Conn) chan struct{} {
	c.Lock()
	defer c.Unlock()

	c.done = make(chan struct{})
	c.ws = ws
	return c.done
}

func (c *connection) stop() {
	c.Lock()
	ws := c.ws
	done := c.done
	c.ws = nil
	c.done = nil
	if done != nil {
		close(done)
	}
	c.Unlock()

	if ws != nil {
		_ = ws.Close()
	}
}

func (c *connection) stopIfCurrent(ws *websocket.Conn, done chan struct{}) bool {
	c.Lock()
	if c.ws != ws || c.done != done {
		c.Unlock()
		return false
	}

	c.ws = nil
	c.done = nil
	close(done)
	c.Unlock()

	_ = ws.Close()
	return true
}

func (c *connection) writeJSON(value any) error {
	c.Lock()
	defer c.Unlock()

	if c.ws == nil {
		return errConnectionClosed
	}

	if err := c.ws.SetWriteDeadline(time.Now().Add(WRITE_WAIT)); err != nil {
		return fmt.Errorf("could not set websocket write deadline: %w", err)
	}
	if err := c.ws.WriteJSON(value); err != nil {
		return fmt.Errorf("could not write websocket message: %w", err)
	}

	return nil
}

func (c *connection) writePing(ws *websocket.Conn) error {
	c.Lock()
	defer c.Unlock()

	if c.ws != ws {
		return errConnectionClosed
	}

	if err := ws.SetWriteDeadline(time.Now().Add(WRITE_WAIT)); err != nil {
		return fmt.Errorf("could not set websocket write deadline: %w", err)
	}
	if err := ws.WriteMessage(websocket.PingMessage, nil); err != nil {
		return fmt.Errorf("could not write websocket ping: %w", err)
	}

	return nil
}

// DERO block template and synchronization state.
type jobs struct {
	job rpc.GetBlockTemplate_Result
	sync.RWMutex
}

// EPOCH main structure.
type EPOCH struct {
	conn       connection             // Connection to GetWork from DERO node
	jobs       jobs                   // DERO block template for work
	port       string                 // GetWork port that EPOCH will connect to
	address    string                 // EPOCH reward address
	processing bool                   // When EPOCH is processing or submitting jobs
	processes  int                    // Number of concurrently running hash operations
	maxHashes  int                    // Maximum accepted hashes for one request
	maxThreads int                    // Maximum concurrent workers
	semaphore  chan struct{}          // Limits workers across concurrent requests
	session    GetSessionEPOCH_Result // Session counts hashes and submissions
	sync.RWMutex
	lifecycle sync.RWMutex // Serializes connection lifecycle with hash operations
}

var epoch EPOCH

var (
	errConnectionClosed = errors.New("epoch connection is closed")
	errInvalidTimeout   = errors.New("epoch timeout must be positive")
)

const (
	DEFAULT_MAX_THREADS = 2
	DEFAULT_WORK_PORT   = 10100
	LIMIT_MAX_HASHES    = 10000
	PONG_WAIT           = 60 * time.Second
	PING_PERIOD         = 54 * time.Second
	WRITE_WAIT          = 10 * time.Second
)

// Initialize EPOCH package defaults.
func init() {
	epoch.port = fmt.Sprintf(":%d", DEFAULT_WORK_PORT)
	epoch.maxHashes = 1000
	epoch.session.Version = "1.0.0"
	SetMaxThreads(DEFAULT_MAX_THREADS)
}

// Check if EPOCH connection is active.
func IsActive() bool {
	return epoch.conn.active()
}

// Set EPOCH processing when doing jobs or submissions.
func setProcessing(processing bool) {
	epoch.Lock()
	if processing {
		epoch.processing = true
	} else if epoch.processes == 0 {
		epoch.processing = false
	}
	epoch.Unlock()
}

func beginProcessing() {
	epoch.Lock()
	epoch.processes++
	epoch.processing = true
	epoch.Unlock()
}

func endProcessing() {
	epoch.Lock()
	if epoch.processes > 0 {
		epoch.processes--
	}
	epoch.processing = epoch.processes > 0
	epoch.Unlock()
}

// Set a new DERO block template and return its last error.
func (e *EPOCH) newJob(job rpc.GetBlockTemplate_Result) (lastError string) {
	e.jobs.Lock()
	e.jobs.job = job
	e.jobs.Unlock()

	return job.LastError
}

// Get the current DERO block template.
func (e *EPOCH) getJob() (job rpc.GetBlockTemplate_Result) {
	e.jobs.RLock()
	job = e.jobs.job
	e.jobs.RUnlock()

	return
}

// JobIsReady waits for a JobID to be present, or returns an error after timeout.
func JobIsReady(timeout time.Duration) (err error) {
	if timeout <= 0 {
		return fmt.Errorf("could not get EPOCH job: %w", errInvalidTimeout)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		epoch.jobs.RLock()
		ready := epoch.jobs.job.JobID != ""
		epoch.jobs.RUnlock()
		if ready {
			return nil
		}

		select {
		case <-timer.C:
			return fmt.Errorf("could not get EPOCH job after %s", timeout)
		case <-ticker.C:
		}
	}
}

// Check if EPOCH is processing jobs or submissions.
func IsProcessing() bool {
	epoch.RLock()
	defer epoch.RUnlock()

	return epoch.processing || epoch.processes > 0
}

// Set the EPOCH reward address, which must be a registered DERO address.
func SetAddress(address string) (err error) {
	if _, err = globals.ParseValidateAddress(address); err != nil {
		return
	}

	epoch.Lock()
	epoch.address = address
	epoch.Unlock()

	return nil
}

// Get the EPOCH reward address.
func GetAddress() string {
	epoch.RLock()
	defer epoch.RUnlock()

	return epoch.address
}

// Set the GetWork port if port is valid.
func SetPort(port int) (err error) {
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid EPOCH port %d", port)
	}

	epoch.Lock()
	epoch.port = fmt.Sprintf(":%d", port)
	epoch.Unlock()

	return nil
}

// Get the EPOCH work port.
func GetPort() string {
	epoch.RLock()
	defer epoch.RUnlock()

	return strings.TrimPrefix(epoch.port, ":")
}

// Set the maximum number of hash attempts or submissions for one request.
func SetMaxHashes(hashes int) (err error) {
	if hashes < 1 {
		return fmt.Errorf("hashes must be at least 1")
	}
	if hashes > LIMIT_MAX_HASHES {
		return fmt.Errorf("cannot exceed %d hashes", LIMIT_MAX_HASHES)
	}

	epoch.Lock()
	epoch.maxHashes = hashes
	epoch.Unlock()

	return nil
}

// Get the EPOCH maxHashes value.
func GetMaxHashes() int {
	epoch.RLock()
	defer epoch.RUnlock()

	return epoch.maxHashes
}

// Parse EPOCH hashes and return them as a formatted string.
func HashesToString(hashes uint64) string {
	if hashes >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(hashes)/1_000_000)
	}

	return fmt.Sprintf("%.1fK", float64(hashes)/1_000)
}

// Set the maximum number of threads used for attempting or submitting.
func SetMaxThreads(threads int) {
	max := runtime.NumCPU()
	if threads > max {
		threads = max
	} else if threads < 1 {
		threads = 1
	}

	epoch.Lock()
	epoch.maxThreads = threads
	epoch.Unlock()
}

// Get the EPOCH maxThreads value.
func GetMaxThreads() int {
	epoch.RLock()
	defer epoch.RUnlock()

	return epoch.maxThreads
}

// Stop listening to the GetWork server.
func StopGetWork() {
	epoch.lifecycle.Lock()
	defer epoch.lifecycle.Unlock()

	epoch.conn.stop()
	clearJobLocked()
}

func clearJobLocked() {
	epoch.jobs.Lock()
	epoch.jobs.job = rpc.GetBlockTemplate_Result{}
	epoch.jobs.Unlock()
}

func clearJobIfInactive() {
	epoch.lifecycle.Lock()
	defer epoch.lifecycle.Unlock()

	if !epoch.conn.active() {
		clearJobLocked()
	}
}

// Start listening to the GetWork server. If address is empty, the configured
// EPOCH address is used. endpoint is the DERO daemon address.
func StartGetWork(address, endpoint string) (err error) {
	epoch.lifecycle.Lock()
	defer epoch.lifecycle.Unlock()

	if IsActive() {
		return fmt.Errorf("already running")
	}

	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return fmt.Errorf("could not get host: %w", err)
	}

	if address != "" {
		if err = SetAddress(address); err != nil {
			return fmt.Errorf("could not set address: %w", err)
		}
	}

	epoch.RLock()
	rewardAddress := epoch.address
	workPort := strings.TrimPrefix(epoch.port, ":")
	maxThreads := epoch.maxThreads
	epoch.RUnlock()

	if _, err = globals.ParseValidateAddress(rewardAddress); err != nil {
		return fmt.Errorf("address %q is not valid: %w", rewardAddress, err)
	}

	workEndpoint := net.JoinHostPort(host, workPort)
	u := url.URL{Scheme: "wss", Host: workEndpoint, Path: "/ws/" + rewardAddress}

	// DERO GetWork endpoints commonly use self-signed certificates. Keep the
	// historical behavior, but use a private dialer so global websocket state
	// is never mutated. Certificate pinning/configuration should be exposed in
	// a future API for deployments requiring server authentication.
	dialer := *websocket.DefaultDialer
	dialer.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // required for existing DERO GetWork endpoints
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ws, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("could not connect to GetWork: %w", err)
	}

	if err = ws.SetReadDeadline(time.Now().Add(PONG_WAIT)); err != nil {
		_ = ws.Close()
		return fmt.Errorf("could not set websocket read deadline: %w", err)
	}
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(PONG_WAIT))
	})

	epoch.jobs.Lock()
	epoch.jobs.job = rpc.GetBlockTemplate_Result{}
	epoch.jobs.Unlock()
	epoch.Lock()
	epoch.session.Hashes = 0
	epoch.session.MiniBlocks = 0
	epoch.semaphore = make(chan struct{}, maxThreads)
	epoch.Unlock()

	done := epoch.conn.install(ws)
	logger.Printf("[EPOCH] Connected to %s\n", u.String())
	logger.Printf("[EPOCH] Will use %d threads\n", maxThreads)

	go readJobs(ws, done)
	go pingConnection(ws, done)

	return nil
}

func readJobs(ws *websocket.Conn, done chan struct{}) {
	defer func() {
		if epoch.conn.stopIfCurrent(ws, done) {
			clearJobIfInactive()
		}
	}()

	var result rpc.GetBlockTemplate_Result
	for {
		if readErr := ws.ReadJSON(&result); readErr != nil {
			if !websocket.IsCloseError(readErr, websocket.CloseNormalClosure, websocket.CloseGoingAway) && !strings.Contains(readErr.Error(), "closed network connection") {
				logger.Errorf("[EPOCH] connection error: %s\n", readErr)
			}
			break
		}

		if lastError := epoch.newJob(result); lastError != "" {
			logger.Errorf("[EPOCH] Job error: %s\n", lastError)
		}
	}

	logger.Printf("[EPOCH] Closed\n")
}

func pingConnection(ws *websocket.Conn, done chan struct{}) {
	ticker := time.NewTicker(PING_PERIOD)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := epoch.conn.writePing(ws); err != nil {
				if epoch.conn.stopIfCurrent(ws, done) {
					clearJobIfInactive()
				}
				return
			}
		case <-done:
			return
		}
	}
}

// GetSession returns the current EPOCH session statistics, or an error if the
// operation does not finish before timeout.
func GetSession(timeout time.Duration) (session GetSessionEPOCH_Result, err error) {
	if timeout <= 0 {
		return session, fmt.Errorf("could not get EPOCH session: %w", errInvalidTimeout)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		if !IsProcessing() {
			epoch.RLock()
			session = epoch.session
			epoch.RUnlock()
			return session, nil
		}

		select {
		case <-timer.C:
			return session, fmt.Errorf("could not get EPOCH session after %s", timeout)
		case <-ticker.C:
		}
	}
}

// Compute a POW hash from a job template and return variables for submission.
func powHash() (job rpc.GetBlockTemplate_Result, powhash [32]byte, work [block.MINIBLOCK_SIZE]byte, diff big.Int, err error) {
	var randomBuf [12]byte
	if _, err = rand.Read(randomBuf[:]); err != nil {
		return job, powhash, work, diff, fmt.Errorf("could not generate random bytes: %w", err)
	}

	job = epoch.getJob()

	n, decodeErr := hex.Decode(work[:], []byte(job.Blockhashing_blob))
	if decodeErr != nil {
		return job, powhash, work, diff, fmt.Errorf("could not decode block hashing blob: %w", decodeErr)
	}
	if n != block.MINIBLOCK_SIZE {
		return job, powhash, work, diff, fmt.Errorf("decoded block hashing blob has %d bytes, want %d", n, block.MINIBLOCK_SIZE)
	}

	copy(work[block.MINIBLOCK_SIZE-12:], randomBuf[:])
	work[block.MINIBLOCK_SIZE-1] = 1

	if _, ok := diff.SetString(job.Difficulty, 10); !ok || diff.Sign() <= 0 {
		return job, powhash, work, diff, fmt.Errorf("invalid EPOCH difficulty %q", job.Difficulty)
	}

	if work[0]&0xf != 1 {
		return job, powhash, work, diff, fmt.Errorf("unknown version, please check for updates %v", work[0]&0xf)
	}

	powhash = astrobwtv3.AstroBWTv3(work[:])
	return job, powhash, work, diff, nil
}

// Check if powhash is valid and submit it as a miniblock to the connected daemon.
func submitBlock(job rpc.GetBlockTemplate_Result, powhash [32]byte, work [block.MINIBLOCK_SIZE]byte, diff big.Int) (valid bool, err error) {
	if !blockchain.CheckPowHashBig(powhash, &diff) {
		return false, nil
	}

	logger.Printf("[EPOCH] Submitting valid miniblock POW hash, difficulty: %s height: %d\n", job.Difficulty, job.Height)
	if err = epoch.conn.writeJSON(rpc.SubmitBlock_Params{
		JobID:                 job.JobID,
		MiniBlockhashing_blob: fmt.Sprintf("%x", work[:]),
	}); err != nil {
		return false, err
	}

	return true, nil
}

type workerResult struct {
	submitted bool
	err       error
}

func runWorkers(count int, work func() (bool, error)) (attempted, submitted int, firstErr error) {
	return runIndexedWorkers(count, func(int) (bool, error) {
		return work()
	})
}

func runIndexedWorkers(count int, work func(int) (bool, error)) (attempted, submitted int, firstErr error) {
	if count == 0 {
		return 0, 0, nil
	}

	epoch.RLock()
	semaphore := epoch.semaphore
	maxThreads := epoch.maxThreads
	epoch.RUnlock()
	if semaphore == nil || maxThreads < 1 || cap(semaphore) < 1 {
		return 0, 0, errConnectionClosed
	}

	workerCount := min(count, min(maxThreads, cap(semaphore)))
	tasks := make(chan int)
	results := make(chan workerResult, count)
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for index := range tasks {
				semaphore <- struct{}{}
				isSubmitted, err := work(index)
				<-semaphore
				results <- workerResult{submitted: isSubmitted, err: err}
			}
		}()
	}

	for i := 0; i < count; i++ {
		tasks <- i
	}
	close(tasks)
	wg.Wait()
	close(results)

	for result := range results {
		attempted++
		if result.submitted {
			submitted++
		}
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
	}

	return attempted, submitted, firstErr
}

// AttemptHashes performs POW for the requested number of hashes and submits
// valid hashes as miniblocks to the connected node.
func AttemptHashes(hashes int) (result EPOCH_Result, err error) {
	epoch.lifecycle.RLock()
	defer epoch.lifecycle.RUnlock()

	if !IsActive() {
		return result, fmt.Errorf("epoch is not active: %w", errConnectionClosed)
	}

	maxHashes := GetMaxHashes()
	if hashes < 1 {
		return result, fmt.Errorf("hashes must be at least 1")
	}
	if hashes > maxHashes {
		return result, fmt.Errorf("hashes exceeds maxHashes %d/%d", hashes, maxHashes)
	}

	beginProcessing()
	defer endProcessing()

	started := time.Now()
	attempted, submitted, firstErr := runWorkers(hashes, func() (bool, error) {
		job, powhash, work, diff, powErr := powHash()
		if powErr != nil {
			return false, powErr
		}
		return submitBlock(job, powhash, work, diff)
	})

	result.Hashes = uint64(attempted)
	result.Submitted = submitted
	duration := time.Since(started)
	result.Duration = duration.Milliseconds()
	if duration > 0 {
		result.HashPerSec = math.Round(float64(attempted)/duration.Seconds()*100) / 100
	}

	epoch.Lock()
	epoch.session.Hashes += uint64(attempted)
	epoch.session.MiniBlocks += submitted
	epoch.Unlock()

	if firstErr != nil {
		result.Error = firstErr
		return result, firstErr
	}

	return result, nil
}

// SubmitHashes checks and submits valid precomputed hashes as miniblocks. Only
// the session miniblock total is increased by this method.
func SubmitHashes(params []Submit_Params) (result EPOCH_Result, err error) {
	epoch.lifecycle.RLock()
	defer epoch.lifecycle.RUnlock()

	if !IsActive() {
		return result, fmt.Errorf("epoch is not active: %w", errConnectionClosed)
	}

	maxHashes := GetMaxHashes()
	if len(params) > maxHashes {
		return result, fmt.Errorf("requested submission exceeds maxHashes %d/%d", len(params), maxHashes)
	}

	beginProcessing()
	defer endProcessing()

	started := time.Now()
	attempted, submitted, firstErr := runSubmitWorkers(params)

	result.Hashes = uint64(attempted)
	result.Submitted = submitted
	result.Duration = time.Since(started).Milliseconds()
	epoch.Lock()
	epoch.session.MiniBlocks += submitted
	epoch.Unlock()

	if firstErr != nil {
		result.Error = firstErr
		return result, firstErr
	}

	return result, nil
}

func runSubmitWorkers(params []Submit_Params) (attempted, submitted int, firstErr error) {
	return runIndexedWorkers(len(params), func(index int) (bool, error) {
		param := params[index]
		return submitBlock(param.Job, param.PowHash, param.EpochWork, param.Difficulty)
	})
}
