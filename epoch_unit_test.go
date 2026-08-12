package epoch

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/rpc"
)

func TestCanceledMethodContexts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := AttemptEPOCH(ctx, Attempt_Params{Hashes: 1}); err == nil {
		t.Fatal("AttemptEPOCH unexpectedly ignored canceled context")
	}
	if _, err := SubmitEPOCH(ctx, nil); err == nil {
		t.Fatal("SubmitEPOCH unexpectedly ignored canceled context")
	}
	if _, err := GetMaxHashesEPOCH(ctx); err == nil {
		t.Fatal("GetMaxHashesEPOCH unexpectedly ignored canceled context")
	}
}

func TestConfigurationValidation(t *testing.T) {
	oldMaxHashes := GetMaxHashes()
	oldMaxThreads := GetMaxThreads()
	t.Cleanup(func() {
		_ = SetMaxHashes(oldMaxHashes)
		SetMaxThreads(oldMaxThreads)
	})

	for _, test := range []struct {
		name   string
		value  int
		accept bool
	}{
		{name: "negative", value: -1},
		{name: "zero", value: 0},
		{name: "too large", value: LIMIT_MAX_HASHES + 1},
		{name: "minimum", value: 1, accept: true},
		{name: "maximum", value: LIMIT_MAX_HASHES, accept: true},
	} {
		t.Run("SetMaxHashes/"+test.name, func(t *testing.T) {
			err := SetMaxHashes(test.value)
			if test.accept && err != nil {
				t.Fatalf("SetMaxHashes(%d): %v", test.value, err)
			}
			if !test.accept && err == nil {
				t.Fatalf("SetMaxHashes(%d) unexpectedly succeeded", test.value)
			}
		})
	}

	for _, test := range []struct {
		value  uint64
		expect string
	}{
		{value: 0, expect: "0.0K"},
		{value: 999_999, expect: "1000.0K"},
		{value: 1_000_000, expect: "1.0M"},
		{value: 10_100_000, expect: "10.1M"},
	} {
		if got := HashesToString(test.value); got != test.expect {
			t.Errorf("HashesToString(%d) = %q, want %q", test.value, got, test.expect)
		}
	}
}

func TestConfigurationAccessIsRaceFree(t *testing.T) {
	oldMaxHashes := GetMaxHashes()
	oldMaxThreads := GetMaxThreads()
	t.Cleanup(func() {
		_ = SetMaxHashes(oldMaxHashes)
		SetMaxThreads(oldMaxThreads)
	})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			if err := SetMaxHashes(value%LIMIT_MAX_HASHES + 1); err != nil {
				t.Errorf("SetMaxHashes: %v", err)
			}
			SetMaxThreads(value)
			_ = GetMaxHashes()
			_ = GetMaxThreads()
		}(i)
	}
	wg.Wait()
}

func TestTimeoutsAndProcessingState(t *testing.T) {
	epoch.jobs.Lock()
	oldJob := epoch.jobs.job
	epoch.jobs.job = rpc.GetBlockTemplate_Result{}
	epoch.jobs.Unlock()
	epoch.Lock()
	oldProcessing := epoch.processing
	oldProcesses := epoch.processes
	epoch.processing = false
	epoch.processes = 0
	epoch.Unlock()
	t.Cleanup(func() {
		epoch.jobs.Lock()
		epoch.jobs.job = oldJob
		epoch.jobs.Unlock()
		epoch.Lock()
		epoch.processing = oldProcessing
		epoch.processes = oldProcesses
		epoch.Unlock()
	})

	if err := JobIsReady(0); err == nil {
		t.Fatal("JobIsReady(0) unexpectedly succeeded")
	}
	if _, err := GetSession(0); err == nil {
		t.Fatal("GetSession(0) unexpectedly succeeded")
	}
	if err := JobIsReady(20 * time.Millisecond); err == nil {
		t.Fatal("JobIsReady unexpectedly found an empty job")
	}

	setProcessing(true)
	if _, err := GetSession(20 * time.Millisecond); err == nil {
		t.Fatal("GetSession unexpectedly returned while processing")
	}
	setProcessing(false)
}

func TestProcessingReferenceCount(t *testing.T) {
	epoch.Lock()
	oldProcessing := epoch.processing
	oldProcesses := epoch.processes
	epoch.processing = false
	epoch.processes = 0
	epoch.Unlock()
	t.Cleanup(func() {
		epoch.Lock()
		epoch.processing = oldProcessing
		epoch.processes = oldProcesses
		epoch.Unlock()
	})

	beginProcessing()
	beginProcessing()
	setProcessing(false)
	if !IsProcessing() {
		t.Fatal("processing became false while an operation remained")
	}
	endProcessing()
	if !IsProcessing() {
		t.Fatal("processing became false after only one operation ended")
	}
	endProcessing()
	if IsProcessing() {
		t.Fatal("processing remained true after all operations ended")
	}
}

func TestPowHashInputErrors(t *testing.T) {
	epoch.jobs.Lock()
	oldJob := epoch.jobs.job
	epoch.jobs.Unlock()
	t.Cleanup(func() {
		epoch.jobs.Lock()
		epoch.jobs.job = oldJob
		epoch.jobs.Unlock()
	})

	for _, test := range []struct {
		name string
		blob string
		diff string
	}{
		{name: "invalid blob", blob: "not-hex", diff: "1"},
		{name: "wrong blob size", blob: "00", diff: "1"},
		{name: "invalid difficulty", blob: strings.Repeat("00", block.MINIBLOCK_SIZE), diff: "not-a-number"},
	} {
		t.Run(test.name, func(t *testing.T) {
			epoch.newJob(rpc.GetBlockTemplate_Result{
				Blockhashing_blob: test.blob,
				Difficulty:        test.diff,
			})
			if _, _, _, _, err := powHash(); err == nil {
				t.Fatal("powHash unexpectedly succeeded")
			}
		})
	}
}
