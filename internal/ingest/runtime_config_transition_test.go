package ingest

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func TestAgeReductionSteps(t *testing.T) {
	tests := []struct {
		name    string
		current int
		target  int
		want    []int
		wantErr bool
	}{
		{name: "target is one quarter", current: 1000, target: 250, want: []int{813, 625, 438, 250}},
		{name: "target is ten percent", current: 1000, target: 100, want: []int{910, 820, 730, 640, 550, 460, 370, 280, 190, 100}},
		{name: "target below ten percent is capped at ten steps", current: 1000, target: 50, want: []int{905, 810, 715, 620, 525, 430, 335, 240, 145, 50}},
		{name: "small reduction stays monotonic", current: 1000, target: 990, want: []int{995, 990}},
		{name: "no-op", current: 1000, target: 1000},
		{name: "zero target rejected", current: 1000, target: 0, wantErr: true},
		{name: "increase is not a reduction", current: 1000, target: 1001, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ageReductionSteps(tc.current, tc.target)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ageReductionSteps() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ageReductionSteps() = %v, want %v", got, tc.want)
			}
			if len(got) > maxAgeReductionSteps {
				t.Fatalf("got %d steps, maximum is %d", len(got), maxAgeReductionSteps)
			}
			previous := tc.current
			for _, step := range got {
				if step > previous {
					t.Fatalf("steps increased from %d to %d", previous, step)
				}
				previous = step
			}
		})
	}
}

func TestSizeReductionPlanUsesSeventyFivePercentWorkingBand(t *testing.T) {
	plan, err := newSizeReductionPlan(500, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if plan.stepLimit != 75 {
		t.Fatalf("stepLimit = %d, want 75", plan.stepLimit)
	}
	if plan.stepCount != 6 {
		t.Fatalf("stepCount = %d, want 6", plan.stepCount)
	}

	var got []int
	for {
		next, ok := plan.Next()
		if !ok {
			break
		}
		got = append(got, next)
	}
	want := []int{425, 350, 275, 200, 125, 100}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("size reduction steps = %v, want %v", got, want)
	}
	if got[len(got)-1] != 100 {
		t.Fatalf("final step = %d, want exact target 100", got[len(got)-1])
	}
}

func TestSizeReductionPlanRejectsUnavailableWorkingBand(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity int
	}{
		{name: "reserve disabled", capacity: 0},
		{name: "reserve too small", capacity: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newSizeReductionPlan(100, 10, tc.capacity); err == nil {
				t.Fatal("newSizeReductionPlan() succeeded; want error")
			}
		})
	}
}

func TestSizeReductionPlanDoesNotAllocateAllSteps(t *testing.T) {
	plan, err := newSizeReductionPlan(int(^uint(0)>>1), 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if plan.stepCount < 1_000_000 {
		t.Fatalf("stepCount = %d, want a large plan without eager allocation", plan.stepCount)
	}
	if next, ok := plan.Next(); !ok || next != plan.current {
		t.Fatalf("first Next() = (%d, %v), current = %d", next, ok, plan.current)
	}
}

func TestSplitFlushRecordsPreservesTypedBatchMetadataAndRowOrder(t *testing.T) {
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{10, 20, 30, 40, 50},
			"value": []float64{1, 2, 3, 4, 5},
			"host":  []string{"a", "b", "c", "d", "e"},
		},
		Validity: map[string][]bool{
			"value": {true, false, true, true, false},
			"host":  nil,
		},
		TagColumns: []string{"host"},
		DedupTime:  true,
		Signature:  "host:string,time:int64,value:float64",
	}

	var tasks [][]interface{}
	total, err := visitFlushRecordChunks([]interface{}{batch}, 2, func(task []interface{}, _ int) error {
		tasks = append(tasks, task)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(tasks) != 3 {
		t.Fatalf("split returned %d records across %d tasks, want 5 across 3", total, len(tasks))
	}

	var gotTimes []int64
	var gotValidity []bool
	for taskIndex, task := range tasks {
		taskRows := 0
		for _, raw := range task {
			part, ok := raw.(*TypedColumnBatch)
			if !ok {
				t.Fatalf("task %d batch type = %T, want *TypedColumnBatch", taskIndex, raw)
			}
			times := part.Data["time"].([]int64)
			taskRows += len(times)
			gotTimes = append(gotTimes, times...)
			gotValidity = append(gotValidity, part.Validity["value"]...)
			if !reflect.DeepEqual(part.TagColumns, []string{"host"}) || !part.DedupTime || part.Signature != batch.Signature {
				t.Fatalf("task %d lost batch metadata: %+v", taskIndex, part)
			}
			if part.Validity["host"] != nil {
				t.Fatalf("task %d changed nil validity semantics for host: %v", taskIndex, part.Validity["host"])
			}
		}
		if taskRows > 2 {
			t.Fatalf("task %d has %d records; maximum is 2", taskIndex, taskRows)
		}
	}
	if want := []int64{10, 20, 30, 40, 50}; !reflect.DeepEqual(gotTimes, want) {
		t.Fatalf("split row order = %v, want %v", gotTimes, want)
	}
	if want := []bool{true, false, true, true, false}; !reflect.DeepEqual(gotValidity, want) {
		t.Fatalf("split validity = %v, want %v", gotValidity, want)
	}
}

func TestSplitFlushRecordsPreservesLegacyColumnMap(t *testing.T) {
	batch := map[string]interface{}{
		"time":  []int64{10, 20, 30},
		"value": []float64{1.0, 2.0, 3.0},
	}
	var tasks [][]interface{}
	total, err := visitFlushRecordChunks([]interface{}{batch}, 2, func(task []interface{}, _ int) error {
		tasks = append(tasks, task)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(tasks) != 2 {
		t.Fatalf("split returned %d records across %d tasks, want 3 across 2", total, len(tasks))
	}
	first, ok := tasks[0][0].(map[string]interface{})
	if !ok {
		t.Fatalf("first split batch has type %T, want map[string]interface{}", tasks[0][0])
	}
	if got := first["time"].([]int64); !reflect.DeepEqual(got, []int64{10, 20}) {
		t.Fatalf("first split time = %v", got)
	}
	last, ok := tasks[1][0].(map[string]interface{})
	if !ok {
		t.Fatalf("last split batch has type %T, want map[string]interface{}", tasks[1][0])
	}
	if got := last["value"].([]float64); !reflect.DeepEqual(got, []float64{3.0}) {
		t.Fatalf("last split value = %v", got)
	}
}

func TestSplitFlushRecordsRejectsUnsupportedBatchWithoutMutation(t *testing.T) {
	original := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1, 2}, "invalid": "not-a-column-slice"}}
	if _, err := visitFlushRecordChunks([]interface{}{original}, 1, func([]interface{}, int) error { return nil }); err == nil {
		t.Fatal("visitFlushRecordChunks() succeeded on unsupported column data; want error")
	}
	if got := original.Data["time"].([]int64); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("rejected split mutated the source batch: %v", got)
	}
}

func TestApplyRuntimeConfigTransitionSplitsAndFlushesExistingSizeBuffer(t *testing.T) {
	cfg := &config.IngestConfig{
		MaxBufferSize:       6,
		MaxBufferAgeMS:      60_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      8,
		FlushTimeoutSeconds: 5,
	}
	buffer := NewArrowBuffer(cfg, &mockStorageBackend{}, zerolog.Nop())
	t.Cleanup(func() { _ = buffer.Close() })
	if err := buffer.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 4}); err != nil {
		t.Fatalf("ConfigureElasticReserve: %v", err)
	}

	start := time.Now().UnixMicro()
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{start, start + 1, start + 2, start + 3, start + 4},
			"value": []float64{1, 2, 3, 4, 5},
		},
		Validity:  map[string][]bool{"value": {true, false, true, true, false}},
		Signature: "time:int64,value:float64",
	}
	if err := buffer.WriteTypedColumnarDirect(context.Background(), "default", "transition-size", batch, 5); err != nil {
		t.Fatalf("WriteTypedColumnarDirect: %v", err)
	}

	if err := buffer.ApplyRuntimeConfigTransition(context.Background(), 2, 60_000, nil); err != nil {
		t.Fatalf("ApplyRuntimeConfigTransition: %v", err)
	}
	if got, _ := buffer.RuntimeConfig(); got != 2 {
		t.Fatalf("max_buffer_size = %d, want 2", got)
	}
	if got := buffer.totalRecordsWritten.Load(); got != 5 {
		t.Fatalf("totalRecordsWritten = %d, want all 5 flushed", got)
	}
	if got := buffer.elasticReserveRecords.Load(); got != 0 {
		t.Fatalf("elastic reserve occupancy = %d, want zero between steps", got)
	}
}

func TestApplyRuntimeConfigTransitionFlushesEligibleAgeBuffer(t *testing.T) {
	cfg := &config.IngestConfig{
		MaxBufferSize:       100,
		MaxBufferAgeMS:      10_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      8,
		FlushTimeoutSeconds: 5,
	}
	buffer := NewArrowBuffer(cfg, &mockStorageBackend{}, zerolog.Nop())
	t.Cleanup(func() { _ = buffer.Close() })
	start := time.Now().UnixMicro()
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{start, start + 1},
			"value": []float64{1, 2},
		},
		Signature: "time:int64,value:float64",
	}
	if err := buffer.WriteTypedColumnarDirect(context.Background(), "default", "transition-age", batch, 2); err != nil {
		t.Fatalf("WriteTypedColumnarDirect: %v", err)
	}
	shard := buffer.getShard("default/transition-age")
	shard.mu.Lock()
	shard.bufferStartTimes["default/transition-age"] = time.Now().Add(-time.Minute)
	shard.mu.Unlock()

	if err := buffer.ApplyRuntimeConfigTransition(context.Background(), 100, 2_500, nil); err != nil {
		t.Fatalf("ApplyRuntimeConfigTransition: %v", err)
	}
	if _, got := buffer.RuntimeConfig(); got != 2_500 {
		t.Fatalf("max_buffer_age_ms = %d, want 2500", got)
	}
	if got := buffer.totalRecordsWritten.Load(); got != 2 {
		t.Fatalf("totalRecordsWritten = %d, want all 2 flushed", got)
	}
}

func TestApplyRuntimeConfigTransitionRestoresFailedSizeFlush(t *testing.T) {
	cfg := &config.IngestConfig{
		MaxBufferSize:       6,
		MaxBufferAgeMS:      60_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      8,
		FlushTimeoutSeconds: 5,
	}
	buffer := NewArrowBuffer(cfg, &failingStorageBackend{err: errors.New("storage unavailable")}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = buffer.Close() })
	if err := buffer.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 4}); err != nil {
		t.Fatalf("ConfigureElasticReserve: %v", err)
	}
	start := time.Now().UnixMicro()
	batch := &TypedColumnBatch{Data: map[string]interface{}{
		"time":  []int64{start, start + 1, start + 2, start + 3, start + 4},
		"value": []float64{1, 2, 3, 4, 5},
	}}
	if err := buffer.WriteTypedColumnarDirect(context.Background(), "default", "transition-failure", batch, 5); err != nil {
		t.Fatalf("WriteTypedColumnarDirect: %v", err)
	}

	if err := buffer.ApplyRuntimeConfigTransition(context.Background(), 2, 60_000, nil); err == nil {
		t.Fatal("ApplyRuntimeConfigTransition() succeeded with unavailable storage; want error")
	}
	if got, _ := buffer.RuntimeConfig(); got != 6 {
		t.Fatalf("max_buffer_size after failed transition = %d, want original value 6", got)
	}
	shard := buffer.getShard("default/transition-failure")
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if got := shard.bufferRecordCounts["default/transition-failure"]; got != 5 {
		t.Fatalf("restored buffered row count = %d, want 5", got)
	}
	if got := len(shard.buffers["default/transition-failure"]); got == 0 {
		t.Fatal("failed transition discarded the buffered batches")
	}
}

func TestApplyRuntimeConfigTransitionRestoresFailedAgeFlush(t *testing.T) {
	cfg := &config.IngestConfig{
		MaxBufferSize:       100,
		MaxBufferAgeMS:      10_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      8,
		FlushTimeoutSeconds: 5,
	}
	buffer := NewArrowBuffer(cfg, &failingStorageBackend{err: errors.New("storage unavailable")}, zerolog.Nop())
	t.Cleanup(func() { _ = buffer.Close() })
	start := time.Now().UnixMicro()
	batch := &TypedColumnBatch{Data: map[string]interface{}{
		"time":  []int64{start, start + 1},
		"value": []float64{1, 2},
	}}
	if err := buffer.WriteTypedColumnarDirect(context.Background(), "default", "transition-age-failure", batch, 2); err != nil {
		t.Fatalf("WriteTypedColumnarDirect: %v", err)
	}
	key := "default/transition-age-failure"
	shard := buffer.getShard(key)
	shard.mu.Lock()
	shard.bufferStartTimes[key] = time.Now().Add(-time.Minute)
	shard.mu.Unlock()

	if err := buffer.ApplyRuntimeConfigTransition(context.Background(), 100, 2_500, nil); err == nil {
		t.Fatal("ApplyRuntimeConfigTransition() succeeded with unavailable storage; want error")
	}
	if _, got := buffer.RuntimeConfig(); got != 10_000 {
		t.Fatalf("max_buffer_age_ms after failed transition = %d, want original value 10000", got)
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if got := shard.bufferRecordCounts[key]; got != 2 {
		t.Fatalf("restored buffered row count = %d, want 2", got)
	}
	if got := len(shard.buffers[key]); got == 0 {
		t.Fatal("failed age transition discarded the buffered batches")
	}
}

type failAfterStorageWrites struct {
	failingStorageBackend
	mu       sync.Mutex
	writes   int
	failAt   int
	writeErr error
}

func (s *failAfterStorageWrites) nextWriteError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	if s.writes == s.failAt {
		return s.writeErr
	}
	return nil
}

func (s *failAfterStorageWrites) Write(context.Context, string, []byte) error {
	return s.nextWriteError()
}

func (s *failAfterStorageWrites) WriteReader(context.Context, string, io.Reader, int64) error {
	return s.nextWriteError()
}

type blockingTransitionStorage struct {
	failingStorageBackend
	startOnce sync.Once
	started   chan struct{}
	release   chan struct{}
}

func (s *blockingTransitionStorage) blockWrite() error {
	s.startOnce.Do(func() { close(s.started) })
	<-s.release
	return nil
}

func (s *blockingTransitionStorage) Write(context.Context, string, []byte) error {
	return s.blockWrite()
}

func (s *blockingTransitionStorage) WriteReader(context.Context, string, io.Reader, int64) error {
	return s.blockWrite()
}

func TestApplyRuntimeConfigTransitionRestoresOnlyUnflushedSuffix(t *testing.T) {
	cfg := &config.IngestConfig{
		MaxBufferSize:       6,
		MaxBufferAgeMS:      60_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      8,
		FlushTimeoutSeconds: 5,
	}
	storage := &failAfterStorageWrites{failAt: 2, writeErr: errors.New("second chunk write failed")}
	buffer := NewArrowBuffer(cfg, storage, zerolog.Nop())
	t.Cleanup(func() { _ = buffer.Close() })
	if err := buffer.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 4}); err != nil {
		t.Fatalf("ConfigureElasticReserve: %v", err)
	}
	start := time.Now().UnixMicro()
	batch := &TypedColumnBatch{Data: map[string]interface{}{
		"time":  []int64{start, start + 1, start + 2, start + 3, start + 4},
		"value": []float64{1, 2, 3, 4, 5},
	}}
	if err := buffer.WriteTypedColumnarDirect(context.Background(), "default", "transition-partial-failure", batch, 5); err != nil {
		t.Fatalf("WriteTypedColumnarDirect: %v", err)
	}

	if err := buffer.ApplyRuntimeConfigTransition(context.Background(), 2, 60_000, nil); err == nil {
		t.Fatal("ApplyRuntimeConfigTransition() succeeded after the second chunk failed; want error")
	}
	if got := buffer.totalRecordsWritten.Load(); got != 3 {
		t.Fatalf("successfully written rows = %d, want first bounded chunk of 3", got)
	}
	shard := buffer.getShard("default/transition-partial-failure")
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if got := shard.bufferRecordCounts["default/transition-partial-failure"]; got != 2 {
		t.Fatalf("restored unflushed suffix = %d rows, want 2", got)
	}
	remaining := shard.buffers["default/transition-partial-failure"]
	if len(remaining) == 0 {
		t.Fatal("the unflushed suffix was discarded")
	}
	first, ok := remaining[0].(*TypedColumnBatch)
	if !ok {
		t.Fatalf("restored batch type = %T, want *TypedColumnBatch", remaining[0])
	}
	if got := first.Data["value"].([]float64); !reflect.DeepEqual(got, []float64{4, 5}) {
		t.Fatalf("restored suffix values = %v, want [4 5]", got)
	}
}

func TestCloseWaitsForActiveRuntimeIngestTransition(t *testing.T) {
	cfg := &config.IngestConfig{
		MaxBufferSize:       6,
		MaxBufferAgeMS:      60_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      8,
		FlushTimeoutSeconds: 5,
	}
	storage := &blockingTransitionStorage{started: make(chan struct{}), release: make(chan struct{})}
	buffer := NewArrowBuffer(cfg, storage, zerolog.Nop())
	if err := buffer.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 4}); err != nil {
		t.Fatalf("ConfigureElasticReserve: %v", err)
	}
	start := time.Now().UnixMicro()
	batch := &TypedColumnBatch{Data: map[string]interface{}{
		"time":  []int64{start, start + 1, start + 2, start + 3, start + 4},
		"value": []float64{1, 2, 3, 4, 5},
	}}
	if err := buffer.WriteTypedColumnarDirect(context.Background(), "default", "transition-shutdown", batch, 5); err != nil {
		t.Fatalf("WriteTypedColumnarDirect: %v", err)
	}

	transitionResult := make(chan error, 1)
	go func() {
		transitionResult <- buffer.ApplyRuntimeConfigTransition(context.Background(), 2, 60_000, nil)
	}()
	<-storage.started
	closeStarted := make(chan struct{})
	closeResult := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeResult <- buffer.Close()
	}()
	<-closeStarted
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned while the transition storage write was blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if buffer.closing.Load() {
		t.Fatal("Close marked ArrowBuffer as closing before the active transition finished")
	}

	close(storage.release)
	if err := <-transitionResult; err != nil {
		t.Fatalf("ApplyRuntimeConfigTransition: %v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("Close after transition: %v", err)
	}
}

func TestMaximumBufferedRowBytesIncludesVariableWidthValues(t *testing.T) {
	batch := &TypedColumnBatch{Data: map[string]interface{}{
		"time":  []int64{1, 2},
		"value": []string{"a", "a substantially longer value"},
	}}
	got, err := maximumBufferedRowBytes(batch, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got < uint64(len("a substantially longer value")) {
		t.Fatalf("maximumBufferedRowBytes() = %d, does not include the largest string payload", got)
	}
}

func TestValidateTransitionMemoryHeadroomFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		required  uint64
		available uint64
		known     bool
		wantErr   bool
	}{
		{name: "enough memory", required: 100, available: 100, known: true},
		{name: "insufficient memory", required: 101, available: 100, known: true, wantErr: true},
		{name: "unknown memory", required: 1, available: 0, known: false, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTransitionMemoryHeadroom(tc.required, tc.available, tc.known)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateTransitionMemoryHeadroom() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
