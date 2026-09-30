package ingest

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// gatedStorageBackend holds the first storage write until the test releases
// it. This models a temporary storage stall while keeping the queue-full
// interval deterministic.
type gatedStorageBackend struct {
	started    chan struct{}
	release    chan struct{}
	releaseOne sync.Once
	writeOnce  sync.Once
	mu         sync.Mutex
	writes     [][]byte
}

func newGatedStorageBackend() *gatedStorageBackend {
	return &gatedStorageBackend{started: make(chan struct{}), release: make(chan struct{})}
}

func (s *gatedStorageBackend) Write(ctx context.Context, _ string, data []byte) error {
	s.writeOnce.Do(func() { close(s.started) })
	select {
	case <-s.release:
		s.mu.Lock()
		copyOfData := append([]byte(nil), data...)
		s.writes = append(s.writes, copyOfData)
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *gatedStorageBackend) releaseGate() {
	s.releaseOne.Do(func() { close(s.release) })
}

func (s *gatedStorageBackend) storedFiles() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	files := make([][]byte, len(s.writes))
	for i := range s.writes {
		files[i] = append([]byte(nil), s.writes[i]...)
	}
	return files
}

func readSequenceIDs(t *testing.T, files [][]byte) []string {
	t.Helper()
	var ids []string
	for fileIndex, data := range files {
		parquetReader, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("open stored parquet file %d: %v", fileIndex, err)
		}
		arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, nil)
		if err != nil {
			_ = parquetReader.Close()
			t.Fatalf("create Arrow reader for stored parquet file %d: %v", fileIndex, err)
		}
		table, err := arrowReader.ReadTable(context.Background())
		if err != nil {
			_ = parquetReader.Close()
			t.Fatalf("read stored parquet file %d: %v", fileIndex, err)
		}
		indices := table.Schema().FieldIndices("sequence")
		if len(indices) != 1 {
			table.Release()
			_ = parquetReader.Close()
			t.Fatalf("stored parquet file %d has %d sequence columns, want 1", fileIndex, len(indices))
		}
		for _, chunk := range table.Column(indices[0]).Data().Chunks() {
			strings, ok := chunk.(*array.String)
			if !ok {
				table.Release()
				_ = parquetReader.Close()
				t.Fatalf("stored parquet file %d sequence chunk has type %T, want *array.String", fileIndex, chunk)
			}
			for row := 0; row < strings.Len(); row++ {
				ids = append(ids, strings.Value(row))
			}
		}
		table.Release()
		_ = parquetReader.Close()
	}
	return ids
}

func sequenceColumns(batch, rows int) map[string][]interface{} {
	columns := map[string][]interface{}{
		"time":     make([]interface{}, rows),
		"sequence": make([]interface{}, rows),
		"value":    make([]interface{}, rows),
	}
	base := time.Now().UnixMicro() + int64(batch*rows*1000)
	for i := 0; i < rows; i++ {
		columns["time"][i] = base + int64(i)*1000
		columns["sequence"][i] = strconv.Itoa(batch*rows + i)
		columns["value"][i] = float64(batch*rows + i)
	}
	return columns
}

func (s *gatedStorageBackend) WriteReader(ctx context.Context, _ string, r io.Reader, _ int64) error {
	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}
	return s.Write(ctx, "", nil)
}

func (*gatedStorageBackend) Read(context.Context, string) ([]byte, error)                 { return nil, nil }
func (*gatedStorageBackend) ReadTo(context.Context, string, io.Writer) error              { return nil }
func (*gatedStorageBackend) List(context.Context, string) ([]string, error)               { return nil, nil }
func (*gatedStorageBackend) Delete(context.Context, string) error                         { return nil }
func (*gatedStorageBackend) Exists(context.Context, string) (bool, error)                 { return false, nil }
func (*gatedStorageBackend) Close() error                                                 { return nil }
func (*gatedStorageBackend) Type() string                                                 { return "gated-test" }
func (*gatedStorageBackend) ConfigJSON() string                                           { return "{}" }
func (*gatedStorageBackend) ReadToAt(context.Context, string, io.Writer, int64) error     { return nil }
func (*gatedStorageBackend) StatFile(context.Context, string) (int64, error)              { return -1, nil }
func (*gatedStorageBackend) AppendReader(context.Context, string, io.Reader, int64) error { return nil }

func TestElasticReserveDrainsTemporaryStorageBacklog(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			store := newGatedStorageBackend()
			cfg := &config.IngestConfig{
				MaxBufferSize:   500,
				MaxBufferAgeMS:  60_000,
				Compression:     "snappy",
				UseDictionary:   true,
				WriteStatistics: true,
				DataPageVersion: "2.0",
				FlushWorkers:    1,
				FlushQueueSize:  1,
				ShardCount:      1,
			}
			buf := NewArrowBuffer(cfg, store, zerolog.Nop())
			t.Cleanup(func() {
				store.releaseGate()
				_ = buf.Close()
			})

			if enabled {
				if err := buf.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 4_000}); err != nil {
					t.Fatalf("enable reserve: %v", err)
				}
			}

			const batches = 8
			const recordsPerBatch = 500
			if err := buf.WriteColumnarDirect(context.Background(), "db", "m", sequenceColumns(0, recordsPerBatch)); err != nil {
				t.Fatalf("write first batch: %v", err)
			}
			select {
			case <-store.started:
			case <-time.After(5 * time.Second):
				t.Fatal("first storage write did not reach the gate")
			}

			before := metrics.Get().Snapshot()
			for i := 1; i < batches; i++ {
				if err := buf.WriteColumnarDirect(context.Background(), "db", "m", sequenceColumns(i, recordsPerBatch)); err != nil {
					t.Fatalf("write batch %d: %v", i+1, err)
				}
			}
			queued := metrics.Get().Snapshot()
			store.releaseGate()

			deadline := time.Now().Add(10 * time.Second)
			for buf.pendingFlushTasks.Load() != 0 || buf.queueDepth.Load() != 0 || buf.ElasticReserveUsedRecords() != 0 {
				if time.Now().After(deadline) {
					t.Fatalf("flush backlog did not drain: pending=%d queue=%d reserve=%d", buf.pendingFlushTasks.Load(), buf.queueDepth.Load(), buf.ElasticReserveUsedRecords())
				}
				time.Sleep(time.Millisecond)
			}

			stats := buf.GetStats()
			buffered := stats["total_records_buffered"].(int64)
			written := stats["total_records_written"].(int64)
			storedIDs := readSequenceIDs(t, store.storedFiles())
			full := queued["buffer_flush_queue_full_records_total"].(int64) - before["buffer_flush_queue_full_records_total"].(int64)
			admitted := queued["buffer_elastic_reserve_admissions_total"].(int64) - before["buffer_elastic_reserve_admissions_total"].(int64)
			unprotected := queued["buffer_unprotected_overflow_records_total"].(int64) - before["buffer_unprotected_overflow_records_total"].(int64)

			if full == 0 {
				t.Fatal("test did not force the flush queue to fill")
			}
			if buffered != batches*recordsPerBatch {
				t.Fatalf("accepted records = %d, want %d", buffered, batches*recordsPerBatch)
			}
			if enabled {
				if admitted == 0 || unprotected != 0 {
					t.Fatalf("reserve flow metrics: admitted=%d unprotected=%d, want admissions and no unprotected overflow", admitted, unprotected)
				}
				if written != buffered {
					t.Fatalf("stored records = %d, accepted = %d with reserve enabled", written, buffered)
				}
				assertExactlyOnceSequenceIDs(t, storedIDs, batches*recordsPerBatch)
				if used := buf.ElasticReserveUsedRecords(); used != 0 {
					t.Fatalf("reserve occupancy after recovery = %d, want 0", used)
				}
			} else {
				if unprotected == 0 {
					t.Fatal("reserve-disabled control did not report unprotected overflow")
				}
				if written >= buffered {
					t.Fatalf("control case stored %d of %d accepted records; expected the stalled queue to expose unprotected loss", written, buffered)
				}
				assertUniqueKnownSequenceIDs(t, storedIDs, batches*recordsPerBatch)
				if got, want := int64(batches*recordsPerBatch-len(storedIDs)), unprotected; got != want {
					t.Fatalf("missing sequence count = %d, unprotected overflow metric = %d", got, want)
				}
			}
		})
	}
}

func assertExactlyOnceSequenceIDs(t *testing.T, got []string, wantCount int) {
	t.Helper()
	assertUniqueKnownSequenceIDs(t, got, wantCount)
	if len(got) != wantCount {
		t.Fatalf("stored sequence count = %d, want %d", len(got), wantCount)
	}
}

func assertUniqueKnownSequenceIDs(t *testing.T, got []string, totalExpected int) {
	t.Helper()
	seen := make(map[string]struct{}, len(got))
	for _, id := range got {
		sequence, err := strconv.Atoi(id)
		if err != nil || sequence < 0 || sequence >= totalExpected {
			t.Fatalf("stored unexpected sequence ID %q", id)
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("stored duplicate sequence ID %q", id)
		}
		seen[id] = struct{}{}
	}
}
