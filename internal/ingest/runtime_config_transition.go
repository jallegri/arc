package ingest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

const maxAgeReductionSteps = 10

var ErrRuntimeIngestTransitionBusy = errors.New("another runtime ingest change is active")

func (b *ArrowBuffer) notifyFlushActivity() {
	if b.flushActivityCh == nil {
		return
	}
	select {
	case b.flushActivityCh <- struct{}{}:
	default:
	}
}

// waitForFlushIdle is used only by runtime configuration transitions. Each
// queue/reserve admission and worker completion emits a coalesced event, so the
// control path sleeps until work changes instead of polling the ingest path.
func (b *ArrowBuffer) waitForFlushIdle(ctx context.Context) error {
	for {
		if b.pendingFlushTasks.Load() == 0 && b.elasticReserveRecords.Load() == 0 {
			return nil
		}
		if b.flushActivityCh == nil {
			return fmt.Errorf("flush activity notifications are unavailable")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.flushActivityCh:
		}
	}
}

func (b *ArrowBuffer) flushBuffersAtSizeThreshold(ctx context.Context, threshold, maxTaskRecords int) error {
	var flushErrors []error
	for shardIndex, shard := range b.shards {
		shard.mu.RLock()
		keys := make([]string, 0)
		for key, count := range shard.bufferRecordCounts {
			if count >= threshold {
				keys = append(keys, key)
			}
		}
		shard.mu.RUnlock()

		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return errors.Join(append(flushErrors, err)...)
			}
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				flushErrors = append(flushErrors, fmt.Errorf("invalid buffer key %q", key))
				continue
			}

			shard.mu.Lock()
			if shard.bufferRecordCounts[key] < threshold {
				shard.mu.Unlock()
				continue
			}
			if err := b.flushBufferLockedBounded(ctx, shard, key, parts[0], parts[1], maxTaskRecords); err != nil {
				flushErrors = append(flushErrors, fmt.Errorf("flush size-reduced buffer %q on shard %d: %w", key, shardIndex, err))
			}
			shard.mu.Unlock()
		}
	}
	return errors.Join(flushErrors...)
}

// flushBufferLockedBounded transfers an oversized buffer to bounded synchronous
// flush tasks. The caller holds shard.mu; this method returns with it held.
func (b *ArrowBuffer) flushBufferLockedBounded(ctx context.Context, shard *bufferShard, bufferKey, database, measurement string, maxTaskRecords int) error {
	batches, exists := shard.buffers[bufferKey]
	if !exists || len(batches) == 0 {
		return nil
	}
	recordCount := shard.bufferRecordCounts[bufferKey]
	startTime := shard.bufferStartTimes[bufferKey]
	schema := shard.bufferSchemas[bufferKey]
	if _, err := validateFlushRecords(batches, maxTaskRecords); err != nil {
		return err
	}
	records := append([]interface{}(nil), batches...)
	delete(shard.buffers, bufferKey)
	delete(shard.bufferStartTimes, bufferKey)
	delete(shard.bufferRecordCounts, bufferKey)
	delete(shard.bufferSchemas, bufferKey)
	shard.mu.Unlock()

	if startTime.IsZero() {
		startTime = time.Now().UTC()
	}
	completedRecords := 0
	_, flushErr := visitFlushRecordChunks(records, maxTaskRecords, func(taskRecords []interface{}, taskRecordCount int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		merged, err := b.mergeBatches(taskRecords)
		if err != nil {
			b.markFlushFailure()
			return fmt.Errorf("merge %d of %d records: %w", taskRecordCount, recordCount, err)
		}
		flushCtx, flushCancel := context.WithTimeout(ctx, b.flushTimeout)
		err = b.flushBufferLockedDataTime(flushCtx, bufferKey, database, measurement, merged, taskRecordCount, startTime)
		flushCancel()
		if err != nil {
			b.markFlushFailure()
			return fmt.Errorf("write bounded flush task: %w", err)
		}
		completedRecords += taskRecordCount
		return nil
	})
	shard.mu.Lock()
	if flushErr != nil {
		remaining, remainingCount, restoreErr := flushRecordsAfter(records, completedRecords)
		if restoreErr != nil {
			b.markFlushFailure()
			return errors.Join(flushErr, fmt.Errorf("restore unflushed buffer records: %w", restoreErr))
		}
		if remainingCount > 0 {
			// Writes may have created a new generation for this key while the
			// control path was flushing outside shard.mu. Keep the older rows
			// first and restore the exact uncompleted suffix ahead of them.
			shard.buffers[bufferKey] = append(remaining, shard.buffers[bufferKey]...)
			shard.bufferRecordCounts[bufferKey] += remainingCount
			if currentStart, ok := shard.bufferStartTimes[bufferKey]; !ok || startTime.Before(currentStart) {
				shard.bufferStartTimes[bufferKey] = startTime
			}
			if _, ok := shard.bufferSchemas[bufferKey]; !ok {
				shard.bufferSchemas[bufferKey] = schema
			}
		}
	}
	return flushErr
}

func flushRecordsAfter(records []interface{}, completedRows int) ([]interface{}, int, error) {
	if completedRows < 0 {
		return nil, 0, fmt.Errorf("completed row count must not be negative")
	}
	remaining := make([]interface{}, 0, len(records))
	rowsToSkip := completedRows
	remainingRows := 0
	for i, record := range records {
		rows, err := bufferedBatchRowCount(record)
		if err != nil {
			return nil, 0, fmt.Errorf("count batch %d: %w", i, err)
		}
		if rowsToSkip >= rows {
			rowsToSkip -= rows
			continue
		}
		if rowsToSkip > 0 {
			record, err = sliceBufferedBatch(record, rowsToSkip, rows)
			if err != nil {
				return nil, 0, fmt.Errorf("slice remaining batch %d: %w", i, err)
			}
			rows -= rowsToSkip
			rowsToSkip = 0
		}
		remaining = append(remaining, record)
		remainingRows += rows
	}
	if rowsToSkip > 0 {
		return nil, 0, fmt.Errorf("completed row count exceeds buffered row count")
	}
	return remaining, remainingRows, nil
}

func (b *ArrowBuffer) flushAgedBuffersAt(ctx context.Context, threshold time.Duration) error {
	var flushErrors []error
	now := time.Now().UTC()
	for shardIndex, shard := range b.shards {
		shard.mu.RLock()
		keys := make([]string, 0)
		for key, startTime := range shard.bufferStartTimes {
			if now.Sub(startTime) >= threshold {
				keys = append(keys, key)
			}
		}
		shard.mu.RUnlock()

		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return errors.Join(append(flushErrors, err)...)
			}
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				flushErrors = append(flushErrors, fmt.Errorf("invalid buffer key %q", key))
				continue
			}

			shard.mu.Lock()
			startTime, exists := shard.bufferStartTimes[key]
			if !exists || time.Since(startTime) < threshold {
				shard.mu.Unlock()
				continue
			}
			originalBatches := append([]interface{}(nil), shard.buffers[key]...)
			originalCount := shard.bufferRecordCounts[key]
			originalSchema := shard.bufferSchemas[key]
			flushCtx, flushCancel := context.WithTimeout(ctx, b.flushTimeout)
			err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1])
			flushCancel()
			if err != nil {
				// The normal age flusher follows Arc's existing error behavior.
				// A runtime transition must keep the buffered rows available for
				// retry if this transition-triggered direct flush cannot complete.
				shard.buffers[key] = append(originalBatches, shard.buffers[key]...)
				shard.bufferRecordCounts[key] += originalCount
				if currentStart, ok := shard.bufferStartTimes[key]; !ok || startTime.Before(currentStart) {
					shard.bufferStartTimes[key] = startTime
				}
				if _, ok := shard.bufferSchemas[key]; !ok {
					shard.bufferSchemas[key] = originalSchema
				}
				flushErrors = append(flushErrors, fmt.Errorf("flush aged buffer %q on shard %d: %w", key, shardIndex, err))
			}
			shard.mu.Unlock()
		}
	}
	return errors.Join(flushErrors...)
}

func (b *ArrowBuffer) ApplyRuntimeConfigTransition(ctx context.Context, targetSize, targetAgeMS int, onStep func(setting string, step, total, activeValue int)) error {
	if b == nil {
		return fmt.Errorf("arrow buffer is unavailable")
	}
	if !b.runtimeChangeMu.TryLock() {
		return ErrRuntimeIngestTransitionBusy
	}
	defer b.runtimeChangeMu.Unlock()
	if b.closing.Load() {
		return fmt.Errorf("cannot reconfigure ArrowBuffer while closing")
	}
	if err := validateRuntimeIngestConfig(RuntimeIngestConfig{MaxBufferSize: targetSize, MaxBufferAgeMS: targetAgeMS}); err != nil {
		return err
	}

	currentSize, currentAge := b.RuntimeConfig()
	sizePlan := sizeReductionPlan{current: currentSize, target: targetSize}
	if targetSize < currentSize {
		if !b.elasticReserveEnabled.Load() {
			return fmt.Errorf("elastic reserve must be enabled for max_buffer_size reductions")
		}
		var err error
		sizePlan, err = newSizeReductionPlan(currentSize, targetSize, int(b.elasticReserveCapacity.Load()))
		if err != nil {
			return err
		}
	}
	var ageSteps []int
	if targetAgeMS < currentAge {
		steps, err := ageReductionSteps(currentAge, targetAgeMS)
		if err != nil {
			return err
		}
		ageSteps = steps
	}

	transitionCtx, cancel := context.WithCancel(b.ctx)
	defer cancel()
	if err := b.waitForFlushIdle(transitionCtx); err != nil {
		return fmt.Errorf("wait for pre-existing flush work: %w", err)
	}
	if targetSize < currentSize && b.elasticReserveRecords.Load() != 0 {
		return fmt.Errorf("elastic reserve must be empty before a max_buffer_size reduction")
	}
	if targetSize < currentSize {
		if err := b.preflightSizeTransition(sizePlan.stepLimit); err != nil {
			return fmt.Errorf("max_buffer_size reduction preflight: %w", err)
		}
	}
	originalSize, originalAge := currentSize, currentAge
	if targetSize > currentSize || targetAgeMS > currentAge {
		activeSize := currentSize
		activeAge := currentAge
		if targetSize > currentSize {
			activeSize = targetSize
		}
		if targetAgeMS > currentAge {
			activeAge = targetAgeMS
		}
		if err := b.PatchRuntimeConfig(&activeSize, &activeAge); err != nil {
			return err
		}
	}

	rollback := func(cause error) error {
		if err := b.PatchRuntimeConfig(&originalSize, &originalAge); err != nil {
			return errors.Join(cause, fmt.Errorf("restore original runtime thresholds: %w", err))
		}
		return cause
	}

	if targetSize >= currentSize && targetAgeMS >= currentAge {
		return b.PatchRuntimeConfig(&targetSize, &targetAgeMS)
	}

	if targetSize < currentSize {
		b.elasticAdmissionLimit.Store(int64(sizePlan.stepLimit))
		defer b.elasticAdmissionLimit.Store(b.elasticReserveCapacity.Load())
		for step := 1; step <= sizePlan.stepCount; step++ {
			nextSize, ok := sizePlan.Next()
			if !ok {
				break
			}
			if onStep != nil {
				onStep("max_buffer_size", step, sizePlan.stepCount, nextSize)
			}
			failureSeq := b.flushFailureSeq.Load()
			if err := b.PatchRuntimeConfig(&nextSize, nil); err != nil {
				return rollback(fmt.Errorf("apply max_buffer_size step %d/%d: %w", step, sizePlan.stepCount, err))
			}
			if err := b.flushBuffersAtSizeThreshold(transitionCtx, nextSize, sizePlan.stepLimit); err != nil {
				return rollback(fmt.Errorf("flush max_buffer_size step %d/%d: %w", step, sizePlan.stepCount, err))
			}
			if err := b.waitForFlushIdle(transitionCtx); err != nil {
				return rollback(fmt.Errorf("wait for max_buffer_size step %d/%d: %w", step, sizePlan.stepCount, err))
			}
			if b.flushFailureSeq.Load() != failureSeq {
				return rollback(fmt.Errorf("a size-triggered flush failed during max_buffer_size step %d/%d", step, sizePlan.stepCount))
			}
		}
	}

	if len(ageSteps) > 0 {
		for step, nextAge := range ageSteps {
			stepNumber := step + 1
			if onStep != nil {
				onStep("max_buffer_age_ms", stepNumber, len(ageSteps), nextAge)
			}
			failureSeq := b.flushFailureSeq.Load()
			if err := b.PatchRuntimeConfig(nil, &nextAge); err != nil {
				return rollback(fmt.Errorf("apply max_buffer_age_ms step %d/%d: %w", stepNumber, len(ageSteps), err))
			}
			if err := b.flushAgedBuffersAt(transitionCtx, time.Duration(nextAge)*time.Millisecond); err != nil {
				return rollback(fmt.Errorf("flush max_buffer_age_ms step %d/%d: %w", stepNumber, len(ageSteps), err))
			}
			if err := b.waitForFlushIdle(transitionCtx); err != nil {
				return rollback(fmt.Errorf("wait for max_buffer_age_ms step %d/%d: %w", stepNumber, len(ageSteps), err))
			}
			if b.flushFailureSeq.Load() != failureSeq {
				return rollback(fmt.Errorf("a flush failed during max_buffer_age_ms step %d/%d", stepNumber, len(ageSteps)))
			}
		}
	}

	if targetSize < originalSize && sizePlan.current != targetSize {
		return rollback(fmt.Errorf("runtime size reduction did not reach target %d", targetSize))
	}
	if gotSize, gotAge := b.RuntimeConfig(); gotSize != targetSize || gotAge != targetAgeMS {
		return rollback(fmt.Errorf("runtime transition ended at (%d,%d), want (%d,%d)", gotSize, gotAge, targetSize, targetAgeMS))
	}
	return nil
}

// preflightSizeTransition estimates the largest bounded merge/encode workspace
// from the widest buffered row and the step limit. It runs only on the control
// path. The 4x multiplier leaves headroom for the merged batch and Arrow/Parquet
// encoding; this is deliberately conservative and fails closed if the estimate
// or available process/container memory cannot be established.
func (b *ArrowBuffer) preflightSizeTransition(maxTaskRecords int) error {
	if maxTaskRecords <= 0 {
		return fmt.Errorf("transition task record limit must be positive")
	}
	var batchesToInspect []interface{}
	for shardIndex := range b.shards {
		shard := b.shards[shardIndex]
		shard.mu.RLock()
		for _, batches := range shard.buffers {
			batchesToInspect = append(batchesToInspect, batches...)
		}
		shard.mu.RUnlock()
	}
	var maxRowBytes uint64
	for batchIndex, batch := range batchesToInspect {
		rowCount, err := bufferedBatchRowCount(batch)
		if err != nil {
			return fmt.Errorf("buffered batch %d: %w", batchIndex, err)
		}
		rowBytes, err := maximumBufferedRowBytes(batch, rowCount)
		if err != nil {
			return fmt.Errorf("buffered batch %d: %w", batchIndex, err)
		}
		if rowBytes > maxRowBytes {
			maxRowBytes = rowBytes
		}
	}
	if maxRowBytes == 0 {
		return nil
	}
	if uint64(maxTaskRecords) > math.MaxUint64/maxRowBytes {
		return fmt.Errorf("estimated transition workspace overflows")
	}
	workspace := maxRowBytes * uint64(maxTaskRecords)
	if workspace > math.MaxUint64/4 {
		return fmt.Errorf("estimated transition workspace overflows")
	}
	workspace *= 4
	available, known := availableMemoryBytes()
	return validateTransitionMemoryHeadroom(workspace, available, known)
}

func validateTransitionMemoryHeadroom(required, available uint64, known bool) error {
	if !known {
		return fmt.Errorf("cannot determine available memory for bounded flush workspace")
	}
	if required > available {
		return fmt.Errorf("estimated bounded flush workspace needs %d bytes, but only %d bytes are available", required, available)
	}
	return nil
}

func maximumBufferedRowBytes(record interface{}, rowCount int) (uint64, error) {
	var columns map[string]interface{}
	var validity map[string][]bool
	switch batch := record.(type) {
	case *TypedColumnBatch:
		columns = batch.Data
		validity = batch.Validity
	case map[string]interface{}:
		columns = batch
	default:
		return 0, fmt.Errorf("unsupported buffered batch type %T", record)
	}
	var maximum uint64
	for row := 0; row < rowCount; row++ {
		rowBytes := uint64(64) // row and column bookkeeping overhead
		for name, column := range columns {
			reflected := reflect.ValueOf(column)
			if !reflected.IsValid() || reflected.Kind() != reflect.Slice {
				return 0, fmt.Errorf("column %q has unsupported type %T", name, column)
			}
			rowBytes += 16 // map entry, slice metadata, and validity accounting
			if row < reflected.Len() {
				item := reflected.Index(row)
				switch item.Kind() {
				case reflect.String:
					rowBytes += uint64(item.Len()) + 16
				case reflect.Int64, reflect.Float64:
					rowBytes += 8
				case reflect.Bool:
					rowBytes++
				case reflect.Struct:
					rowBytes += uint64(item.Type().Size())
				default:
					return 0, fmt.Errorf("column %q has unsupported element type %s", name, item.Kind())
				}
			}
			if _, ok := validity[name]; ok {
				rowBytes++
			}
		}
		if rowBytes > maximum {
			maximum = rowBytes
		}
	}
	return maximum, nil
}

// ageReductionSteps returns monotonic intermediate thresholds for a reduction.
// The final element is always target. Increases are deliberately handled by
// the caller because they do not require a drain transition.
func ageReductionSteps(current, target int) ([]int, error) {
	if current <= 0 || target <= 0 {
		return nil, fmt.Errorf("buffer age thresholds must be greater than zero")
	}
	if target > current {
		return nil, fmt.Errorf("target buffer age %d is greater than current age %d", target, current)
	}
	if target == current {
		return nil, nil
	}

	steps := 1 + (current-1)/target
	if steps > maxAgeReductionSteps {
		steps = maxAgeReductionSteps
	}
	delta := current - target
	values := make([]int, 0, steps)
	for i := 1; i <= steps; i++ {
		// current and target have already passed runtime config validation, so
		// multiplying by at most ten remains within the supported age range.
		reduced := (delta/steps)*i + (delta%steps)*i/steps
		values = append(values, current-reduced)
	}
	values[len(values)-1] = target
	return values, nil
}

// sizeReductionSteps bounds each threshold reduction by the reserve's 75%
// transition working band. The remaining 25% is left for an unrelated burst.
type sizeReductionPlan struct {
	current   int
	target    int
	stepLimit int
	stepCount int
}

func newSizeReductionPlan(current, target, reserveCapacityRecords int) (sizeReductionPlan, error) {
	if current <= 0 || target <= 0 {
		return sizeReductionPlan{}, fmt.Errorf("buffer size thresholds must be greater than zero")
	}
	if target > current {
		return sizeReductionPlan{}, fmt.Errorf("target buffer size %d is greater than current size %d", target, current)
	}
	if target == current {
		return sizeReductionPlan{current: current, target: target}, nil
	}
	if reserveCapacityRecords <= 0 {
		return sizeReductionPlan{}, fmt.Errorf("elastic reserve must be enabled with positive record capacity for a buffer size reduction")
	}

	// Compute floor(3*capacity/4) without overflowing capacity*3.
	workingBand := reserveCapacityRecords/4*3 + reserveCapacityRecords%4*3/4
	if workingBand <= 0 {
		return sizeReductionPlan{}, fmt.Errorf("elastic reserve capacity %d is too small to provide a transition working band", reserveCapacityRecords)
	}

	delta := current - target
	stepCount := 1 + (delta-1)/workingBand
	return sizeReductionPlan{current: current, target: target, stepLimit: workingBand, stepCount: stepCount}, nil
}

// Next advances the plan without allocating an array proportional to the
// number of steps. A small reserve can therefore never cause an enormous
// preflight allocation before the transition starts.
func (p *sizeReductionPlan) Next() (int, bool) {
	if p.current <= p.target {
		return p.target, false
	}
	if remaining := p.current - p.target; remaining > p.stepLimit {
		p.current -= p.stepLimit
	} else {
		p.current = p.target
	}
	return p.current, true
}

// visitFlushRecordChunks emits buffered batches as flush tasks with at most
// maxRecords rows. It is intended for the reconfiguration control path; normal
// ingestion does not call it. Full batches are transferred by reference, while
// only boundary slices are copied. The callback keeps task-list memory bounded
// even when a very small reserve requires many size-reduction steps.
func visitFlushRecordChunks(records []interface{}, maxRecords int, visit func([]interface{}, int) error) (int, error) {
	if visit == nil {
		return 0, fmt.Errorf("flush task visitor is required")
	}
	totalRecords, err := validateFlushRecords(records, maxRecords)
	if err != nil {
		return 0, err
	}
	var task []interface{}
	taskRecords := 0
	for i, record := range records {
		rows, err := bufferedBatchRowCount(record)
		if err != nil {
			return totalRecords, fmt.Errorf("count buffered batch %d during split: %w", i, err)
		}
		for start := 0; start < rows; {
			count := maxRecords - taskRecords
			if remaining := rows - start; remaining < count {
				count = remaining
			}
			piece := record
			if start != 0 || count != rows {
				piece, err = sliceBufferedBatch(record, start, start+count)
				if err != nil {
					return totalRecords, fmt.Errorf("slice buffered batch %d rows [%d:%d]: %w", i, start, start+count, err)
				}
			}
			task = append(task, piece)
			taskRecords += count
			start += count
			if taskRecords == maxRecords {
				if err := visit(task, taskRecords); err != nil {
					return totalRecords, err
				}
				task = nil
				taskRecords = 0
			}
		}
	}
	if len(task) > 0 {
		if err := visit(task, taskRecords); err != nil {
			return totalRecords, err
		}
	}
	return totalRecords, nil
}

func validateFlushRecords(records []interface{}, maxRecords int) (int, error) {
	if maxRecords <= 0 {
		return 0, fmt.Errorf("maximum flush task size must be greater than zero")
	}
	if len(records) == 0 {
		return 0, fmt.Errorf("cannot split an empty flush buffer")
	}
	totalRecords := 0
	for i, record := range records {
		if err := validateBufferedBatchSlicable(record); err != nil {
			return 0, fmt.Errorf("validate buffered batch %d: %w", i, err)
		}
		count, err := bufferedBatchRowCount(record)
		if err != nil {
			return 0, fmt.Errorf("count buffered batch %d: %w", i, err)
		}
		if count <= 0 {
			return 0, fmt.Errorf("buffered batch %d has no rows", i)
		}
		if totalRecords > int(^uint(0)>>1)-count {
			return 0, fmt.Errorf("buffered record count overflows int")
		}
		totalRecords += count
	}
	return totalRecords, nil
}

func bufferedBatchRowCount(record interface{}) (int, error) {
	var columns map[string]interface{}
	switch batch := record.(type) {
	case *TypedColumnBatch:
		if batch == nil {
			return 0, fmt.Errorf("nil typed batch")
		}
		columns = batch.Data
	case map[string]interface{}:
		columns = batch
	default:
		return 0, fmt.Errorf("unsupported buffered batch type %T", record)
	}
	timeColumn, ok := columns["time"]
	if !ok || timeColumn == nil {
		return 0, fmt.Errorf("buffered batch has no time column")
	}
	times, ok := timeColumn.([]int64)
	if !ok {
		return 0, fmt.Errorf("time column has unsupported type %T", timeColumn)
	}
	return len(times), nil
}

func validateBufferedBatchSlicable(record interface{}) error {
	var columns map[string]interface{}
	switch batch := record.(type) {
	case *TypedColumnBatch:
		if batch == nil {
			return fmt.Errorf("nil typed batch")
		}
		columns = batch.Data
	case map[string]interface{}:
		columns = batch
	default:
		return fmt.Errorf("unsupported buffered batch type %T", record)
	}
	for name, column := range columns {
		switch column.(type) {
		case []int64, []float64, []string, []bool, []decimal128.Num:
		default:
			return fmt.Errorf("column %q has unsupported type %T", name, column)
		}
	}
	return nil
}

func sliceBufferedBatch(record interface{}, start, end int) (interface{}, error) {
	var columns map[string]interface{}
	var source *TypedColumnBatch
	switch batch := record.(type) {
	case *TypedColumnBatch:
		source = batch
		columns = batch.Data
	case map[string]interface{}:
		columns = batch
	default:
		return nil, fmt.Errorf("unsupported buffered batch type %T", record)
	}

	slicedColumns := make(map[string]interface{}, len(columns))
	for name, column := range columns {
		sliced, err := sliceReflectValue(column, start, end)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", name, err)
		}
		slicedColumns[name] = sliced
	}
	if source == nil {
		return slicedColumns, nil
	}

	var slicedValidity map[string][]bool
	if source.Validity != nil {
		slicedValidity = make(map[string][]bool, len(source.Validity))
		for name, validity := range source.Validity {
			if validity == nil {
				slicedValidity[name] = nil
				continue
			}
			if end <= len(validity) {
				slicedValidity[name] = validity[start:end]
				continue
			}
			sliced := make([]bool, end-start)
			for i := range sliced {
				index := start + i
				if index < len(validity) {
					sliced[i] = validity[index]
				}
			}
			slicedValidity[name] = sliced
		}
	}
	return &TypedColumnBatch{
		Data:       slicedColumns,
		Validity:   slicedValidity,
		TagColumns: append([]string(nil), source.TagColumns...),
		DedupTime:  source.DedupTime,
		Signature:  source.Signature,
	}, nil
}

func sliceReflectValue(value interface{}, start, end int) (interface{}, error) {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || reflected.Kind() != reflect.Slice {
		return nil, fmt.Errorf("value has unsupported type %T", value)
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid row range [%d:%d]", start, end)
	}
	if end <= reflected.Len() {
		return reflected.Slice(start, end).Interface(), nil
	}
	sliced := reflect.MakeSlice(reflected.Type(), end-start, end-start)
	for i := 0; i < end-start; i++ {
		index := start + i
		if index < reflected.Len() {
			sliced.Index(i).Set(reflected.Index(index))
		}
	}
	return sliced.Interface(), nil
}
