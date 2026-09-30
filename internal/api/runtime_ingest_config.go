package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// IngestRuntimeConfig exposes the ingest buffer thresholds that can be
// changed safely while the process is running.
type IngestRuntimeConfig interface {
	RuntimeConfig() (maxBufferSize int, maxBufferAgeMS int)
	PatchRuntimeConfig(maxBufferSize, maxBufferAgeMS *int) error
	ApplyRuntimeConfigTransition(ctx context.Context, targetSize, targetAgeMS int, onStep func(setting string, step, total, activeValue int)) error
}

// RuntimeIngestConfigHandler serves process-local ingest buffer settings.
type RuntimeIngestConfigHandler struct {
	buffer        IngestRuntimeConfig
	store         *ingest.RuntimeIngestConfigStore
	startup       ingest.RuntimeIngestConfig
	runtimeOnly   bool
	authManager   *auth.AuthManager
	logger        zerolog.Logger
	mutationMu    sync.Mutex
	statusMu      sync.RWMutex
	status        *runtimeIngestTransitionResponse
	transitionSeq atomic.Uint64
}

// NewRuntimeIngestConfigHandler creates the runtime ingest configuration API.
func NewRuntimeIngestConfigHandler(buffer IngestRuntimeConfig, store *ingest.RuntimeIngestConfigStore, startup ingest.RuntimeIngestConfig, authManager *auth.AuthManager, logger zerolog.Logger) *RuntimeIngestConfigHandler {
	return &RuntimeIngestConfigHandler{
		buffer:      buffer,
		store:       store,
		startup:     startup,
		authManager: authManager,
		logger:      logger.With().Str("component", "runtime-ingest-config").Logger(),
	}
}

// RegisterRoutes registers admin-only runtime ingest configuration endpoints.
func (h *RuntimeIngestConfigHandler) RegisterRoutes(app *fiber.App) {
	path := "/api/v1/config/runtime/ingest"
	if h.authManager != nil {
		app.Get(path, auth.RequireAdmin(h.authManager), h.handleGet)
		app.Patch(path, auth.RequireAdmin(h.authManager), h.handlePatch)
		app.Delete(path, auth.RequireAdmin(h.authManager), h.handleDelete)
		app.Get(path+"/transition", auth.RequireAdmin(h.authManager), h.handleGetTransition)
		return
	}
	app.Get(path, h.handleGet)
	app.Patch(path, h.handlePatch)
	app.Delete(path, h.handleDelete)
	app.Get(path+"/transition", h.handleGetTransition)
}

type runtimeIngestConfigResponse struct {
	MaxBufferSize  int                              `json:"max_buffer_size"`
	MaxBufferAgeMS int                              `json:"max_buffer_age_ms"`
	Scope          string                           `json:"scope"`
	Persistent     bool                             `json:"persistent"`
	Source         string                           `json:"source"`
	Transition     *runtimeIngestTransitionResponse `json:"transition,omitempty"`
}

type runtimeIngestTransitionResponse struct {
	ID             string    `json:"transition_id"`
	State          string    `json:"state"`
	Setting        string    `json:"setting,omitempty"`
	RequestedSize  int       `json:"requested_max_buffer_size"`
	RequestedAgeMS int       `json:"requested_max_buffer_age_ms"`
	ActiveValue    int       `json:"active_step_value,omitempty"`
	Step           int       `json:"step,omitempty"`
	TotalSteps     int       `json:"total_steps,omitempty"`
	WaitReason     string    `json:"wait_reason,omitempty"`
	Error          string    `json:"error,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (h *RuntimeIngestConfigHandler) handleGet(c *fiber.Ctx) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest configuration is unavailable"})
	}
	maxSize, maxAge := h.buffer.RuntimeConfig()
	_, persistent, err := h.store.Load()
	if err != nil {
		h.logger.Error().Err(err).Msg("Could not read persisted runtime ingest configuration")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not read runtime ingest configuration"})
	}
	source := "startup_config"
	if persistent {
		source = "persistent_override"
	} else if h.runtimeOnly {
		source = "runtime_override"
	}
	return c.JSON(runtimeIngestConfigResponse{
		MaxBufferSize:  maxSize,
		MaxBufferAgeMS: maxAge,
		Scope:          "current_process",
		Persistent:     persistent,
		Source:         source,
		Transition:     h.transitionStatus(),
	})
}

func (h *RuntimeIngestConfigHandler) handleGetTransition(c *fiber.Ctx) error {
	status := h.transitionStatus()
	if status == nil {
		status = &runtimeIngestTransitionResponse{State: "idle", UpdatedAt: time.Now().UTC()}
	}
	return c.JSON(status)
}

func (h *RuntimeIngestConfigHandler) transitionStatus() *runtimeIngestTransitionResponse {
	h.statusMu.RLock()
	defer h.statusMu.RUnlock()
	if h.status == nil {
		return nil
	}
	copy := *h.status
	return &copy
}

func (h *RuntimeIngestConfigHandler) restoreTransitionStatus(id string, previous *runtimeIngestTransitionResponse) {
	h.statusMu.Lock()
	defer h.statusMu.Unlock()
	if h.status != nil && h.status.ID == id {
		h.status = previous
	}
}

func (h *RuntimeIngestConfigHandler) startTransition(size, age int) string {
	id := fmt.Sprintf("ingest-%d-%d", time.Now().UTC().UnixNano(), h.transitionSeq.Add(1))
	h.statusMu.Lock()
	h.status = &runtimeIngestTransitionResponse{
		ID:             id,
		State:          "preflight",
		RequestedSize:  size,
		RequestedAgeMS: age,
		WaitReason:     "validating target and waiting for existing flush work",
		UpdatedAt:      time.Now().UTC(),
	}
	h.statusMu.Unlock()
	return id
}

func (h *RuntimeIngestConfigHandler) updateTransitionStep(id, setting string, step, total, value int) {
	h.statusMu.Lock()
	defer h.statusMu.Unlock()
	if h.status == nil || h.status.ID != id {
		return
	}
	h.status.State = "running"
	h.status.Setting = setting
	h.status.Step = step
	h.status.TotalSteps = total
	h.status.ActiveValue = value
	if setting == "max_buffer_size" {
		h.status.WaitReason = "waiting for size-triggered flush work and elastic reserve to drain"
	} else {
		h.status.WaitReason = "waiting for the direct age-flush pass and flush workers"
	}
	h.status.UpdatedAt = time.Now().UTC()
}

func (h *RuntimeIngestConfigHandler) finishTransition(id string, err error) {
	activeSize, activeAge := h.buffer.RuntimeConfig()
	h.statusMu.Lock()
	defer h.statusMu.Unlock()
	if h.status == nil || h.status.ID != id {
		return
	}
	h.status.WaitReason = ""
	h.status.UpdatedAt = time.Now().UTC()
	switch h.status.Setting {
	case "max_buffer_size":
		h.status.ActiveValue = activeSize
	case "max_buffer_age_ms":
		h.status.ActiveValue = activeAge
	}
	if err != nil {
		h.status.State = "failed"
		h.status.Error = err.Error()
	} else {
		h.status.State = "complete"
		h.status.Error = ""
		h.status.RequestedSize = activeSize
		h.status.RequestedAgeMS = activeAge
	}
}

type runtimeIngestConfigPatch struct {
	MaxBufferSize  *int  `json:"max_buffer_size"`
	MaxBufferAgeMS *int  `json:"max_buffer_age_ms"`
	Persistent     *bool `json:"persistent"`
}

func (h *RuntimeIngestConfigHandler) handlePatch(c *fiber.Ctx) error {
	if !h.mutationMu.TryLock() {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "another runtime ingest configuration change is active"})
	}
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest configuration is unavailable"})
	}
	var patch runtimeIngestConfigPatch
	if err := c.BodyParser(&patch); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}
	persist := true // Preserve the existing PATCH behavior when the field is omitted.
	if patch.Persistent != nil {
		persist = *patch.Persistent
	}
	if patch.MaxBufferSize == nil && patch.MaxBufferAgeMS == nil && !persist {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "provide max_buffer_size and/or max_buffer_age_ms"})
	}

	oldSize, oldAge := h.buffer.RuntimeConfig()
	size, age := oldSize, oldAge
	if patch.MaxBufferSize != nil {
		size = *patch.MaxBufferSize
	}
	if patch.MaxBufferAgeMS != nil {
		age = *patch.MaxBufferAgeMS
	}
	previousTransition := h.transitionStatus()
	transitionID := h.startTransition(size, age)
	if err := h.buffer.ApplyRuntimeConfigTransition(c.UserContext(), size, age, func(setting string, step, total, value int) {
		h.updateTransitionStep(transitionID, setting, step, total, value)
	}); err != nil {
		if errors.Is(err, ingest.ErrRuntimeIngestTransitionBusy) {
			h.restoreTransitionStatus(transitionID, previousTransition)
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": err.Error()})
		}
		h.finishTransition(transitionID, err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error(), "transition": h.transitionStatus()})
	}
	var storeErr error
	if persist {
		storeErr = h.store.Save(ingest.RuntimeIngestConfig{MaxBufferSize: size, MaxBufferAgeMS: age})
	} else {
		storeErr = h.store.Delete()
	}
	if storeErr != nil {
		rollbackErr := h.buffer.PatchRuntimeConfig(&oldSize, &oldAge)
		h.logger.Error().Err(storeErr).Interface("rollback_error", rollbackErr).Msg("Could not save runtime ingest settings; reverted live settings")
		persistErr := fmt.Errorf("could not persist runtime ingest configuration: %w", storeErr)
		h.finishTransition(transitionID, errors.Join(persistErr, rollbackErr))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not persist runtime ingest configuration", "transition": h.transitionStatus()})
	}
	h.runtimeOnly = !persist
	maxSize, maxAge := h.buffer.RuntimeConfig()
	h.finishTransition(transitionID, nil)
	source := "runtime_override"
	if persist {
		source = "persistent_override"
	}

	h.logger.Info().Int("max_buffer_size", maxSize).Int("max_buffer_age_ms", maxAge).Bool("persistent", persist).Msg("Updated live ingest buffer settings")
	return c.JSON(runtimeIngestConfigResponse{
		MaxBufferSize:  maxSize,
		MaxBufferAgeMS: maxAge,
		Scope:          "current_process",
		Persistent:     persist,
		Source:         source,
		Transition:     h.transitionStatus(),
	})
}

func (h *RuntimeIngestConfigHandler) handleDelete(c *fiber.Ctx) error {
	if !h.mutationMu.TryLock() {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "another runtime ingest configuration change is active"})
	}
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest configuration is unavailable"})
	}
	oldSize, oldAge := h.buffer.RuntimeConfig()
	previousTransition := h.transitionStatus()
	transitionID := h.startTransition(h.startup.MaxBufferSize, h.startup.MaxBufferAgeMS)
	if err := h.buffer.ApplyRuntimeConfigTransition(c.UserContext(), h.startup.MaxBufferSize, h.startup.MaxBufferAgeMS, func(setting string, step, total, value int) {
		h.updateTransitionStep(transitionID, setting, step, total, value)
	}); err != nil {
		if errors.Is(err, ingest.ErrRuntimeIngestTransitionBusy) {
			h.restoreTransitionStatus(transitionID, previousTransition)
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": err.Error()})
		}
		h.finishTransition(transitionID, err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("could not restore startup ingest configuration: %v", err), "transition": h.transitionStatus()})
	}
	if err := h.store.Delete(); err != nil {
		rollbackErr := h.buffer.PatchRuntimeConfig(&oldSize, &oldAge)
		h.logger.Error().Err(err).Interface("rollback_error", rollbackErr).Msg("Could not clear persisted runtime ingest settings; restored live settings")
		persistErr := fmt.Errorf("could not clear persisted runtime ingest configuration: %w", err)
		h.finishTransition(transitionID, errors.Join(persistErr, rollbackErr))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not clear persisted runtime ingest configuration", "transition": h.transitionStatus()})
	}
	h.runtimeOnly = false
	h.finishTransition(transitionID, nil)
	h.logger.Info().Int("max_buffer_size", h.startup.MaxBufferSize).Int("max_buffer_age_ms", h.startup.MaxBufferAgeMS).Msg("Restored startup ingest buffer settings")
	return c.JSON(runtimeIngestConfigResponse{
		MaxBufferSize:  h.startup.MaxBufferSize,
		MaxBufferAgeMS: h.startup.MaxBufferAgeMS,
		Scope:          "current_process",
		Persistent:     false,
		Source:         "startup_config",
		Transition:     h.transitionStatus(),
	})
}
