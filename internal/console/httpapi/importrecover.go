package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/EsDmitrii/kconmon-ng/internal/console/authz"
	"github.com/EsDmitrii/kconmon-ng/internal/console/store"
)

// ---------------------------------------------------------------------------
// Section order
// ---------------------------------------------------------------------------

// Section ordinals pin the dependency order importer.run and the recoverable runner share: a
// definition may point at a target and a schedule at a definition.
const (
	secTargets = iota
	secDefinitions
	secSchedules
	secAlertRules
	secWebhooks
	secMaintenance
	secRBAC
	importSectionCount
)

// importSectionNames are the progress labels, in section order.
var importSectionNames = []string{
	"targets", "checkDefinitions", "checkSchedules", "alertRules",
	"webhooks", "maintenanceWindows", "rbac",
}

// ---------------------------------------------------------------------------
// intSet: per-section processed-item set
// ---------------------------------------------------------------------------

// intSet is a set of item indices, JSON-encoded as a sorted integer array: a checkpoint's shape
// stays small and diffable.
type intSet map[int]struct{}

func newIntSet() intSet { return intSet{} }

func (s intSet) has(i int) bool { _, ok := s[i]; return ok }
func (s intSet) add(i int)      { s[i] = struct{}{} }
func (s intSet) size() int      { return len(s) }

func (s intSet) MarshalJSON() ([]byte, error) {
	vals := make([]int, 0, len(s))
	for k := range s {
		vals = append(vals, k)
	}
	slices.Sort(vals)
	return json.Marshal(vals)
}

func (s *intSet) UnmarshalJSON(raw []byte) error {
	if strings.TrimSpace(string(raw)) == "null" {
		*s = newIntSet()
		return nil
	}
	var vals []int
	if err := json.Unmarshal(raw, &vals); err != nil {
		return err
	}
	out := make(intSet, len(vals))
	for _, v := range vals {
		out[v] = struct{}{}
	}
	*s = out
	return nil
}

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

// importCheckpoint is the durable progress record of ONE logical import. It is the whole state a
// process taking over after a restart needs: the next section, the processed-item sets, the
// partial ledger, the ID remap, and the once-only side-effect gates.
type importCheckpoint struct {
	// Section is the NEXT section ordinal to execute; importSectionCount means every section ran.
	Section int `json:"section"`
	// Done[sec] holds the item indices already durably processed; those items are never re-run.
	Done []intSet `json:"done"`
	// Response is the partial ledger while running and the complete ledger at completion.
	Response importResponse `json:"response"`

	// ID remap, persisted so a takeover holds the exact bundle->destination map the prior executor
	// built, including identity mappings of rows this console already held.
	TargetIDs map[string]string `json:"targetIds"`
	DefIDs    map[string]string `json:"defIds"`
	DefNames  map[string]string `json:"defNames"`
	// DefShapes is destination definition id -> [checkType, destinationKind].
	DefShapes map[string][2]string `json:"defShapes"`

	// RBACBindingsRecorded says the skipped-bindings outcome already entered the ledger, so
	// resuming the RBAC section cannot count or warn it twice.
	RBACBindingsRecorded bool `json:"rbacBindingsRecorded"`

	// Side-effect gates. A runtime side effect is performed once per logical import, immediately
	// after the first successfully checkpointed create/update item; a false gate at section end
	// performs it once more (the crash window between the side effect and the gate's checkpoint).
	AlertSyncKicked bool `json:"alertSyncKicked"`
	RBACNotified    bool `json:"rbacNotified"`
}

// newImportCheckpoint returns a checkpoint at the start: every collection initialized (so errors
// and warnings marshal as [] not null) and every map allocated.
func newImportCheckpoint() *importCheckpoint {
	done := make([]intSet, importSectionCount)
	for i := range done {
		done[i] = newIntSet()
	}
	return &importCheckpoint{
		Section: secTargets, Done: done,
		Response: importResponse{
			DryRun:             false,
			Targets:            newImportCollectionResult(),
			CheckDefinitions:   newImportCollectionResult(),
			CheckSchedules:     newImportCollectionResult(),
			AlertRules:         newImportCollectionResult(),
			Webhooks:           newImportCollectionResult(),
			MaintenanceWindows: newImportCollectionResult(),
			RBACRoles:          newImportCollectionResult(),
			RBACBindings:       newImportCollectionResult(),
		},
		TargetIDs: map[string]string{}, DefIDs: map[string]string{},
		DefNames: map[string]string{}, DefShapes: map[string][2]string{},
	}
}

// importProgress is the recognizable progress description in the 202 in-progress response.
type importProgress struct {
	// Section is the section currently being (or next to be) processed.
	Section string `json:"section"`
	// SectionIndex is its zero-based ordinal.
	SectionIndex int `json:"sectionIndex"`
	// CompletedItems and TotalItems count bundle items, skipped and failed included.
	CompletedItems int `json:"completedItems"`
	TotalItems     int `json:"totalItems"`
}

// rbacRoleCount reports how many roles the RBAC section carries (0 when absent).
func rbacRoleCount(section *exportRBAC) int {
	if section == nil {
		return 0
	}
	return len(section.Roles)
}

// progress computes the current progress against the bundle this operation is pinned to.
func (cp *importCheckpoint) progress(bundle *exportBundle) importProgress {
	totals := [importSectionCount]int{
		len(bundle.Targets), len(bundle.CheckDefinitions), len(bundle.CheckSchedules),
		len(bundle.AlertRules), len(bundle.Webhooks), len(bundle.MaintenanceWindows),
		rbacRoleCount(bundle.RBAC),
	}
	total, completed := 0, 0
	for sec, n := range totals {
		total += n
		if sec < len(cp.Done) {
			completed += cp.Done[sec].size()
		}
	}
	sec := cp.Section
	if sec < 0 {
		sec = 0
	}
	if sec >= importSectionCount {
		sec = importSectionCount - 1
	}
	return importProgress{
		Section: importSectionNames[sec], SectionIndex: cp.Section,
		CompletedItems: completed, TotalItems: total,
	}
}

// ---------------------------------------------------------------------------
// Durable operation seam
// ---------------------------------------------------------------------------

// ImportOperations is the seam over import_operations; in production *store.DB satisfies it.
type ImportOperations interface {
	CreateImportOperation(ctx context.Context, id, fingerprint, startedBy, executorID string, checkpoint json.RawMessage) (store.ImportOperation, error)
	GetImportOperation(ctx context.Context, id string) (store.ImportOperation, error)
	ClaimImportOperation(ctx context.Context, id, executorID string, staleBefore time.Time) (store.ImportOperation, bool, error)
	SaveImportCheckpoint(ctx context.Context, id, executorID string, checkpoint json.RawMessage) error
	CompleteImportOperation(ctx context.Context, id, executorID string, result json.RawMessage) error
}

var _ ImportOperations = (*store.DB)(nil)

// ---------------------------------------------------------------------------
// Constants and fingerprint
// ---------------------------------------------------------------------------

// Recoverable-import status strings (importResponse.Status; distinct from the table status).
const (
	importStatusInProgress = "in-progress"
	importStatusSucceeded  = "succeeded"
)

const (
	importOperationIDMaxLen = 128
	// importTakeoverTimeout is the heartbeat window: an operation whose heartbeat is older is
	// presumed to have no live executor and may be claimed.
	importTakeoverTimeout = 45 * time.Second
	// importJoinWait bounds how long a duplicate request waits for a same-process execution before
	// it answers 202 in-progress.
	importJoinWait = 10 * time.Second
)

// errImportFenced marks that another executor took ownership while this one was applying items;
// the request then reads the row the new owner advances.
var errImportFenced = errors.New("httpapi: import executor was fenced by another process")

// fingerprintBundle hashes the canonical encoding of the bundle so reusing an operationId with a
// different bundle is recognizable.
func fingerprintBundle(b *exportBundle) string {
	raw, err := json.Marshal(b)
	if err != nil {
		// exportBundle is built of marshallable fields; a failure here is a programming error.
		panic(fmt.Errorf("httpapi: fingerprint bundle: %w", err))
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// operationIDProblem validates the caller's identifier: trimmed, bounded, and free of control
// characters (it is stored in a text column but never itself audited).
func operationIDProblem(id string) string {
	id = strings.TrimSpace(id)
	switch {
	case id == "":
		return "operationId must not be empty"
	case len(id) > importOperationIDMaxLen:
		return "operationId is " + strconv.Itoa(len(id)) + " bytes, limit is " + strconv.Itoa(importOperationIDMaxLen)
	case invalidParamText(id):
		return "operationId must be valid UTF-8 without control characters"
	}
	return ""
}

// ---------------------------------------------------------------------------
// In-process active-execution registry
// ---------------------------------------------------------------------------

// activeImport marks one operation currently being executed IN THIS process; done closes when the
// execution returns.
type activeImport struct {
	id   string
	done chan struct{}
}

// importRegistry tracks live executors of this process, so a concurrent same-id request joins the
// active execution instead of starting a second one.
type importRegistry struct {
	mu      sync.Mutex
	actives map[string]*activeImport
}

func newImportRegistry() *importRegistry {
	return &importRegistry{actives: map[string]*activeImport{}}
}

func (r *importRegistry) get(id string) *activeImport {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.actives[id]
}

// tryAdd registers a only when no active entry exists yet; it closes the same-process race
// window: the slot is taken BEFORE the durable operation row is inserted.
func (r *importRegistry) tryAdd(a *activeImport) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.actives[a.id]; exists {
		return false
	}
	r.actives[a.id] = a
	return true
}

// remove unregisters a, then closes its done channel: the channel never closes while it is still
// registered.
func (r *importRegistry) remove(a *activeImport) {
	r.mu.Lock()
	if cur := r.actives[a.id]; cur == a {
		delete(r.actives, a.id)
	}
	r.mu.Unlock()
	close(a.done)
}

// ---------------------------------------------------------------------------
// importer checkpoint integration
// ---------------------------------------------------------------------------

// attachRecoverable wires an existing checkpoint into the importer and restores its in-memory
// maps from the durable record.
func (i *importer) attachRecoverable(opID string, cp *importCheckpoint) {
	i.opID = opID
	i.cp = cp
	i.opStore = i.server.importOps
	i.targetIDs = cp.TargetIDs
	i.defIDs = cp.DefIDs
	i.defNames = cp.DefNames
	i.defShapes = make(map[string]definitionShape, len(cp.DefShapes))
	for id, shape := range cp.DefShapes {
		i.defShapes[id] = definitionShape{checkType: shape[0], destinationKind: shape[1]}
	}
}

// checkpointRaw copies the importer's in-memory maps back into the checkpoint and encodes it.
func (i *importer) checkpointRaw() (json.RawMessage, error) {
	cp := i.cp
	cp.TargetIDs = i.targetIDs
	cp.DefIDs = i.defIDs
	cp.DefNames = i.defNames
	cp.DefShapes = make(map[string][2]string, len(i.defShapes))
	for id, shape := range i.defShapes {
		cp.DefShapes[id] = [2]string{shape.checkType, shape.destinationKind}
	}
	return json.Marshal(cp)
}

// persistCheckpoint writes the checkpoint and renews the heartbeat in one statement; a lost
// ownership fence returns errImportFenced.
func (i *importer) persistCheckpoint() error {
	if i.cp == nil {
		return nil
	}
	raw, err := i.checkpointRaw()
	if err != nil {
		return fmt.Errorf("encode import checkpoint: %w", err)
	}
	err = i.opStore.SaveImportCheckpoint(i.ctx, i.opID, i.server.executorID, raw)
	switch {
	case errors.Is(err, store.ErrImportNotOwned):
		return errImportFenced
	case err != nil:
		return fmt.Errorf("persist import checkpoint: %w", err)
	}
	return nil
}

// isDone reports whether item idx of section sec is already durably processed.
func (i *importer) isDone(sec, idx int) bool {
	return i.cp != nil && i.cp.Done[sec].has(idx)
}

// processItem runs one item's body and then durably records its index; the side effect the first
// successful item unlocks follows. On the legacy path it simply runs the body.
func (i *importer) processItem(sec, idx int, body func()) error {
	if i.cp == nil {
		body()
		return nil
	}
	body()
	if err := i.ctx.Err(); err != nil {
		return err
	}
	i.cp.Done[sec].add(idx)
	if err := i.persistCheckpoint(); err != nil {
		return err
	}
	return i.applySideEffect(sec)
}

// applySideEffect performs the section's once-only runtime side effect when at least one item was
// created or updated: alert-rule reconciliation kick and RBAC-change notification. Idempotent
// through the checkpoint gates, and therefore safe to call after every item and at section end.
func (i *importer) applySideEffect(sec int) error {
	cp := i.cp
	switch sec {
	case secAlertRules:
		if cp.AlertSyncKicked {
			return nil
		}
		r := &cp.Response.AlertRules
		if r.Created == 0 && r.Updated == 0 {
			return nil
		}
		i.server.kickSync()
		cp.AlertSyncKicked = true
		return i.persistCheckpoint()
	case secRBAC:
		if cp.RBACNotified {
			return nil
		}
		r := &cp.Response.RBACRoles
		if r.Created == 0 && r.Updated == 0 {
			return nil
		}
		i.server.publishRBACChanged(i.ctx)
		cp.RBACNotified = true
		return i.persistCheckpoint()
	}
	return nil
}

// sectionCollection returns the ledger collection for sec.
func (i *importer) sectionCollection(sec int) *importCollectionResult {
	switch sec {
	case secTargets:
		return &i.cp.Response.Targets
	case secDefinitions:
		return &i.cp.Response.CheckDefinitions
	case secSchedules:
		return &i.cp.Response.CheckSchedules
	case secAlertRules:
		return &i.cp.Response.AlertRules
	case secWebhooks:
		return &i.cp.Response.Webhooks
	case secMaintenance:
		return &i.cp.Response.MaintenanceWindows
	}
	return nil
}

// runRecoverable advances the operation from its checkpoint through the remaining sections in
// dependency order; processed sections and items are skipped.
func (i *importer) runRecoverable(bundle *exportBundle) (importResponse, error) {
	cp := i.cp
	if cp == nil {
		return importResponse{}, errors.New("httpapi: runRecoverable without a checkpoint")
	}
	if i.mayWriteChecks {
		i.bundleDefinitions = bundle.CheckDefinitions
	}

	type sectionStep struct {
		count   int
		allowed bool
		perm    authz.Permission
		apply   func(res *importCollectionResult) error
		always  func() error
	}
	steps := []sectionStep{
		{len(bundle.Targets), i.mayWriteTargets, authz.PermTargetsWrite,
			func(res *importCollectionResult) error { return i.importTargets(bundle.Targets, res) }, i.mapExistingTargets},
		{len(bundle.CheckDefinitions), i.mayWriteChecks, authz.PermChecksWrite,
			func(res *importCollectionResult) error { return i.importDefinitions(bundle.CheckDefinitions, res) }, i.mapExistingDefinitions},
		{len(bundle.CheckSchedules), i.mayWriteSchedules, authz.PermSchedulesWrite,
			func(res *importCollectionResult) error { return i.importSchedules(bundle.CheckSchedules, res) }, nil},
		{len(bundle.AlertRules), i.mayManageAlerts, authz.PermAlertsManage,
			func(res *importCollectionResult) error { return i.importAlertRules(bundle.AlertRules, res) }, nil},
		{len(bundle.Webhooks), i.mayManageWebhooks, authz.PermWebhooksManage,
			func(res *importCollectionResult) error { return i.importWebhooks(bundle.Webhooks, res) }, nil},
		{len(bundle.MaintenanceWindows), i.mayWriteMaintenance, authz.PermMaintenanceWrite,
			func(res *importCollectionResult) error {
				return i.importMaintenanceWindows(bundle.MaintenanceWindows, res)
			}, nil},
	}

	for sec := cp.Section; sec < importSectionCount; sec++ {
		if err := i.ctx.Err(); err != nil {
			return importResponse{}, err
		}
		if sec == secRBAC {
			if err := i.importRBAC(bundle.RBAC, &cp.Response.RBACRoles, &cp.Response.RBACBindings); err != nil {
				return importResponse{}, err
			}
		} else {
			step := steps[sec]
			res := i.sectionCollection(sec)
			switch {
			case step.count > 0 && !step.allowed:
				// Same counted skip and one warning importer.section writes.
				res.Skipped += step.count
				res.warn(string(step.perm), sectionNoPermissionReason(step.perm))
				for idx := range step.count {
					cp.Done[sec].add(idx)
				}
			case step.count > 0:
				if err := step.apply(res); err != nil {
					return importResponse{}, err
				}
			}
			if step.always != nil {
				if err := step.always(); err != nil {
					return importResponse{}, err
				}
			}
		}
		// Section-end fallback fires a side effect lost in the crash window (gate checkpoint), and
		// is otherwise a no-op.
		if err := i.applySideEffect(sec); err != nil {
			return importResponse{}, err
		}
		cp.Section = sec + 1
		if err := i.persistCheckpoint(); err != nil {
			return importResponse{}, err
		}
	}
	return cp.Response, nil
}

// ---------------------------------------------------------------------------
// HTTP orchestration
// ---------------------------------------------------------------------------

// newImporter builds the importer with the caller's permissions and attribution decided once,
// exactly as handleImport did inline.
func (s *Server) newImporter(r *http.Request) *importer {
	return &importer{
		server: s, ctx: r.Context(),
		mayManageRBAC:       s.callerCan(r, authz.PermRBACManage),
		importedBy:          importAuthor(r),
		mayWriteTargets:     s.callerCan(r, authz.PermTargetsWrite),
		mayWriteChecks:      s.callerCan(r, authz.PermChecksWrite),
		mayWriteSchedules:   s.callerCan(r, authz.PermSchedulesWrite),
		mayWriteMaintenance: s.callerCan(r, authz.PermMaintenanceWrite),
		mayManageAlerts:     s.callerCan(r, authz.PermAlertsManage),
		mayManageWebhooks:   s.callerCan(r, authz.PermWebhooksManage),
	}
}

// waitForOp reads an operation a same-process goroutine is about to insert, retrying briefly.
func (s *Server) waitForOp(ctx context.Context, id string) (store.ImportOperation, error) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		op, err := s.importOps.GetImportOperation(ctx, id)
		if err == nil {
			return op, nil
		}
		if !errors.Is(err, store.ErrNotFound) || ctx.Err() != nil {
			return store.ImportOperation{}, err
		}
		if time.Now().After(deadline) {
			return store.ImportOperation{}, errors.New("import operation did not appear in time")
		}
		time.Sleep(time.Millisecond)
	}
}

// serveRecoverableImport joins or creates the durable operation and answers a repeatable result.
func (s *Server) serveRecoverableImport(w http.ResponseWriter, r *http.Request, req *importRequest) {
	id := strings.TrimSpace(req.OperationID)
	fp := fingerprintBundle(req.Bundle)
	ctx := r.Context()

	op, err := s.importOps.GetImportOperation(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		// Take the local slot BEFORE inserting the durable row, so concurrent same-process requests
		// join this execution through the registry instead of racing the new row.
		active := &activeImport{id: id, done: make(chan struct{})}
		if s.importActives.tryAdd(active) {
			raw, mErr := json.Marshal(newImportCheckpoint())
			if mErr != nil {
				s.importActives.remove(active)
				slog.Error("httpapi: encode initial import checkpoint", "error", mErr)
				writeProblem(w, http.StatusInternalServerError, "internal error", "")
				return
			}
			op, err = s.importOps.CreateImportOperation(ctx, id, fp, importAuthor(r), s.executorID, raw)
			if errors.Is(err, store.ErrAlreadyExists) {
				// Another process inserted the same row concurrently: release the slot and join it.
				s.importActives.remove(active)
				op, err = s.waitForOp(ctx, id)
			}
			if err != nil {
				slog.Error("httpapi: import operation store failed", "error", err)
				writeProblem(w, http.StatusBadGateway, "import unavailable",
					"failed to read or create the import operation")
				return
			}
			if op.Fingerprint != fp {
				writeProblem(w, http.StatusConflict, "import operation already exists for a different bundle",
					"operationId "+strconv.Quote(id)+" is already advancing another import; reuse the same id "+
						"only for retries of this exact request, or choose a new id")
				return
			}
			suppressAudit(r)
			if op.Status == store.ImportStatusSucceeded {
				var res importResponse
				if err := json.Unmarshal(op.Result, &res); err != nil {
					slog.Error("httpapi: stored import result unreadable", "id", id, "error", err)
					writeProblem(w, http.StatusBadGateway, "import unavailable", "the stored import result could not be read")
					return
				}
				writeJSON(w, &res)
				return
			}
			s.executeImport(w, r, id, active, &op, req.Bundle)
			return
		}
		// A same-process goroutine holds the slot but has not inserted the row yet: wait for it.
		op, err = s.waitForOp(ctx, id)
		if err != nil {
			slog.Error("httpapi: import operation store failed", "error", err)
			writeProblem(w, http.StatusBadGateway, "import unavailable",
				"failed to read the import operation")
			return
		}
	} else if err != nil {
		slog.Error("httpapi: import operation store failed", "error", err)
		writeProblem(w, http.StatusBadGateway, "import unavailable",
			"failed to read the import operation")
		return
	}

	if op.Fingerprint != fp {
		writeProblem(w, http.StatusConflict, "import operation already exists for a different bundle",
			"operationId "+strconv.Quote(id)+" is already advancing another import; reuse the same id "+
				"only for retries of this exact request, or choose a new id")
		return
	}

	// From here the request participates in the durable operation: suppress the per-request audit
	// row; exactly one logical audit row is written when the import completes.
	suppressAudit(r)

	if op.Status == store.ImportStatusSucceeded {
		var res importResponse
		if err := json.Unmarshal(op.Result, &res); err != nil {
			slog.Error("httpapi: stored import result unreadable", "id", id, "error", err)
			writeProblem(w, http.StatusBadGateway, "import unavailable", "the stored import result could not be read")
			return
		}
		writeJSON(w, &res)
		return
	}

	// An execution live in THIS process: join it and wait bounded.
	if active := s.importActives.get(id); active != nil {
		s.joinActiveImport(w, r, id, active, req.Bundle)
		return
	}

	// Owned by another process with a fresh heartbeat: the executor is alive.
	if time.Since(op.LastHeartbeatAt) < s.importTakeoverTimeout {
		s.answerInProgress(w, &op, req.Bundle)
		return
	}

	// Stale heartbeat -- no live executor: take the local slot, claim the durable row, and resume.
	active := &activeImport{id: id, done: make(chan struct{})}
	if !s.importActives.tryAdd(active) {
		s.joinActiveImport(w, r, id, s.importActives.get(id), req.Bundle)
		return
	}
	claimed, ok, err := s.importOps.ClaimImportOperation(ctx, id, s.executorID, time.Now().Add(-s.importTakeoverTimeout))
	if err != nil {
		s.importActives.remove(active)
		slog.Error("httpapi: import takeover claim failed", "id", id, "error", err)
		writeProblem(w, http.StatusBadGateway, "import unavailable", "failed to take over the import operation")
		return
	}
	if !ok {
		// Lost the race to a fresh heartbeat or another claim: the row holds the current answer.
		s.importActives.remove(active)
		s.answerCurrentState(w, r, id, req.Bundle)
		return
	}
	s.executeImport(w, r, id, active, &claimed, req.Bundle)
}

// executeImport runs the operation to completion and finalizes it; active is the registry entry
// already held by this request and unregistered on return.
func (s *Server) executeImport(w http.ResponseWriter, r *http.Request, id string, active *activeImport, op *store.ImportOperation, bundle *exportBundle) {
	cp := newImportCheckpoint()
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, cp); err != nil {
			slog.Error("httpapi: import checkpoint unreadable", "id", id, "error", err)
			writeProblem(w, http.StatusBadGateway, "import unavailable", "the saved import checkpoint could not be read")
			return
		}
	}
	imp := s.newImporter(r)
	imp.attachRecoverable(id, cp)

	defer s.importActives.remove(active)

	res, err := imp.runRecoverable(bundle)
	switch {
	case errors.Is(err, errImportFenced):
		s.answerCurrentState(w, r, id, bundle)
		return
	case err != nil:
		slog.Error("httpapi: recoverable import stopped before completion", "id", id, "error", err)
		if r.Context().Err() != nil {
			// Caller disconnected: checkpoint stands at the last saved item, operation stays
			// running and a later request joins or takes it over.
			return
		}
		writeProblem(w, http.StatusBadGateway, "import interrupted",
			"the import stopped before finishing; retry POST /api/v1/import with the same operationId "+
				"to continue from the last saved item")
		return
	}

	res.OperationID = id
	res.Status = importStatusSucceeded
	progress := cp.progress(bundle)
	res.Progress = &progress
	raw, err := json.Marshal(&res)
	if err != nil {
		slog.Error("httpapi: encode final import result", "id", id, "error", err)
		writeProblem(w, http.StatusInternalServerError, "internal error", "")
		return
	}
	if err := s.importOps.CompleteImportOperation(r.Context(), id, s.executorID, raw); err != nil {
		if errors.Is(err, store.ErrImportNotOwned) {
			// Another executor finished or owns it: read back its answer.
			s.answerCurrentState(w, r, id, bundle)
			return
		}
		slog.Error("httpapi: complete import operation failed", "id", id, "error", err)
		writeProblem(w, http.StatusBadGateway, "import unavailable", "the import finished but its result could not be stored")
		return
	}

	// The ONE audit row for the logical import: same allow-listed per-collection counts a legacy
	// import records, enqueued only now that the success is terminal.
	subject, _ := SubjectFrom(r.Context())
	detail, _ := json.Marshal(res.auditDetail())
	s.recordAudit(r, subject, auditOutcomeAllowed, detail)
	writeJSON(w, &res)
}

// joinActiveImport waits for the same-process execution up to importJoinWait and otherwise
// answers a recognizable in-progress snapshot.
func (s *Server) joinActiveImport(w http.ResponseWriter, r *http.Request, id string, active *activeImport, bundle *exportBundle) {
	timer := time.NewTimer(s.importJoinWait)
	defer timer.Stop()
	select {
	case <-active.done:
		s.answerCurrentState(w, r, id, bundle)
	case <-timer.C:
		op, err := s.importOps.GetImportOperation(r.Context(), id)
		if err != nil {
			slog.Error("httpapi: read import operation after join wait failed", "id", id, "error", err)
			writeProblem(w, http.StatusBadGateway, "import unavailable", "failed to read the import operation")
			return
		}
		s.answerState(w, &op, bundle)
	case <-r.Context().Done():
	}
}

// answerCurrentState answers from the durable row: final result when succeeded, in-progress
// otherwise.
func (s *Server) answerCurrentState(w http.ResponseWriter, r *http.Request, id string, bundle *exportBundle) {
	op, err := s.importOps.GetImportOperation(r.Context(), id)
	if err != nil {
		slog.Error("httpapi: read import operation failed", "id", id, "error", err)
		writeProblem(w, http.StatusBadGateway, "import unavailable", "failed to read the import operation")
		return
	}
	s.answerState(w, &op, bundle)
}

// answerState renders a succeeded row's stored result or a running row's in-progress snapshot.
func (s *Server) answerState(w http.ResponseWriter, op *store.ImportOperation, bundle *exportBundle) {
	if op.Status == store.ImportStatusSucceeded {
		var res importResponse
		if err := json.Unmarshal(op.Result, &res); err == nil {
			writeJSON(w, &res)
			return
		}
	}
	s.answerInProgress(w, op, bundle)
}

// answerInProgress answers 202 with the partial ledger and a recognizable in-progress status.
func (s *Server) answerInProgress(w http.ResponseWriter, op *store.ImportOperation, bundle *exportBundle) {
	cp := newImportCheckpoint()
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, cp); err != nil {
			slog.Error("httpapi: import checkpoint unreadable during in-progress answer", "id", op.ID, "error", err)
			writeProblem(w, http.StatusBadGateway, "import unavailable", "the saved import checkpoint could not be read")
			return
		}
	}
	res := cp.Response
	res.DryRun = false
	res.OperationID = op.ID
	res.Status = importStatusInProgress
	progress := cp.progress(bundle)
	res.Progress = &progress
	writeJSONStatus(w, http.StatusAccepted, &res)
}

// newExecutorID identifies this console process for ownership and heartbeat fencing.
func newExecutorID() string {
	return "console-" + uuid.NewString()
}
