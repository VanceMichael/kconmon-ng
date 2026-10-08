package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EsDmitrii/kconmon-ng/internal/console/authz"
	"github.com/EsDmitrii/kconmon-ng/internal/console/cache"
	"github.com/EsDmitrii/kconmon-ng/internal/console/store"
)

// ---------------------------------------------------------------------------
// fakeImportOps: in-memory ImportOperations
// ---------------------------------------------------------------------------

type fakeImportOps struct {
	mu  sync.Mutex
	ops map[string]store.ImportOperation
}

func newFakeImportOps() *fakeImportOps {
	return &fakeImportOps{ops: map[string]store.ImportOperation{}}
}

func copyBytes(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func copyImportOperation(op store.ImportOperation) store.ImportOperation {
	op.Checkpoint = copyBytes(op.Checkpoint)
	op.Result = copyBytes(op.Result)
	return op
}

func (f *fakeImportOps) CreateImportOperation(_ context.Context, id, fingerprint, startedBy, executorID string, checkpoint json.RawMessage) (store.ImportOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.ops[id]; exists {
		return store.ImportOperation{}, store.ErrAlreadyExists
	}
	now := time.Now().UTC()
	op := store.ImportOperation{
		ID: id, Fingerprint: fingerprint, Status: store.ImportStatusRunning,
		Checkpoint: copyBytes(checkpoint), ExecutorID: executorID, StartedBy: startedBy,
		LastHeartbeatAt: now, CreatedAt: now, UpdatedAt: now,
	}
	f.ops[id] = op
	return copyImportOperation(op), nil
}

func (f *fakeImportOps) GetImportOperation(_ context.Context, id string) (store.ImportOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	op, ok := f.ops[id]
	if !ok {
		return store.ImportOperation{}, store.ErrNotFound
	}
	return copyImportOperation(op), nil
}

func (f *fakeImportOps) ClaimImportOperation(_ context.Context, id, executorID string, staleBefore time.Time) (store.ImportOperation, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	op, ok := f.ops[id]
	if !ok || op.Status != store.ImportStatusRunning || !op.LastHeartbeatAt.Before(staleBefore) {
		return store.ImportOperation{}, false, nil
	}
	now := time.Now().UTC()
	op.ExecutorID = executorID
	op.LastHeartbeatAt = now
	op.UpdatedAt = now
	f.ops[id] = op
	return copyImportOperation(op), true, nil
}

func (f *fakeImportOps) SaveImportCheckpoint(_ context.Context, id, executorID string, checkpoint json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	op, ok := f.ops[id]
	if !ok || op.ExecutorID != executorID || op.Status != store.ImportStatusRunning {
		return store.ErrImportNotOwned
	}
	now := time.Now().UTC()
	op.Checkpoint = copyBytes(checkpoint)
	op.LastHeartbeatAt = now
	op.UpdatedAt = now
	f.ops[id] = op
	return nil
}

func (f *fakeImportOps) CompleteImportOperation(_ context.Context, id, executorID string, result json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	op, ok := f.ops[id]
	if !ok || op.ExecutorID != executorID || op.Status != store.ImportStatusRunning {
		return store.ErrImportNotOwned
	}
	now := time.Now().UTC()
	op.Status = store.ImportStatusSucceeded
	op.Result = copyBytes(result)
	op.LastHeartbeatAt = now
	op.UpdatedAt = now
	f.ops[id] = op
	return nil
}

// ---------------------------------------------------------------------------
// spyBus: counts RBAC-change publications
// ---------------------------------------------------------------------------

type spyBus struct {
	mu        sync.Mutex
	publishes []string
}

func newSpyBus() *spyBus { return &spyBus{} }

func (b *spyBus) Publish(_ context.Context, topic string, _ cache.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publishes = append(b.publishes, topic)
	return nil
}

func (b *spyBus) Subscribe(string) (<-chan cache.Message, func()) {
	ch := make(chan cache.Message)
	return ch, func() {}
}

func (b *spyBus) CrossReplica() bool { return true }

func (b *spyBus) publishCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.publishes)
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

type recoverableFixture struct {
	exportFixture
	ops    *fakeImportOps
	audit  *fakeAuditStore
	syncer *fakeRuleSyncer
	bus    *spyBus
}

func newRecoverableServer(t *testing.T, wrapSchedules func(ScheduleService) ScheduleService, targets TargetService) recoverableFixture {
	t.Helper()
	checks := newFakeChecksStore()
	innerTargets := &linkedTargetService{fakeTargetService: newFakeTargetService(), checks: checks}
	targetSeam := TargetService(innerTargets)
	if targets != nil {
		targetSeam = targets
	}
	scheduleSeam := ScheduleService(checks)
	if wrapSchedules != nil {
		scheduleSeam = wrapSchedules(scheduleSeam)
	}
	fx := exportFixture{
		targets: innerTargets, checks: checks, alertRules: newFakeAlertRuleStore(),
		webhooks: newFakeWebhookStore(), maintenance: newFakeMaintenanceStore(),
		rbac: newFakeRoleAdmin(),
	}
	ops := newFakeImportOps()
	auditStore := &fakeAuditStore{}
	syncer := &fakeRuleSyncer{}
	bus := newSpyBus()
	authr := fakeAuthenticator{subject: authz.Subject{Kind: authz.SubjectUser, ID: "u1"}}
	fx.server = newAuthzServer(t, authr, authz.NewPolicy(nil), Deps{
		Roles:       fakeRoleResolver{roles: []string{"admin"}},
		Targets:     targetSeam,
		Schedules:   scheduleSeam,
		Definitions: checks,
		AlertRules:  fx.alertRules,
		Webhooks:    fx.webhooks,
		Maintenance: fx.maintenance,
		RBAC:        fx.rbac,

		ImportOperations: ops, Audit: auditStore,
		RuleSync: syncer, Bus: bus,
	})
	return recoverableFixture{
		exportFixture: fx, ops: ops, audit: auditStore, syncer: syncer, bus: bus,
	}
}

// doKeyedImport POSTs a keyed non-dry-run import.
func doKeyedImport(t *testing.T, srv *Server, id string, bundle *exportBundle) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(importRequest{OperationID: id, Bundle: bundle})
	if err != nil {
		t.Fatalf("marshal import request: %v", err)
	}
	return doRequest(t, srv, http.MethodPost, "/api/v1/import",
		strings.NewReader(string(raw)), mutateWithCSRF)
}

// ---------------------------------------------------------------------------
// Bundle builders
// ---------------------------------------------------------------------------

func fullRecoverableBundle() *exportBundle {
	windowStart := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	return &exportBundle{
		Version: exportBundleVersion, ExportedAt: time.Now().UTC(),
		Targets: []targetResponse{{
			ID: "bundle-target-1", Name: "edge-lb", Kind: "host",
			Address: "10.0.0.7", Labels: json.RawMessage(`{}`),
		}},
		CheckDefinitions: []definitionResponse{{
			ID: "bundle-def-1", Name: "edge-tcp", SourceSelection: "one-per-zone",
			DestinationKind: "target", DestinationTargetID: "bundle-target-1",
			CheckType: "tcp", Plane: "pod", Params: json.RawMessage(`{}`), Enabled: true,
		}},
		CheckSchedules: []exportSchedule{{
			ID: "bundle-sched-1", DefinitionID: "bundle-def-1", Kind: "interval",
			IntervalNs: int64(time.Minute), Enabled: true,
		}},
		AlertRules: []exportAlertRule{{
			ID: "bundle-rule-1", Name: "EdgeDown", Kind: "raw",
			Params:   json.RawMessage(`{"expr":"up == 0"}`),
			Severity: "warning", ForNs: int64(5 * time.Minute),
			Labels: json.RawMessage(`{}`), Annotations: json.RawMessage(`{}`),
			Enabled: true,
		}},
		MaintenanceWindows: []maintenanceResponse{{
			Scope: "node-a", StartAt: windowStart, EndAt: windowStart.Add(time.Hour),
			Reason: "firmware upgrade",
		}},
		RBAC: &exportRBAC{
			Roles: []exportRole{{Name: "path-reader", Permissions: []string{string(authz.PermMTRRead)}}},
		},
	}
}

func decodeImportBody(t *testing.T, w *httptest.ResponseRecorder) importResponse {
	t.Helper()
	var out importResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode import body: %v (%s)", err, w.Body)
	}
	return out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// A repeated keyed import returns the same stored result and creates nothing again.
func TestRecoverableImportIdempotentRepeatRead(t *testing.T) {
	fx := newRecoverableServer(t, nil, nil)
	fx.server.importTakeoverTimeout = 0
	bundle := fullRecoverableBundle()

	w := doKeyedImport(t, fx.server, "op-1", bundle)
	if w.Code != http.StatusOK {
		t.Fatalf("first import = %d, want 200: %s", w.Code, w.Body)
	}
	first := decodeImportBody(t, w)
	if first.OperationID != "op-1" || first.Status != importStatusSucceeded {
		t.Errorf("envelope = %q/%q, want op-1/succeeded", first.OperationID, first.Status)
	}
	if first.Progress == nil || first.Progress.CompletedItems != first.Progress.TotalItems {
		t.Errorf("progress = %+v, want all items complete", first.Progress)
	}
	if len(fx.targets.byID) != 1 {
		t.Fatalf("targets = %d rows, want 1", len(fx.targets.byID))
	}

	w2 := doKeyedImport(t, fx.server, "op-1", bundle)
	if w2.Code != http.StatusOK {
		t.Fatalf("repeat import = %d, want 200", w2.Code)
	}
	second := decodeImportBody(t, w2)
	if second.Targets.Created != 1 || second.Targets.Updated != 0 {
		t.Errorf("repeat targets ledger = %+v, want the stored one, no new write", second.Targets)
	}
	if len(fx.targets.byID) != 1 {
		t.Errorf("targets = %d rows after repeat, want still 1", len(fx.targets.byID))
	}
}

// crashingSchedules fails the FIRST ListSchedules, simulating a process that stopped at the
// schedules section after targets and definitions were already checkpointed.
type crashingSchedules struct {
	*fakeChecksStore
	mu       sync.Mutex
	failNext bool
}

func (c *crashingSchedules) ListSchedules(ctx context.Context, f store.ScheduleFilter) (store.SchedulePage, error) {
	c.mu.Lock()
	fail := c.failNext
	c.failNext = false
	c.mu.Unlock()
	if fail {
		return store.SchedulePage{}, errors.New("simulated crash: schedule backend unavailable")
	}
	return c.fakeChecksStore.ListSchedules(ctx, f)
}

// After a mid-import crash the same id resumes from the checkpoint: completed sections are not
// re-applied and remaining sections complete in dependency order.
func TestRecoverableImportResumesAfterCrash(t *testing.T) {
	fx := newRecoverableServer(t, func(inner ScheduleService) ScheduleService {
		return &crashingSchedules{fakeChecksStore: inner.(*fakeChecksStore), failNext: true}
	}, nil)
	fx.server.importTakeoverTimeout = 0
	bundle := fullRecoverableBundle()

	w := doKeyedImport(t, fx.server, "op-crash", bundle)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("crashed import = %d, want 502: %s", w.Code, w.Body)
	}
	if len(fx.targets.byID) != 1 {
		t.Errorf("targets = %d, want the 1 the dead executor completed", len(fx.targets.byID))
	}
	if schedules, _ := fx.checks.ListSchedules(context.Background(), store.ScheduleFilter{}); len(schedules.Schedules) != 0 {
		t.Errorf("schedules = %d, want none before resume", len(schedules.Schedules))
	}
	op, err := fx.ops.GetImportOperation(context.Background(), "op-crash")
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if op.Status != store.ImportStatusRunning {
		t.Errorf("op status = %s, want running", op.Status)
	}

	w2 := doKeyedImport(t, fx.server, "op-crash", bundle)
	if w2.Code != http.StatusOK {
		t.Fatalf("resumed import = %d, want 200: %s", w2.Code, w2.Body)
	}
	res := decodeImportBody(t, w2)
	if res.Targets.Created != 1 || res.CheckDefinitions.Created != 1 ||
		res.CheckSchedules.Created != 1 || res.AlertRules.Created != 1 ||
		res.MaintenanceWindows.Created != 1 || res.RBACRoles.Created != 1 {
		t.Errorf("resume ledger = %+v, want each section's one item created", res)
	}
	if len(fx.targets.byID) != 1 {
		t.Errorf("targets = %d rows after resume, want 1 (no duplicate create)", len(fx.targets.byID))
	}

	// Final result repeats from storage without further writes.
	w3 := doKeyedImport(t, fx.server, "op-crash", bundle)
	if w3.Code != http.StatusOK {
		t.Fatalf("final repeat = %d, want 200", w3.Code)
	}
}

// Reusing an id with a different bundle is a conflict, never an ambiguous merge.
func TestRecoverableImportFingerprintMismatchConflict(t *testing.T) {
	fx := newRecoverableServer(t, nil, nil)
	fx.server.importTakeoverTimeout = 0
	bundle := fullRecoverableBundle()

	w := doKeyedImport(t, fx.server, "op-dup", bundle)
	if w.Code != http.StatusOK {
		t.Fatalf("first import = %d, want 200", w.Code)
	}
	other := fullRecoverableBundle()
	other.Targets[0].Name = "another-lb"
	w2 := doKeyedImport(t, fx.server, "op-dup", other)
	if w2.Code != http.StatusConflict {
		t.Fatalf("different bundle = %d, want 409: %s", w2.Code, w2.Body)
	}
}

// Concurrent requests carrying the same id all read the same result and exactly one set of rows
// is created.
func TestRecoverableImportConcurrentRequests(t *testing.T) {
	fx := newRecoverableServer(t, nil, nil)
	bundle := fullRecoverableBundle()
	const n = 10

	start := make(chan struct{})
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := range n {
		wg.Go(func() {
			<-start
			w := doKeyedImport(t, fx.server, "op-concurrent", bundle)
			codes[i] = w.Code
		})
	}
	close(start)
	wg.Wait()
	for i, code := range codes {
		if code != http.StatusOK {
			t.Errorf("request %d = %d, want 200", i, code)
		}
	}
	if len(fx.targets.byID) != 1 {
		t.Errorf("targets = %d rows, want exactly 1", len(fx.targets.byID))
	}
}

// blockingTargets holds the first CreateTarget until release, so another request can observe the
// recognizable in-progress result.
type blockingTargets struct {
	*linkedTargetService
	inside  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingTargets) CreateTarget(ctx context.Context, in store.TargetInput) (store.Target, error) {
	b.once.Do(func() { close(b.inside) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return store.Target{}, ctx.Err()
	}
	return b.linkedTargetService.CreateTarget(ctx, in)
}

// A request arriving while the same operation executes in this process gets 202 in-progress with
// a recognizable envelope; after completion both read the same final result.
func TestRecoverableImportInProgressAnswer(t *testing.T) {
	checks := newFakeChecksStore()
	inner := &linkedTargetService{fakeTargetService: newFakeTargetService(), checks: checks}
	blocker := &blockingTargets{
		linkedTargetService: inner,
		inside:              make(chan struct{}), release: make(chan struct{}),
	}
	fx := newRecoverableServer(t, nil, blocker)
	fx.server.importJoinWait = 30 * time.Millisecond
	bundle := fullRecoverableBundle()

	var wg sync.WaitGroup
	wg.Go(func() {
		w := doKeyedImport(t, fx.server, "op-progress", bundle)
		if w.Code != http.StatusOK {
			t.Errorf("first import = %d, want 200: %s", w.Code, w.Body)
		}
	})
	<-blocker.inside

	w2 := doKeyedImport(t, fx.server, "op-progress", bundle)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("concurrent import = %d, want 202: %s", w2.Code, w2.Body)
	}
	res := decodeImportBody(t, w2)
	if res.Status != importStatusInProgress || res.OperationID != "op-progress" {
		t.Errorf("202 envelope = %q/%q, want in-progress/op-progress", res.Status, res.OperationID)
	}
	if res.Progress == nil || res.Progress.Section != "targets" || res.Progress.TotalItems != 6 {
		t.Errorf("202 progress = %+v, want targets, 6 total items", res.Progress)
	}

	close(blocker.release)
	wg.Wait()

	if len(inner.byID) != 1 {
		t.Errorf("targets = %d rows, want 1", len(inner.byID))
	}
}

// Alert-rule sync and RBAC-change notification fire once for the logical import, not per attempt.
func TestRecoverableImportSideEffectsOnce(t *testing.T) {
	fx := newRecoverableServer(t, nil, nil)
	fx.server.importTakeoverTimeout = 0
	bundle := fullRecoverableBundle()

	w := doKeyedImport(t, fx.server, "op-effects", bundle)
	if w.Code != http.StatusOK {
		t.Fatalf("import = %d, want 200", w.Code)
	}
	if got := fx.syncer.kickCount(); got != 1 {
		t.Errorf("rule sync kicks = %d, want exactly 1", got)
	}
	if got := fx.bus.publishCount(); got != 1 {
		t.Errorf("rbac publishes = %d, want exactly 1", got)
	}

	for range 3 {
		w2 := doKeyedImport(t, fx.server, "op-effects", bundle)
		if w2.Code != http.StatusOK {
			t.Fatalf("repeat = %d, want 200", w2.Code)
		}
	}
	if got := fx.syncer.kickCount(); got != 1 {
		t.Errorf("rule sync kicks after repeats = %d, want still 1", got)
	}
	if got := fx.bus.publishCount(); got != 1 {
		t.Errorf("rbac publishes after repeats = %d, want still 1", got)
	}
}

// Exactly one audit row exists for the whole logical import despite several transport attempts.
func TestRecoverableImportSingleLogicalAuditRow(t *testing.T) {
	fx := newRecoverableServer(t, nil, nil)
	fx.server.importTakeoverTimeout = 0
	bundle := fullRecoverableBundle()

	w := doKeyedImport(t, fx.server, "op-audit", bundle)
	if w.Code != http.StatusOK {
		t.Fatalf("import = %d, want 200", w.Code)
	}
	for range 2 {
		if w2 := doKeyedImport(t, fx.server, "op-audit", bundle); w2.Code != http.StatusOK {
			t.Fatalf("repeat = %d, want 200", w2.Code)
		}
	}
	entries := waitForAuditEntries(t, fx.audit, 1)
	if len(entries) != 1 {
		t.Fatalf("audit rows = %d, want exactly 1", len(entries))
	}
	if entries[0].Action != "POST /api/v1/import" || entries[0].Outcome != auditOutcomeAllowed {
		t.Errorf("audit row = %s/%s, want POST /api/v1/import/allowed", entries[0].Action, entries[0].Outcome)
	}
}

// A dry run ignores the identifier and creates no recoverable operation and no side effects.
func TestRecoverableImportDryRunCreatesNoOperation(t *testing.T) {
	fx := newRecoverableServer(t, nil, nil)
	bundle := fullRecoverableBundle()

	raw, err := json.Marshal(importRequest{DryRun: true, OperationID: "op-dryrun", Bundle: bundle})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	w := doRequest(t, fx.server, http.MethodPost, "/api/v1/import",
		strings.NewReader(string(raw)), mutateWithCSRF)
	if w.Code != http.StatusOK {
		t.Fatalf("dry run = %d, want 200: %s", w.Code, w.Body)
	}
	var res importResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !res.DryRun || res.OperationID != "" {
		t.Errorf("dry run envelope = %v/%q, want legacy dry-run shape", res.DryRun, res.OperationID)
	}
	if _, err := fx.ops.GetImportOperation(context.Background(), "op-dryrun"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("dry run op err = %v, want ErrNotFound (no durable operation)", err)
	}
	if fx.syncer.kickCount() != 0 {
		t.Errorf("dry run kicks = %d, want 0", fx.syncer.kickCount())
	}
}

// Malformed identifiers are refused before any operation is created.
func TestRecoverableImportInvalidOperationID(t *testing.T) {
	fx := newRecoverableServer(t, nil, nil)
	bundle := fullRecoverableBundle()

	for _, id := range []string{
		"bad\x00id",
		strings.Repeat("a", importOperationIDMaxLen+1),
	} {
		w := doKeyedImport(t, fx.server, id, bundle)
		if w.Code != http.StatusUnprocessableEntity {
			t.Errorf("id %q = %d, want 422: %s", id[:min(8, len(id))], w.Code, w.Body)
		}
	}
}
