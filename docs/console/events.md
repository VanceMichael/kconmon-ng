# Events

The controller's event feed, newest first: restarts, readiness flaps, check observations, on-demand traceroutes, in the order the controller saw them. When you need the raw record of what happened around 14:32, this is it.

<figure markdown>
![Live event feed during a staged break: severity and type filters, Pause, the Live badge, the scope search, Showing 303 of 303 events · capped at 2000, and rows from 06:21:30 to 06:21:31 mixing Error (mtr kc-accept-worker→kc-accept-worker5 failed with 0 hops), Info (tcp check succeeded, tcp diagnostic dispatched, mtr triggered) and Warn (tcp diagnostic timeout, among them kc-accept-worker2→kc-accept-worker)](../img/console-events-live.png){ loading=lazy }
<figcaption>The live feed mid-break: a severity badge on every row (Error, Info and Warn here), the filter toolbar, and "Showing 303 of 303 events · capped at 2000". The Warn rows are TCP diagnostic probes timing out on pairs that touch the cut-off nodes worker2 and worker5, and the Error rows are MTR traces toward worker5 that got no hop back.</figcaption>
</figure>

## How the feed is fed

Live, events are pushed over a WebSocket (topic `live` on `ws(s)://<console-host>/ws`); the header carries a **Live** badge while the stream is up and **Delayed data** when it is not (the [badge's semantics](overview.md#the-console-chrome) are chrome-wide). The browser keeps the most recent **2 000** events in a ring. Anything older is served from event history (`GET /api/v1/events`), which exists only when the console has a database (Helm: `database.existingSecret`, see [Enable the console](../getting-started/enable-the-console.md)).

Events arrive **receipt-first**. Each controller event carries a durable identity — `event_id`, a UUID the controller assigns (controllers older than that keep working through the derived `<seq>-<unixNano>` id every replica computes identically). Before the event drives anything else, the ingester inserts it into a durable receipt log, `event_inbox`, keyed by that id: this insert is the ONE dedup gate shared by reconnect replay and by every console replica. A transactional outbox then drains the log in `(epoch, seq)` order, writes the event's history twin (`topology_events`) — and the Time Machine's baseline seed — in the SAME transaction, and publishes the **confirmed** live frame (with `persisted: true`) only AFTER that commit. The WebSocket feed, REST/Time Machine and the topology fold therefore converge on the same event set; a duplicate redelivery adds no history and never re-advances derived state.

The stream is sequenced. Every event carries a per-controller sequence number, so the tab can detect holes: a gap in the numbering means something went missing between the controller and this tab, and the feed says how many events *may* have been lost, with a "Why?" disclosure explaining the two causes (numbering holes, and frames a hidden tab dropped because the browser gave it nothing to render in). If the socket drops, the client reconnects with exponential backoff from 1 s up to 15 s; the console asks the controller to resume from the highest seq it durably holds for the controller's current incarnation (named by the `x-kconmon-events-epoch` response header), and the controller replays what it still buffers. After a controller restart — a new epoch, seq back at 1 — or an unrecoverable resume gap, the console records a fresh topology baseline at stream establishment to re-anchor the fold, and any remaining gap is reported rather than papered over.

While the database is briefly unreachable, a receipt that cannot yet be written is not dropped and not faked: the ingester keeps it in a bounded pending spool (a shared Valkey/Redis when one is configured, so it survives a console process restart) and immediately publishes it as an **unconfirmed** frame — envelope type `event_pending`, `persisted: false` — marked "Saving…" on its row. When storage returns, the retained events are receipted in order and the SAME ids are re-delivered as confirmed; each row flips in place instead of duplicating. Without a configured database the field is absent and the feed behaves as it always has. Events whose payload this build can never project (an unknown event from a newer controller, corrupt bytes) reach a diagnosable **dead** terminal state in `event_inbox` with a WARN log and metrics; they no longer block later events and age out with normal retention.

With the [Time Machine](time-machine.md) engaged the live tail is off: the feed becomes scrollback ending at the viewed instant, and **Load older** walks back from there.

## Reading the feed

Columns: **Time**, **Severity**, **Summary**, **Scope**. There is no Type column, because the summary opens with the type's own words. Severities are **Info**, **Warn**, **Error**; an unknown wire value still renders raw, since the feed never hides an event it cannot classify.

Operator [annotations](metrics.md#annotations-and-maintenance-windows) are interleaved at their own timestamp with a **Note** badge (ranged ones read *Annotation (span)*). They are not events and the event filters do not touch them. The severity and Note badges keep their words at every width, phones included.

The **Type** filter offers, besides *All types*:

- **Topology changed**: a node or agent joined, left, or changed readiness.
- **Check observed**: a scheduled or continuous check produced a result worth reporting.
- **MTR triggered**: an on-demand traceroute was dispatched, from a run or from `kubectl kconmon mtr` (see [Routes · MTR](routes-mtr.md#reactive-vs-manual-traces)). Reactive traces do not appear here.
- **MTR completed**: that trace finished, with its hops.
- **Diagnostic progress**: a run started from [Run checks](run-checks.md) reported progress.

Filtering: **Severity** and **Type** selects (both default *All*), and **Scope contains**, a case-insensitive substring match on the event's scope. Pair input is normalised, so `node-a->node-b`, `node-a => node-b` and `node-a > node-b` all match the same pair. **Clear filters** resets all three.

**Pause** buffers arrivals, and the button becomes **Resume** with the count ("Resume (3 buffered)"), which drains the buffer into the feed. While paused, the badge reads "Paused · socket live" or "Paused · socket down", so you know whether the feed you are about to resume is still there. The counter line reads "Showing {shown} of {held} events · capped at 2000". The feed fills the window below the toolbar at every width, so on a phone the page scrolls once rather than a list inside a page.

<figure markdown>
![The feed paused: a Resume (258 buffered) button, the badge reading Paused · socket live, Showing 308 of 308 events · capped at 2000 on the Load older line, and the rows frozen at 06:21:31](../img/console-events-paused.png){ loading=lazy }
<figcaption>Paused with a live socket: the badge keeps saying the socket is alive, and <em>Resume (258 buffered)</em> counts what arrived while paused and drains it into the feed.</figcaption>
</figure>

**Load older** pages history back through `GET /api/v1/events`. A page that fails shows the server's reason and leaves **Load older** enabled, so the next press retries the same page. That includes the first page: when it fails, such as with the 503 of a console without event history, the notice carries **Retry** too. When the 2 000-event ring is already full it refuses instead of spending a round trip on rows it would have to drop: "The buffer is full at {cap} events. Older ones cannot be added without dropping newer ones; narrow the filters or reload to start a fresh buffer."

## When the feed degrades

Four distinct cards, four distinct fixes:

**"This replica is not receiving the controller event stream."** The feed is not broken, it is unfed: no events will arrive here while that holds, Matrix and Topology fall back to 15 s polling, and the feed resumes on its own within 15 s of the stream coming back. What to check, in order:

1. `controller.events.enabled` in the Helm values: the domain event stream is **off by default**.
2. Whether the controller actually restarted after you flipped it. Before chart 2.0.3 a values change updated the ConfigMap under a running controller that reads it once at startup, so the stream stayed configured-but-never-started and nothing anywhere logged an error; 2.0.3 added a `checksum/config` annotation so a values change rolls the pods. On an older chart, `kubectl rollout restart` the controller yourself.
3. Replica topology: the stream is served by the controller **leader** only, and with several console replicas each connects independently, so one replica can be fed while another is not.

**"The live topic was rejected."** The WebSocket subscription itself was refused: topics are authorization-gated server-side, and a session whose role lacks the matching read permission cannot subscribe (`events:read` alone does not grant the topology or matrix topics either, and a run's `run:{id}` topic needs `runs:read`, like `GET /api/v1/runs/{id}`). The built-in roles hold all of them. This is a permissions conversation, not a networking one.

**"Event history is unavailable."** The history half failed, or the console could not read its own configuration (`GET /api/v1/config`), so it cannot tell whether event history exists; the card then says "Could not read the console configuration, so event history was not requested: {error}". Either way the live half keeps working off the socket. A console with no database shows no such card: it offers no *Load older*, and the feed holds only what arrived live.

**"Event history is catching up."** This one is different from the previous card: the live socket is healthy, but the durable receipt gate could not write while the database was down. The events listed in the card (and marked **Saving…** on their rows) ARE on screen, but they are not yet in history — Time Machine and *Load older* do not include them. They are retained and filled in automatically, in order, when storage recovers, and the rows flip to confirmed themselves; no reload or replay is needed. Watch the `kconmon_ng_console_events_unconfirmed` gauge and, in an outage long enough to exhaust the bounded spool, `..._unconfirmed_dropped_total` — dropped events are exactly the ones the feed's seq-gap disclosure names.

## Getting here

*open Events* on the [Overview](overview.md) lands here, and event rows appear as timeline entries in [Incidents](incidents.md) whenever their scope matches.

<!-- verified against: web/src/pages/live.tsx, web/src/lib/i18n/dict/live.ts (loadOlder.atCap, missed.title.*,
     noRealtime.*, topicError.*, history.*, unconfirmed.*, counts), web/src/lib/ws.ts (per-hub seq, epoch guard,
     RECONNECT_MIN/MAX 1s..15s, resume with lastSeq), web/src/lib/live-events.ts (unconfirmed→confirmed same-id
     upgrade), internal/console/events/outbox.go (receipt-first event_inbox outbox, pending spool),
     internal/console/store/inbox.go (received/projected/dead, in-order projection),
     api/kconmon.proto (event_id; WatchEventsRequest.last_seq), internal/controller/grpc_server.go
     (resume ring, epoch/resume headers), charts/kconmon-ng/values.yaml L233-235 (controller.events.enabled,
     leader-only), RELEASE_NOTES.md v2.0.3 (checksum/config fix), internal/console/httpapi/ws_authz_test.go
     (topic authz). -->
