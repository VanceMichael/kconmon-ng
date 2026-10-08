import type { LiveEvent } from "./types";

/**
 * The live event ring: pure, React-free, and shared by the Live page and the Overview's recent
 * changes. It lives here rather than in pages/live.tsx so a component on the landing page does not
 * pull the whole Live page into the first download.
 */

/** The browser keeps a bounded ring of the most recent events and drops the oldest. */
export const LIVE_RING_CAP = 2000;

/**
 * eventTime is the parsed timestamp, memoised per event object; an unparseable timestamp becomes
 * -Infinity rather than NaN.
 */
const eventTime = new WeakMap<LiveEvent, number>();

export function timeOf(e: LiveEvent): number {
  const cached = eventTime.get(e);
  if (cached !== undefined) return cached;
  const parsed = Date.parse(e.timestamp);
  const value = Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
  eventTime.set(e, value);
  return value;
}

/**
 * compareDesc orders two events for the newest-first feed — lexicographic on (timestamp, seq); the
 * timestamp leads and the controller-assigned seq only breaks ties.
 */
function compareDesc(a: LiveEvent, b: LiveEvent): number {
  const ta = timeOf(a);
  const tb = timeOf(b);
  if (ta !== tb) return ta > tb ? -1 : 1;
  if (a.seq !== b.seq) return a.seq > b.seq ? -1 : 1;
  return 0;
}

/** isAbove is compareDesc as a predicate; the merge below reads better with it. */
function isAbove(a: LiveEvent, b: LiveEvent): boolean {
  return compareDesc(a, b) < 0;
}

/**
 * pushEvents merges arrivals into a newest-first ring capped at LIVE_RING_CAP,
 * returning a new array — or `prev` itself when nothing new arrived, so React
 * skips the re-render.
 *
 * Two transport facts shape this. Delivery is exactly-once per connection but
 * NOT ordered (a Broadcast can beat the replay that a reconnect asked for), so
 * a lower seq than one already held is a late arrival to be filed in place,
 * never a straggler to discard. And a reconnect replays from the resume cursor,
 * so the same event can genuinely arrive twice — deduped here on the
 * controller-assigned id, the very key the hub itself dedupes on.
 *
 * The one same-id MUTATION is an upgrade: an unconfirmed row (persisted=false,
 * published while the database was unreachable) is replaced in place by the
 * later confirmed frame with the same id (persisted=true). Its timestamp is
 * identical so its sort position does not move; the row only flips state.
 *
 * Both sides are sorted, so this is a straight two-list merge: O(n + m) with the
 * cap applied as it builds, rather than a splice per arrival. That matters on
 * exactly one path — a tab returning from hidden flushes a whole queue at once,
 * and a per-item splice into a full ring would be tens of millions of element
 * moves inside a single frame.
 */
export function pushEvents(prev: LiveEvent[], incoming: LiveEvent[]): LiveEvent[] {
  if (incoming.length === 0) return prev;

  // Phase 1: same-id unconfirmed→confirmed upgrades replace the held row in place.
  let base = prev;
  let upgraded = 0;
  for (const e of incoming) {
    if (typeof e.id !== "string" || e.id === "" || e.persisted !== true) continue;
    const idx = base.findIndex((x) => x.id === e.id);
    if (idx >= 0 && base[idx].persisted === false) {
      if (base === prev) base = [...prev];
      base[idx] = e;
      upgraded++;
    }
  }

  // Phase 2: genuine new arrivals, deduped against held ids and each other.
  const seen = new Set(base.map((e) => e.id));
  const fresh: LiveEvent[] = [];
  for (const e of incoming) {
    // Dedupe on a REAL id only. A row without one is not "the same event" as
    // every other id-less row; and an id already held — a duplicate, or an
    // upgrade phase 1 applied — drives nothing.
    const identified = typeof e.id === "string" && e.id !== "";
    if (identified) {
      if (seen.has(e.id)) continue;
      seen.add(e.id);
    }
    fresh.push(e);
  }
  if (fresh.length === 0) {
    return upgraded > 0 ? base : prev;
  }

  fresh.sort(compareDesc);

  const next: LiveEvent[] = [];
  let i = 0;
  let j = 0;
  while (i < base.length && j < fresh.length && next.length < LIVE_RING_CAP) {
    next.push(isAbove(fresh[j], base[i]) ? fresh[j++] : base[i++]);
  }
  while (i < base.length && next.length < LIVE_RING_CAP) next.push(base[i++]);
  while (j < fresh.length && next.length < LIVE_RING_CAP) next.push(fresh[j++]);
  return next;
}
