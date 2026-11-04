package cli

import (
	"fmt"
	"io"

	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/worker"
)

// poolGauges is the live runtime-pool gauge view rendered by inspect, resolved
// from the in-process provider or the worker query socket. Only the LIVE gauges
// live here; cumulative counters always come from the persisted stats row.
type poolGauges struct {
	Capacity   int
	Containers int
	Busy       int
	Idle       int
	Starting   int
}

// resolvePoolGauges prefers the injected in-process provider, then the worker
// query socket at socketPath. (zero, false) means neither source resolves a live
// pool, so the caller renders the gauges unknown. Live gauges are best-effort
// telemetry: no failure here fails the command, and a persisted gauge is never
// substituted because it would go stale between flushes.
func resolvePoolGauges(
	name string,
	provider PoolSnapshotProvider,
	socketPath string,
) (poolGauges, bool) {
	if snap, ok := livePoolSnapshot(name, provider); ok {
		return poolGauges{
			Capacity:   snap.Capacity,
			Containers: snap.Containers,
			Busy:       snap.Busy,
			Idle:       snap.Idle,
			Starting:   snap.Starting,
		}, true
	}
	return socketPoolGauges(name, socketPath)
}

// livePoolSnapshot consults the injected in-process provider. A nil provider
// (the standalone inspect process) or a provider with no pool for the app
// reports (zero, false).
func livePoolSnapshot(
	name string,
	provider PoolSnapshotProvider,
) (runtime.PoolSnapshot, bool) {
	if provider == nil {
		return runtime.PoolSnapshot{}, false
	}
	return provider(name)
}

// socketPoolGauges queries the running worker for name's live gauges. Any
// failure — no worker, a stale socket, or no pool for the app — reports
// (zero, false) and leaves the persisted counters authoritative.
func socketPoolGauges(name, socketPath string) (poolGauges, bool) {
	st, err := worker.QueryRuntimeState(socketPath, name)
	if err != nil {
		return poolGauges{}, false
	}
	return poolGauges{
		Capacity:   st.Capacity,
		Containers: st.Containers,
		Busy:       st.Busy,
		Idle:       st.Idle,
		Starting:   st.Starting,
	}, true
}

// renderRuntimePool renders the compact Runtime pool section. stats supplies the
// cumulative counters (warm acquires, cold starts, discarded), which always come
// from persisted state; live carries the live gauges when they are known. A nil
// live pool renders the gauges as "unknown" rather than a stale number.
func renderRuntimePool(w io.Writer, stats state.AppStats, live *poolGauges) error {
	out := newOutput(w)
	out.blank()
	out.line("Runtime pool:")
	tw := out.table(3)

	warm := stats.WarmAcquiresTotal
	cold := stats.ColdStartsTotal
	discarded := stats.DiscardedTotal
	totalAcquires := warm + cold
	warmAcquires := fmt.Sprintf("%d (n/a)", warm)
	if totalAcquires > 0 {
		reuseRate := float64(warm) / float64(totalAcquires) * 100
		warmAcquires = fmt.Sprintf("%d (%.1f%%)", warm, reuseRate)
	}

	if live != nil {
		fmt.Fprintf(tw, "  Capacity:\t%d\n", live.Capacity)
		fmt.Fprintf(tw, "  Containers:\t%d\n", live.Containers)
		fmt.Fprintf(tw, "  Busy:\t%d\n", live.Busy)
		fmt.Fprintf(tw, "  Idle:\t%d\n", live.Idle)
		if live.Starting > 0 {
			fmt.Fprintf(tw, "  Starting:\t%d\n", live.Starting)
		}
	} else {
		// No live pool: show explicit unavailability, never a persisted gauge.
		fmt.Fprintln(tw, "  Capacity:\tunknown")
		fmt.Fprintln(tw, "  Containers:\tunknown")
		fmt.Fprintln(tw, "  Busy:\tunknown")
		fmt.Fprintln(tw, "  Idle:\tunknown")
	}
	fmt.Fprintf(tw, "  Warm acquires:\t%s\n", warmAcquires)
	fmt.Fprintf(tw, "  Cold starts:\t%d\n", cold)
	fmt.Fprintf(tw, "  Discarded:\t%d\n", discarded)
	out.flush(tw)
	return out.err
}
