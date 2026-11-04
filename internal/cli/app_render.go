package cli

import (
	"fmt"
	"io"
	"sort"

	"relay/internal/bytesize"
	"relay/internal/state"
)

// renderAppList renders the `app ls` table (NAME, RUNTIME, STATUS, UPDATED).
// Rows are sorted here so the listing is deterministic regardless of the
// caller's row order.
func renderAppList(w io.Writer, rows []state.Row) error {
	sorted := make([]state.Row, len(rows))
	copy(sorted, rows)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})

	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintln(tw, "NAME\tRUNTIME\tSTATUS\tUPDATED")
	for _, r := range sorted {
		runtime := r.Runtime
		if runtime == "" {
			runtime = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			r.Name, runtime, r.Status, displayTime(r))
	}
	out.flush(tw)
	return out.err
}

// renderAppDetail renders the full detail record and its persisted per-app
// stats. A caller that also has live pool gauges appends them with
// renderRuntimePool. Labels are tab-aligned through a tabwriter so padding
// matches the longest label without hand-maintained spaces. The Stats, Events,
// Schedules, and Services sections use a wider padding for visual grouping.
func renderAppDetail(w io.Writer, detail state.Detail, stats state.AppStats) error {
	out := newOutput(w)
	renderAppOverview(out, detail)

	out.blank()
	out.line("Stats:")
	renderAppStatsSection(out, stats)

	if len(detail.Handlers) > 0 {
		out.blank()
		out.line("Events:")
		renderAppEvents(out, detail.Handlers)
	}

	if len(detail.Schedules) > 0 {
		out.blank()
		out.line("Schedules:")
		renderAppSchedules(out, detail.Schedules)
	}

	if len(detail.Services) > 0 {
		out.blank()
		out.line("Services:")
		renderAppServices(out, detail.Services)
	}

	if len(detail.Env) > 0 {
		out.blank()
		out.line("Environment:")
		renderAppEnv(out, detail.Env)
	}

	if len(detail.Secrets) > 0 {
		out.blank()
		out.line("Secrets:")
		renderAppSecrets(out, detail.Secrets)
	}

	return out.err
}

// renderAppOverview renders the identity, lifecycle, and active-generation
// metadata rows at the top of inspect.
func renderAppOverview(out *output, detail state.Detail) {
	tw := out.table(2)
	fmt.Fprintf(tw, "Name:\t%s\n", detail.Name)

	runtime := detail.Runtime
	if runtime == "" {
		runtime = "-"
	}
	fmt.Fprintf(tw, "Runtime:\t%s\n", runtime)

	fmt.Fprintf(tw, "Status:\t%s\n", detail.Status)
	if detail.Image != "" {
		fmt.Fprintf(tw, "Image:\t%s\n", detail.Image)
	}
	if detail.Fingerprint != "" {
		fmt.Fprintf(tw, "Fingerprint:\t%s\n", detail.Fingerprint)
	}
	if detail.PreparedAt != "" {
		fmt.Fprintf(tw, "Prepared:\t%s (%s)\n",
			detail.PreparedAt, relativeAgo(detail.PreparedAt))
	}
	if detail.LastReconcileAt != "" {
		fmt.Fprintf(tw, "Last reconcile:\t%s (%s)\n",
			detail.LastReconcileStatus, relativeAgo(detail.LastReconcileAt))
	}
	if detail.LastError != "" {
		fmt.Fprintf(tw, "Last error:\t%s\n", detail.LastError)
	}
	if detail.Resources != nil {
		fmt.Fprintf(tw, "Resources:\tmemory=%s cpus=%g pids=%d\n",
			bytesize.Format(detail.Resources.MemoryBytes),
			detail.Resources.CPUs,
			detail.Resources.Pids)
	}
	out.flush(tw)
}

// renderAppStatsSection renders the per-app cumulative counters and execution
// history. The zero AppStats is a valid rendering for an app with no stats row.
func renderAppStatsSection(out *output, stats state.AppStats) {
	tw := out.table(3)
	fmt.Fprintf(tw, "  Events matched:\t%d\n", stats.EventsMatchedTotal)
	fmt.Fprintf(tw, "  Handler successes:\t%d\n", stats.HandlerSuccessTotal)
	fmt.Fprintf(tw, "  Handler failures:\t%d\n", stats.HandlerFailureTotal)
	fmt.Fprintf(tw, "  Handler retries:\t%d\n", stats.RetryTotal)
	fmt.Fprintf(tw, "  Invocations exhausted:\t%d\n", stats.DLQTotal)
	fmt.Fprintf(tw, "  Last execution:\t%s\n", lastAgo(stats.LastExecutionAt))
	fmt.Fprintf(tw, "  Last success:\t%s\n", lastAgo(stats.LastSuccessAt))
	fmt.Fprintf(tw, "  Last failure:\t%s\n", lastAgo(stats.LastFailureAt))
	fmt.Fprintf(tw, "  Last exhaustion:\t%s\n", lastAgo(stats.LastDLQAt))
	out.flush(tw)
}

// renderAppEvents renders each event handler and its timeout.
func renderAppEvents(out *output, handlers []state.Handler) {
	tw := out.table(3)
	for _, h := range handlers {
		fmt.Fprintf(tw, "  %s\ttimeout=%s\n", h.Name, h.Timeout)
	}
	out.flush(tw)
}

// renderAppSchedules renders each cron schedule. Cron descriptions are
// best-effort; the raw expression remains authoritative.
func renderAppSchedules(out *output, schedules []state.Schedule) {
	tw := out.table(3)
	for _, s := range schedules {
		if desc, ok := describeCronFunc(s.Cron); ok {
			fmt.Fprintf(
				tw,
				"  %s\thandler=%s cron=%q (%s) timezone=%s timeout=%s\n",
				s.Name,
				s.Handler,
				s.Cron,
				desc,
				s.Timezone,
				s.Timeout,
			)
			continue
		}
		fmt.Fprintf(
			tw,
			"  %s\thandler=%s cron=%q timezone=%s timeout=%s\n",
			s.Name,
			s.Handler,
			s.Cron,
			s.Timezone,
			s.Timeout,
		)
	}
	out.flush(tw)
}

// renderAppServices renders each persistent service. Non-entrypoint sources
// keep their kind visible through the "image:" prefix.
func renderAppServices(out *output, services []state.Service) {
	tw := out.table(3)
	for _, svc := range services {
		source := svc.Entrypoint
		if svc.Image != "" {
			source = "image:" + svc.Image
		}

		if svc.Path != "" {
			fmt.Fprintf(
				tw,
				"  %s\tsource=%s port=%d replicas=%d path=%s\n",
				svc.Name,
				source,
				svc.Port,
				svc.Replicas,
				svc.Path,
			)
			continue
		}
		fmt.Fprintf(tw, "  %s\tsource=%s port=%d replicas=%d\n",
			svc.Name, source, svc.Port, svc.Replicas)
	}
	out.flush(tw)
}

// renderAppEnv renders env-var names only. The persisted value is a redaction
// marker, and inspect renders that fixed marker explicitly so a raw stored value
// can never be printed.
func renderAppEnv(out *output, env map[string]string) {
	tw := out.table(3)
	for _, k := range sortedKeys(env) {
		fmt.Fprintf(tw, "  %s=%s\n", k, state.RedactedEnvValue)
	}
	out.flush(tw)
}

// renderAppSecrets renders secret reference names only; resolved values must
// never be exposed.
func renderAppSecrets(out *output, secrets map[string]string) {
	tw := out.table(3)
	for _, k := range sortedKeys(secrets) {
		fmt.Fprintf(tw, "  %s=%s\n", k, secrets[k])
	}
	out.flush(tw)
}

// sortedKeys returns the map's keys sorted, for deterministic inspect output.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// lastAgo renders a per-app timestamp for inspect: the empty string means the
// event was never observed ("never"); any other value renders through
// relativeAgo, which falls back to the raw string when parsing fails.
func lastAgo(rfc3339 string) string {
	if rfc3339 == "" {
		return "never"
	}
	return relativeAgo(rfc3339)
}

// displayTime returns the UPDATED column value: prepared_at (if set) else
// updated_at, rendered as a short relative age.
func displayTime(r state.Row) string {
	raw := r.PreparedAt
	if raw == "" {
		raw = r.UpdatedAt
	}
	return relativeAgo(raw)
}
