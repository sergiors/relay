"""Per-invocation duration and memory report on native stdout.

Embedded beside the bootstrap and written to ``/relay/_relay_report.py`` in the
runtime image.

After an invocation completes (including a handled failure), one plain line is
appended after END:

  REPORT RequestId: <id> Duration: <ms> ms Memory Used: <MiB> MiB Container Memory Peak: <MiB> MiB

Duration is monotonic wall-clock milliseconds with two decimals. Memory values
come from ``_relay_memory`` as raw byte counters and are truncated to whole MiB.
A field without a value is omitted, so the line never fabricates one. The line
is written through ``_relay_lifecycle.write_line`` so REPORT shares the same
captured native stdout. Reporting is best-effort: writing this diagnostic output
must never change an invocation's result.
"""

import _relay_lifecycle as lifecycle

_MIB = 1024 * 1024


def format_report(request_id, duration_ms, memory_usage):
    """Render the single-line report, omitting memory fields without a value.

    ``memory_usage`` is the invocation's ``_relay_memory.MemoryUsage`` sample.
    """
    parts = [
        f"REPORT RequestId: {request_id}",
        f"Duration: {duration_ms:.2f} ms",
    ]
    if memory_usage.current_bytes is not None:
        parts.append(f"Memory Used: {memory_usage.current_bytes // _MIB} MiB")
    if memory_usage.peak_bytes is not None:
        parts.append(f"Container Memory Peak: {memory_usage.peak_bytes // _MIB} MiB")
    return " ".join(parts)


def report_invocation(request_id, duration_ms, memory_usage):
    """Emit one REPORT line; never raises into the invocation path."""
    try:
        line = format_report(request_id, duration_ms, memory_usage)
    except Exception:
        return
    lifecycle.write_line(line)
