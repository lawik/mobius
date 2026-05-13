defmodule Mobius.RealisticTraceTest do
  @moduledoc """
  A side-by-side comparison of what an operator sees today vs. what they
  WOULD see after the consolidation work in IMPROVEMENTS.md.

  The trace below simulates 5 minutes of a small web service. It includes
  four operationally interesting moments — calm baseline, a transient
  spike, a sustained-load minute, an outage, then recovery. Run this test
  with `--trace` (or just look at the test output below) to see the
  minute-resolution table that each pipeline produces from the same input.

  Run:

      mix test test/mobius/realistic_trace_test.exs --trace

  The assertions encode the gap: the old pipeline silently hides three of
  the four events; the new one shows all four.
  """

  use ExUnit.Case, async: true

  alias Mobius.RRD

  @args [days: 60, hours: 48, minutes: 120, seconds: 120]

  # ---------- the trace ----------
  #
  # The trace is a stream of {ts, :request, latency_ms} tuples plus a
  # global "now" cursor. Each second of wall time is one PDP.
  #
  # Minute 1 (t=0..59):   calm — 10 req/s, ~50ms latency.
  # Minute 2 (t=60..119): one bad second at t=90 — 500 requests, one of
  #                       them 5000ms (a slow upstream). Otherwise calm.
  # Minute 3 (t=120..179): sustained heavy load — 100 req/s, ~80ms.
  # Minute 4 (t=180..239): OUTAGE — service is down, zero requests.
  # Minute 5 (t=240..299): recovery — back to calm 10 req/s, ~50ms.

  defp trace do
    # Pseudo-deterministic latency around a baseline. We don't use :rand
    # to keep the test repeatable; the exact numbers don't matter for the
    # assertions — only the structure does.
    base_lat = fn baseline, ts -> baseline + rem(ts * 7, 11) - 5 end

    minute_1 =
      for ts <- 0..59,
          _ <- 1..10,
          do: {ts, :request, base_lat.(50, ts)}

    minute_2_calm =
      for ts <- 60..119,
          ts != 90,
          _ <- 1..10,
          do: {ts, :request, base_lat.(50, ts)}

    minute_2_spike =
      # 500 requests in the same second, plus one 5000ms outlier
      [{90, :request, 5000} | for(_ <- 1..499, do: {90, :request, base_lat.(50, 90)})]

    minute_3 =
      for ts <- 120..179,
          _ <- 1..100,
          do: {ts, :request, base_lat.(80, ts)}

    minute_4 = []

    minute_5 =
      for ts <- 240..299,
          _ <- 1..10,
          do: {ts, :request, base_lat.(50, ts)}

    Enum.sort_by(minute_1 ++ minute_2_calm ++ minute_2_spike ++ minute_3 ++ minute_4 ++ minute_5,
      fn {ts, _, _} -> ts end
    )
  end

  # ---------- the OLD pipeline (today's Mobius) ----------
  #
  # MetricsTable: counter accumulates cumulatively; summary accumulates
  # globally since process start.
  # Scraper: takes one snapshot per second (PDP). RRD.insert places the
  # snapshot at the minute boundary into the minute bucket and discards
  # the rest within ~2 minutes.

  defp run_old(trace) do
    Mobius.MetricsTable.init(mobius_instance: :old_pipeline, persistence_dir: "/tmp")
    # Wipe any state left from a previous run.
    :ets.delete_all_objects(:old_pipeline)

    # Group events by second so we can drive one scrape per second.
    events_by_second = Enum.group_by(trace, fn {ts, _, _} -> ts end)

    # Loop one tick past the last minute boundary so the t=300 sample
    # (which closes out minute 5) lands in the minute bucket.
    Enum.reduce(0..300, RRD.new(@args), fn ts, rrd ->
      # Apply all events for this second to MetricsTable.
      for {^ts, :request, latency} <- Map.get(events_by_second, ts, []) do
        Mobius.MetricsTable.inc_counter(:old_pipeline, [:http, :req])
        Mobius.MetricsTable.put(:old_pipeline, [:http, :lat], :summary, latency)
      end

      # Scraper tick.
      entries = Mobius.MetricsTable.get_entries(:old_pipeline)

      scrape =
        Enum.map(entries, fn {name, type, value, tags} ->
          %{timestamp: ts, name: name, type: type, value: value, tags: tags}
        end)

      RRD.insert(rrd, ts, scrape)
    end)
  end

  # ---------- the NEW pipeline (proposed) ----------
  #
  # Per-second consolidation as PDPs go in (counter: delta over second;
  # summary: merged over second). Then per-minute consolidation rolls
  # those into CDPs. The minute bucket stores one CDP per minute, each
  # CDP representing the FULL minute, not the value at the :00 mark.

  defp run_new(trace) do
    events_by_second = Enum.group_by(trace, fn {ts, _, _} -> ts end)

    # Accumulators reset at each minute boundary.
    initial = %{
      minute_start: 0,
      minute_count: 0,
      minute_summary: nil,
      cdps: []
    }

    final =
      Enum.reduce(0..299, initial, fn ts, acc ->
        events = Map.get(events_by_second, ts, [])
        count = length(events)
        latencies = for {_, :request, l} <- events, do: l

        sec_summary =
          case latencies do
            [] -> nil
            [first | rest] -> Enum.reduce(rest, Mobius.Summary.new(first), &Mobius.Summary.update(&2, &1))
          end

        # Merge this PDP into the open minute accumulator.
        acc = %{
          acc
          | minute_count: acc.minute_count + count,
            minute_summary: merge_summary(acc.minute_summary, sec_summary)
        }

        # If we just completed a minute, emit a CDP and reset.
        if rem(ts + 1, 60) == 0 do
          cdp = %{
            minute_start: acc.minute_start,
            count: acc.minute_count,
            summary: acc.minute_summary && Mobius.Summary.calculate(acc.minute_summary)
          }

          %{acc | cdps: [cdp | acc.cdps], minute_start: ts + 1, minute_count: 0, minute_summary: nil}
        else
          acc
        end
      end)

    Enum.reverse(final.cdps)
  end

  defp merge_summary(nil, b), do: b
  defp merge_summary(a, nil), do: a

  defp merge_summary(a, b) do
    %{
      min: min(a.min, b.min),
      max: max(a.max, b.max),
      accumulated: a.accumulated + b.accumulated,
      accumulated_sqrd: a.accumulated_sqrd + b.accumulated_sqrd,
      reports: a.reports + b.reports
    }
  end

  # ---------- readable views for humans ----------

  defp old_minute_view(rrd) do
    minute_samples =
      rrd
      |> Map.fetch!(:minute)
      |> CircularBuffer.to_list()

    # Pair consecutive minute boundary samples to compute counter delta.
    minute_samples
    |> Enum.flat_map(fn {ts, metrics} ->
      Enum.map(metrics, fn m -> {ts, m.name, m.type, m.value} end)
    end)
    |> Enum.group_by(fn {_ts, name, type, _v} -> {name, type} end)
    |> Enum.map(fn {{name, type}, list} ->
      {{name, type}, Enum.sort_by(list, fn {ts, _, _, _} -> ts end)}
    end)
    |> Enum.into(%{})
  end

  # ---------- the test ----------

  test "operator's view of 5 minutes of web service activity" do
    trace = trace()
    old_rrd = run_old(trace)
    new_cdps = run_new(trace)

    old_view = old_minute_view(old_rrd)

    counter_samples =
      Map.get(old_view, {"http.req", :counter}, [])
      |> Enum.sort_by(fn {ts, _, _, _} -> ts end)

    summary_samples =
      Map.get(old_view, {"http.lat", :summary}, [])
      |> Enum.sort_by(fn {ts, _, _, _} -> ts end)

    # Compute deltas correctly: prev=0 before the first minute boundary.
    rows_with_delta =
      counter_samples
      |> Enum.map_reduce(0, fn {ts, _, _, cum}, prev ->
        {{ts, cum, cum - prev}, cum}
      end)
      |> elem(0)

    # Pretty-print both views so a human running this test can SEE the
    # difference.
    IO.puts(
      "\n  ┌─ OLD pipeline (today's Mobius) — minute-bucket samples at t=60, 120, 180, 240, 300 ─┐"
    )
    IO.puts("  │ window  │ stored counter  │ Δ events  │  stored summary (cumulative since start)    │")

    rows_with_delta
    |> Enum.with_index()
    |> Enum.each(fn {{ts, cum, delta}, idx} ->
      window_label = "m#{idx + 1} (t=#{idx * 60}..#{(idx + 1) * 60 - 1})"

      summary_at_ts =
        Enum.find_value(summary_samples, fn {sts, _, _, v} ->
          if sts == ts, do: Mobius.Summary.calculate(v)
        end)

      lat_view =
        if summary_at_ts do
          "min=#{summary_at_ts.min} max=#{summary_at_ts.max} avg=#{Float.round(summary_at_ts.average / 1, 1)}"
        else
          "(no sample)"
        end

      IO.puts(
        "  │ #{String.pad_trailing(window_label, 19)} │ cum=#{cum |> Integer.to_string() |> String.pad_leading(5)} │ Δ=#{delta |> Integer.to_string() |> String.pad_leading(5)}    │ #{lat_view}"
      )
    end)

    IO.puts("  └─────────────────────────────────────────────────────────────────────────────────────┘\n")

    IO.puts("  ┌─ NEW pipeline (proposed CDPs) — one CDP per minute window ─┐")
    IO.puts("  │ window               │ events in window │ summary FOR THAT WINDOW       │")

    new_cdps
    |> Enum.with_index()
    |> Enum.each(fn {cdp, idx} ->
      window_label = "m#{idx + 1} (t=#{idx * 60}..#{(idx + 1) * 60 - 1})"

      lat_view =
        case cdp.summary do
          nil -> "(no requests)"
          s -> "min=#{s.min} max=#{s.max} avg=#{Float.round(s.average / 1, 1)}"
        end

      IO.puts(
        "  │ #{String.pad_trailing(window_label, 20)} │ count=#{cdp.count |> Integer.to_string() |> String.pad_leading(5)}      │ #{lat_view}"
      )
    end)

    IO.puts("  └─────────────────────────────────────────────────────────────┘")

    # -------- assertions: what the OLD pipeline hides --------

    # The 5000ms outlier at t=90 dominates the global summary forever.
    # Every minute sample from m2 onward shows max=5000, even after the
    # spike was minutes of normal traffic ago.
    post_spike_summaries =
      summary_samples
      |> Enum.drop(1)
      |> Enum.map(fn {_ts, _, _, data} -> Mobius.Summary.calculate(data).max end)

    assert Enum.all?(post_spike_summaries, &(&1 == 5000)),
           "old pipeline: every post-spike minute shows max=5000 forever, hiding when latency recovered"

    # The OUTAGE in minute 4 is only detectable by computing pairwise
    # deltas of the stored cumulative counter. The raw stored values for
    # m3 and m4 are nearly identical — the cumulative line is essentially
    # a flat horizontal segment across the outage, with no marker. Only
    # an explicit diff exposes it.
    [_, _, {_, _, m3_delta}, {_, _, m4_delta}, _] = rows_with_delta

    assert m4_delta < m3_delta / 100,
           "outage detectable only by diffing: m3 Δ=#{m3_delta}, m4 Δ=#{m4_delta}"

    # -------- assertions: what the NEW pipeline shows --------

    [m1, m2, m3, m4, m5] = new_cdps

    # Each minute's count tells the story directly:
    assert m1.count == 600,         "calm minute"
    assert m2.count == 1090,        "spike minute (590 calm + 500 burst)"
    assert m3.count == 6000,        "sustained-load minute"
    assert m4.count == 0,           "outage"
    assert m5.count == 600,         "recovery"

    # Latency is bounded to the minute it actually happened in:
    assert m1.summary.max < 100
    assert m2.summary.max == 5000
    assert m3.summary.max < 200,
           "m3 max reflects m3's latency, not m2's spike"
    assert m4.summary == nil,
           "outage minute has no summary at all"
    assert m5.summary.max < 100,
           "m5 max reflects recovery latency, not m2's spike"

    # -------- assertions: what the NEW pipeline shows --------

    [m1, m2, m3, m4, m5] = new_cdps

    # The spike minute jumps out: ~1100 events (590 normal + 500 burst)
    # vs ~600 in calm minutes, and max latency is 5000 in m2 only.
    assert m1.count == 600
    assert m2.count > 1000
    assert m3.count == 6000
    assert m4.count == 0
    assert m5.count == 600

    # Latency tail is bounded to the minute it actually happened in:
    assert m1.summary.max < 100
    assert m2.summary.max == 5000
    assert m3.summary.max < 200,
           "minute 3 max should reflect minute 3's latency, not minute 2's spike"

    # Outage minute has no summary (no requests reported).
    assert m4.summary == nil

    # Recovery is visible — latency is back to normal:
    assert m5.summary.max < 100,
           "minute 5 max should reflect minute 5's latency, not the spike from minute 2"
  end
end
