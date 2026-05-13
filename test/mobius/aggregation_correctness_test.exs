defmodule Mobius.AggregationCorrectnessTest do
  @moduledoc """
  These tests simulate usage over time and probe what the user actually sees
  when they query historical metrics through Mobius. They aim to answer the
  question: "Can I trust the numbers I get back?"

  Several behaviors documented here are surprising enough that they explain
  why aggregation over time can appear to "yield weird results." Each test
  is named to describe what is happening — the assertions are written as
  pinning tests, capturing CURRENT behavior, not necessarily correct behavior.
  Where behavior is suspect, the test body says so.
  """

  use ExUnit.Case, async: true

  alias Mobius.RRD

  @args [days: 60, hours: 48, minutes: 120, seconds: 120]

  # Helper: feed `count` sequential scrapes starting at `ts`, one per second,
  # with the metric value supplied by `value_fun.(ts)`.
  defp feed(rrd, start_ts, count, value_fun) do
    Enum.reduce(0..(count - 1), rrd, fn offset, acc ->
      ts = start_ts + offset
      RRD.insert(acc, ts, value_fun.(ts))
    end)
  end

  describe "RRD insert is decimation, not aggregation" do
    test "each scrape lands in exactly ONE resolution bucket" do
      # If we feed 121 consecutive seconds of scrapes, RRD will distribute them:
      # - the first scrape lands in the day bucket (because day_next starts at 0)
      # - scrapes between the first and the next minute boundary land in seconds
      # - the minute-boundary scrape lands in the minute bucket
      #
      # No scrape is ever placed into more than one bucket. There is no
      # cross-bucket averaging. This is what surprises people who expect "RRD"
      # to mean "aggregated rolling averages".

      # Start exactly on a minute boundary so the trace is predictable.
      ts0 = 60_000
      rrd = feed(RRD.new(@args), ts0, 121, &(&1 * 1.0))

      all = RRD.all(rrd)
      timestamps = Enum.map(all, fn {ts, _} -> ts end)

      # No duplicate timestamps across buckets:
      assert length(timestamps) == length(Enum.uniq(timestamps))

      # The very first scrape went to the *day* bucket because day_next was 0
      # at construction time. This is itself a quirk: the first measurement
      # after process start always becomes a "day sample" regardless of when
      # the process started in the calendar day.
      day_samples = rrd_buckets(rrd).day
      assert day_samples == [{ts0, ts0 * 1.0}]
    end

    test "minute-resolution data point is the value AT the boundary, not an average over the minute" do
      # Feed a sawtooth: value = (ts mod 60). The average per minute is ~29.5,
      # min is 0, max is 59. But Mobius will only keep the value at the minute
      # boundary itself, which (because (60k mod 60) == 0) is always 0.

      ts0 = 60_000
      rrd = feed(RRD.new(@args), ts0, 600, &rem(&1, 60))

      buckets = rrd_buckets(rrd)
      minute_values = Enum.map(buckets.minute, fn {_ts, v} -> v end)

      # Every minute-bucket sample equals the value at the minute boundary,
      # which for our sawtooth is always 0:
      assert Enum.all?(minute_values, &(&1 == 0)),
             "Minute samples: #{inspect(minute_values)} — they should *equal* the at-boundary value, not the mean over the minute."

      # Meanwhile the *actual* mean over a one-minute window would be ~29.5.
      sample_mean = Enum.sum(0..59) / 60
      assert_in_delta sample_mean, 29.5, 0.001

      # A user plotting "minute resolution" of this metric will see flat zero
      # and conclude the metric never moves — even though it sweeps 0..59
      # every minute.
    end

    test "transient spikes between boundaries are invisible at coarser resolutions" do
      # Simulate a single 5-second spike (value = 1000) starting at second
      # offset 10, embedded in an otherwise quiet stream (value = 0).
      # Resolution at the second level keeps everything for ~2 minutes; at the
      # minute level, only the value at the :00 second mark each minute is kept.

      ts0 = 60_000
      value_fun = fn ts ->
        offset = ts - ts0
        if offset >= 10 and offset < 15, do: 1000, else: 0
      end

      # Run long enough that the second-resolution buffer wraps and only the
      # minute resolution retains anything from the spike window.
      rrd = feed(RRD.new(@args), ts0, 121 + 120 + 10, value_fun)

      buckets = rrd_buckets(rrd)

      # The spike (1000) ought to have happened at offsets 10..14. The minute
      # samples are taken at offsets 0, 60, 120, ... — none of which see the
      # spike.
      assert Enum.all?(buckets.minute, fn {_ts, v} -> v == 0 end),
             "Spike is gone from minute resolution because it didn't land on a minute boundary."

      # Once the second buffer wraps (after ~120s + the spike sits at offset 10),
      # the spike is gone from second resolution too:
      second_values = Enum.map(buckets.second, fn {_ts, v} -> v end)
      refute 1000 in second_values,
             "Second buffer has wrapped; spike value is no longer in any bucket."

      # Net effect: a real 5-second spike of 1000 has been silently dropped
      # from history once enough time passes. There is no trace of it.
    end

    test "scrapes that arrive after the next-boundary check skip intermediate buckets" do
      # If a scrape is delayed (system pause, GC, etc.) past a minute boundary,
      # the gap is NOT backfilled — we get one minute sample for the new ts,
      # not one per missed minute.
      rrd =
        RRD.new(@args)
        |> RRD.insert(60_000, :first)
        # 5 minutes later, no intermediate scrapes:
        |> RRD.insert(60_300, :much_later)

      buckets = rrd_buckets(rrd)

      # We get one day-bucket entry (the very first insert), one minute entry,
      # but no intermediate minute entries:
      assert length(buckets.day) == 1
      assert length(buckets.minute) == 1
      assert length(buckets.hour) == 0
      assert length(buckets.second) == 0

      # So the historical chart is sparse where the system was paused — but
      # the SPACING of points doesn't make it obvious; the consumer must
      # inspect timestamps to detect the gap.
    end
  end

  describe "counters and sums are cumulative forever" do
    test "MetricsTable counters never reset, so the 'value at ts' grows monotonically" do
      # Set up a fresh table and increment a counter many times.
      table = :"counter_cumulative_#{System.unique_integer([:positive])}"
      Mobius.MetricsTable.init(mobius_instance: table, persistence_dir: "/tmp")

      Enum.each(1..1_000, fn _ -> Mobius.MetricsTable.inc_counter(table, [:my, :counter]) end)

      [{_name, :counter, value_after_1k, _meta}] =
        Mobius.MetricsTable.get_entries_by_metric_name(table, "my.counter")

      assert value_after_1k == 1000

      Enum.each(1..500, fn _ -> Mobius.MetricsTable.inc_counter(table, [:my, :counter]) end)

      [{_name, :counter, value_after_1500, _meta}] =
        Mobius.MetricsTable.get_entries_by_metric_name(table, "my.counter")

      assert value_after_1500 == 1500

      # Implication: if Scraper samples this every second, the time series is
      # always the cumulative count since process start. To answer "events
      # per minute" the consumer must take pairwise differences. Mobius does
      # not do that for you.
    end

    test "sum metric accumulates across the entire process lifetime" do
      table = :"sum_cumulative_#{System.unique_integer([:positive])}"
      Mobius.MetricsTable.init(mobius_instance: table, persistence_dir: "/tmp")

      Enum.each(1..100, fn n -> Mobius.MetricsTable.update_sum(table, [:my, :sum], n) end)

      [{_name, :sum, value, _meta}] =
        Mobius.MetricsTable.get_entries_by_metric_name(table, "my.sum")

      # Sum of 1..100 is 5050.
      assert value == 5050

      # Add another batch:
      Enum.each(1..50, fn _ -> Mobius.MetricsTable.update_sum(table, [:my, :sum], 10) end)

      [{_name, :sum, value, _meta}] =
        Mobius.MetricsTable.get_entries_by_metric_name(table, "my.sum")

      assert value == 5050 + 500

      # Same as counter: the time series for a Sum metric is a running total
      # since the process started. There is no "sum over the last minute".
    end
  end

  describe "summary metric accumulates from process start" do
    test "min/max/avg cover the ENTIRE history of values ever reported, not the last bucket" do
      table = :"summary_global_#{System.unique_integer([:positive])}"
      Mobius.MetricsTable.init(mobius_instance: table, persistence_dir: "/tmp")

      # Report a single extreme value 10 minutes ago (simulated), then 1000
      # ordinary values.
      Mobius.MetricsTable.put(table, [:lat, :ms], :summary, 100_000)

      Enum.each(1..1000, fn _ ->
        Mobius.MetricsTable.put(table, [:lat, :ms], :summary, 50)
      end)

      [{_name, :summary, data, _meta}] =
        Mobius.MetricsTable.get_entries_by_metric_name(table, "lat.ms")

      calc = Mobius.Summary.calculate(data)

      # The single extreme value pollutes max forever — even though it was a
      # one-off ten minutes ago.
      assert calc.max == 100_000

      # And the "average" is dragged up by that one value:
      assert calc.average > 50
      assert_in_delta calc.average, (100_000 + 1000 * 50) / 1001, 0.01

      # When the scraper writes this summary into the RRD every second, every
      # minute-resolution sample shows max=100_000. A user plotting "max
      # latency over the last hour" will think the system is constantly at
      # 100s — when in reality it had one spike ten minutes ago and is calm now.
    end
  end

  describe "Scraper-to-RRD: what 60 days of decimation looks like" do
    test "after 60 days of 1Hz scrapes, only 348 raw points remain regardless of metric volatility" do
      # This replicates the existing 'fill up the all buffers' test but stresses
      # the implication: it does not matter how busy the system is — across 60
      # days, only 60 + 48 + 120 + 120 = 348 SAMPLES of each metric are stored.
      now = 60 * 86400

      buffer =
        Enum.reduce(
          0..(now - 1),
          RRD.new(@args),
          &RRD.insert(&2, &1, &1)
        )

      total = length(RRD.query(buffer, 0))
      assert total == 60 + 48 + 120 + 120
      assert total == 348

      # Each sample is the metric's value at one specific second. Of every
      # 60 seconds, 59 are immediately discarded; of every 60 minutes, 59
      # are discarded; etc.
      #
      # For a stable metric (last_value of memory) this is fine.
      # For a noisy metric (rate of events) this is misleading: the day-level
      # point captures whatever was happening in that one second at midnight.
    end

    test "the very first scrape always lands in the DAY bucket regardless of wall clock" do
      # This is an artifact of `day_next` initializing to 0 in RRD.new/1.
      # The first scrape after process start always satisfies `ts >= day_next`
      # and therefore becomes a 'day-resolution sample' — even if it was 3pm
      # on a random afternoon when the process booted.
      rrd = RRD.insert(RRD.new(@args), 1_700_000_123, :first_value)
      buckets = rrd_buckets(rrd)

      assert buckets.day == [{1_700_000_123, :first_value}]
      assert buckets.hour == []
      assert buckets.minute == []
      assert buckets.second == []

      # So when this process is restarted, you get an extra "day" datapoint
      # at restart time that does not correspond to a calendar-day boundary.
      # Across many restarts, the 'day' bucket fills up with samples taken
      # at unrelated points in the day. Plotting "60-day trend" thus mixes
      # midnight samples with restart samples.
    end
  end

  describe "simulated scraper: telemetry → MetricsTable → RRD over fake time" do
    # These tests bypass the live Scraper (which uses :timer.send_interval and
    # `System.system_time(:second)` — neither is mockable here) and instead
    # replay its per-tick logic by hand:
    #
    #   1. write to MetricsTable via the real handler (Mobius.Events.handle_metrics/4),
    #      so all the same code paths fire that telemetry would trigger;
    #   2. on each simulated tick, snapshot MetricsTable and call RRD.insert/3
    #      with an explicit timestamp — exactly what Scraper.handle_info(:scrape, _)
    #      does (see lib/mobius/scraper.ex:130-142).
    #
    # No Process.sleep. No real clock.

    setup do
      # Each test gets its own table name so they don't collide.
      table = :"sim_#{System.unique_integer([:positive])}"
      Mobius.MetricsTable.init(mobius_instance: table, persistence_dir: "/tmp")
      {:ok, %{table: table}}
    end

    test "counter exported as time series is cumulative — not events-per-tick", %{table: table} do
      metric = Telemetry.Metrics.counter("ecorrect.evt.count")
      handler_config = %{table: table, metrics: [metric]}

      # Fake clock starts at t0; one tick = one simulated second.
      t0 = 1_700_000_000

      # Tick 0: emit 5 events, then scrape.
      Enum.each(1..5, fn _ ->
        Mobius.Events.handle_metrics([:ecorrect, :evt], %{}, %{}, handler_config)
      end)

      rrd = tick(RRD.new(@args), table, t0)

      # Tick 1: emit 3 more, then scrape.
      Enum.each(1..3, fn _ ->
        Mobius.Events.handle_metrics([:ecorrect, :evt], %{}, %{}, handler_config)
      end)

      rrd = tick(rrd, table, t0 + 1)

      series = series_for(rrd, "ecorrect.evt.count", :counter)

      # Monotonic, cumulative — counters in Mobius never reset.
      assert series == [5, 8],
             "expected cumulative [5, 8], got #{inspect(series)}"

      # A user expecting 'events per tick' (5 then 3) would be wrong.
    end

    test "last_value loses transient between-scrape values", %{table: table} do
      metric = Telemetry.Metrics.last_value("ecorrect.transient.value")
      handler_config = %{table: table, metrics: [metric]}

      t0 = 1_700_000_000

      # Between two scrapes, fire 1 → spike to 999_999 → settle to 2.
      Mobius.Events.handle_metrics([:ecorrect, :transient], %{value: 1}, %{}, handler_config)
      Mobius.Events.handle_metrics([:ecorrect, :transient], %{value: 999_999}, %{}, handler_config)
      Mobius.Events.handle_metrics([:ecorrect, :transient], %{value: 2}, %{}, handler_config)

      # Single scrape captures only the final value at tick time.
      rrd = tick(RRD.new(@args), table, t0)

      series = series_for(rrd, "ecorrect.transient.value", :last_value)

      # 999_999 was never seen by the scraper.
      assert series == [2],
             "spike 999_999 should be invisible; got #{inspect(series)}"
    end

    test "summary spike at t=0 pollutes every later sample (no per-bucket reset)", %{table: table} do
      metric = Telemetry.Metrics.summary("ecorrect.lat.ms")
      handler_config = %{table: table, metrics: [metric]}

      t0 = 1_700_000_000

      # One bad outlier at the start.
      Mobius.Events.handle_metrics([:ecorrect, :lat], %{ms: 100_000}, %{}, handler_config)

      # Now 600 ordinary measurements spread across 60 simulated minutes.
      # Each "minute" we report 10 values around 50ms, then scrape.
      rrd =
        Enum.reduce(0..59, RRD.new(@args), fn minute_offset, acc ->
          Enum.each(1..10, fn _ ->
            Mobius.Events.handle_metrics(
              [:ecorrect, :lat],
              %{ms: 50},
              %{},
              handler_config
            )
          end)

          tick(acc, table, t0 + minute_offset * 60)
        end)

      # Every minute-resolution summary sample carries the *all-time* max:
      minute_samples = CircularBuffer.to_list(rrd.minute)

      max_values =
        for {_ts, [%{type: :summary, value: data}]} <- minute_samples do
          Mobius.Summary.calculate(data).max
        end

      assert length(max_values) > 0
      assert Enum.all?(max_values, &(&1 == 100_000)),
             "every minute sample's max should equal the all-time spike 100_000; got #{inspect(max_values)}"

      # User reading "max latency over the last hour" sees 100_000 forever,
      # even though the spike was one measurement an hour ago and never
      # repeated.
    end

    test "spike that falls between minute boundaries is gone after the second buffer wraps", %{
      table: table
    } do
      metric = Telemetry.Metrics.last_value("ecorrect.spike.value")
      handler_config = %{table: table, metrics: [metric]}

      t0 = 1_700_000_000

      # Spike for 5 ticks (offset 10..14), 0 otherwise.
      rrd =
        Enum.reduce(0..(121 + 120 + 10), RRD.new(@args), fn offset, acc ->
          value = if offset >= 10 and offset < 15, do: 1000, else: 0

          Mobius.Events.handle_metrics(
            [:ecorrect, :spike],
            %{value: value},
            %{},
            handler_config
          )

          tick(acc, table, t0 + offset)
        end)

      buckets = rrd_buckets(rrd)

      # The spike happened at offsets 10..14. Minute boundaries are at
      # offsets 0, 60, 120, etc. None of those see 1000.
      minute_values = Enum.map(buckets.minute, fn {_ts, [m]} -> m.value end)
      assert Enum.all?(minute_values, &(&1 == 0)),
             "minute buckets shouldn't contain the spike; got #{inspect(minute_values)}"

      # The second buffer has rotated past the spike too:
      second_values = Enum.map(buckets.second, fn {_ts, [m]} -> m.value end)
      refute 1000 in second_values,
             "second buffer wrapped past spike; got #{inspect(second_values)}"

      # No bucket retains evidence of the spike. It is gone.
    end
  end

  # ---------------- simulated-scraper helpers ----------------

  # Replays Scraper.handle_info(:scrape, _) with a chosen timestamp.
  defp tick(rrd, table, ts) do
    case Mobius.MetricsTable.get_entries(table) do
      [] ->
        rrd

      entries ->
        scrape =
          Enum.map(entries, fn {name, type, value, tags} ->
            %{timestamp: ts, name: name, type: type, value: value, tags: tags}
          end)

        RRD.insert(rrd, ts, scrape)
    end
  end

  # Replays Exports.metrics(...) |> Enum.map(& &1.value) over an RRD.
  defp series_for(rrd, metric_name, type) do
    rrd
    |> RRD.all()
    |> Enum.flat_map(fn {_ts, metrics} -> metrics end)
    |> Enum.filter(&(&1.name == metric_name and &1.type == type))
    |> Enum.map(& &1.value)
  end

  # ---------------- helpers ----------------

  defp rrd_buckets(rrd) do
    %{
      day: CircularBuffer.to_list(rrd.day),
      hour: CircularBuffer.to_list(rrd.hour),
      minute: CircularBuffer.to_list(rrd.minute),
      second: CircularBuffer.to_list(rrd.second)
    }
  end
end
