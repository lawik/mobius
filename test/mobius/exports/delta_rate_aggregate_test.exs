defmodule Mobius.Exports.DeltaRateAggregateTest do
  @moduledoc """
  Tests for `Mobius.Exports.delta/4`, `rate/4`, and `aggregate/4`.

  These helpers run on top of the metrics stored by `Mobius.Scraper`.
  Rather than spin up the full supervision tree and wait on the 1Hz
  timer, we drive the scraper directly with an injected clock so the
  stored samples land at known timestamps.
  """

  use ExUnit.Case, async: false

  alias Mobius.Exports

  defp start_pipeline(metric_specs, tmp_dir) do
    test_id = System.unique_integer([:positive])
    instance = :"exports_helpers_#{test_id}"

    {:ok, _} = start_supervised({Mobius.ManualClock, 1_700_006_400})

    metrics =
      Enum.map(metric_specs, fn
        {:counter, name} -> Telemetry.Metrics.counter(name)
        {:last_value, name} -> Telemetry.Metrics.last_value(name)
      end)

    args = [
      metrics: metrics,
      mobius_instance: instance,
      persistence_dir: tmp_dir,
      clock: Mobius.ManualClock
    ]

    {:ok, _pid} = start_supervised({Mobius, args})

    scraper_pid = Process.whereis(Module.concat(Mobius.Scraper, instance))

    scrape = fn ->
      send(scraper_pid, :scrape)
      :sys.get_state(scraper_pid)
      :ok
    end

    %{instance: instance, advance: &Mobius.ManualClock.advance/1, scrape: scrape}
  end

  @tag :tmp_dir
  test "delta/4 returns events per scrape interval for a counter", %{tmp_dir: tmp_dir} do
    %{instance: instance, advance: advance, scrape: scrape} =
      start_pipeline([{:counter, "rates.evt.count"}], tmp_dir)

    emit = fn n ->
      for _ <- 1..n, do: :telemetry.execute([:rates, :evt], %{count: 1}, %{})
    end

    # tick 0: 5 events, scrape
    emit.(5)
    scrape.()

    # tick 1 (+10s): 3 more events, scrape
    advance.(10)
    emit.(3)
    scrape.()

    # tick 2 (+10s): 7 more events, scrape
    advance.(10)
    emit.(7)
    scrape.()

    deltas = Exports.delta("rates.evt.count", :counter, %{}, mobius_instance: instance, from: 0)

    # Cumulative samples were [5, 8, 15]. Pairwise diffs: [3, 7].
    assert deltas == [{1_700_006_410, 3}, {1_700_006_420, 7}]
  end

  @tag :tmp_dir
  test "rate/4 divides each delta by elapsed seconds", %{tmp_dir: tmp_dir} do
    %{instance: instance, advance: advance, scrape: scrape} =
      start_pipeline([{:counter, "rates.evt.count"}], tmp_dir)

    emit = fn n ->
      for _ <- 1..n, do: :telemetry.execute([:rates, :evt], %{count: 1}, %{})
    end

    emit.(5)
    scrape.()

    # +5s, 10 more events → 10/5 = 2.0
    advance.(5)
    emit.(10)
    scrape.()

    # +20s, 10 more events → 10/20 = 0.5
    advance.(20)
    emit.(10)
    scrape.()

    rates = Exports.rate("rates.evt.count", :counter, %{}, mobius_instance: instance, from: 0)

    assert rates == [{1_700_006_405, 2.0}, {1_700_006_425, 0.5}]
  end

  @tag :tmp_dir
  test "aggregate/4 :max over minute buckets surfaces transient peaks", %{tmp_dir: tmp_dir} do
    %{instance: instance, advance: advance, scrape: scrape} =
      start_pipeline([{:last_value, "rates.mem.bytes"}], tmp_dir)

    # Drive 90 seconds of memory measurements. Most values are 100, but
    # at t=30 we spike to 1000 and at t=75 to 500. A user plotting :avg
    # over a minute would barely see those; :max keeps them visible.
    Enum.each(0..89, fn offset ->
      value =
        cond do
          offset == 30 -> 1000
          offset == 75 -> 500
          true -> 100
        end

      :telemetry.execute([:rates, :mem], %{bytes: value}, %{})
      scrape.()
      advance.(1)
    end)

    max_per_minute =
      Exports.aggregate("rates.mem.bytes", :last_value, %{},
        bucket: :minute,
        function: :max,
        mobius_instance: instance,
        from: 0
      )

    # The trace started at 1_700_006_400. Minute buckets land at
    # 1_700_006_400 (offsets 0..59) and 1_700_006_460 (offsets 60..89).
    assert max_per_minute == [
             {1_700_006_400, 1000},
             {1_700_006_460, 500}
           ]
  end

  @tag :tmp_dir
  test "aggregate/4 with :avg returns the arithmetic mean over each bucket", %{tmp_dir: tmp_dir} do
    %{instance: instance, advance: advance, scrape: scrape} =
      start_pipeline([{:last_value, "rates.mem.bytes"}], tmp_dir)

    Enum.each(0..59, fn offset ->
      :telemetry.execute([:rates, :mem], %{bytes: offset}, %{})
      scrape.()
      advance.(1)
    end)

    avg =
      Exports.aggregate("rates.mem.bytes", :last_value, %{},
        bucket: :minute,
        function: :avg,
        mobius_instance: instance,
        from: 0
      )

    expected_avg = Enum.sum(0..59) / 60
    assert [{1_700_006_400, ^expected_avg}] = avg
  end

  @tag :tmp_dir
  test "delta/4 reads counter CDPs as already-deltas across the seconds boundary",
       %{tmp_dir: tmp_dir} do
    # Drive 4 full minutes of one event per second. The first ~3 minutes
    # of raw PDPs rotate out of the seconds bucket (default size 180),
    # so the oldest part of the series is served as minute CDPs while
    # the most recent part is served as raw cumulative PDPs.
    #
    # If delta/4 naively subtracted adjacent values across the boundary,
    # we'd get either delta-of-deltas (negative) or a giant spike where
    # the CDP value (~59) meets the next cumulative reading. The fix
    # returns CDP values as-is and only subtracts between adjacent PDPs.
    %{instance: instance, advance: advance, scrape: scrape} =
      start_pipeline([{:counter, "boundary.evt.count"}], tmp_dir)

    Enum.each(0..(4 * 60), fn _ ->
      :telemetry.execute([:boundary, :evt], %{count: 1}, %{})
      scrape.()
      advance.(1)
    end)

    deltas =
      Exports.delta("boundary.evt.count", :counter, %{}, mobius_instance: instance, from: 0)

    values = Enum.map(deltas, fn {_ts, v} -> v end)

    # A closed minute CDP reports `last - first` = 59 (cumulative
    # 60 minus cumulative 1) — the very first event of each period
    # is the baseline. Adjacent PDPs report 1 each.
    assert Enum.any?(values, &(&1 == 59)),
           "expected at least one minute CDP reporting ~59 events, got #{inspect(values)}"

    refute Enum.any?(values, &(&1 < 0)),
           "delta values should never be negative, got #{inspect(values)}"

    refute Enum.any?(values, &(&1 > 60)),
           "no delta should exceed a full minute's worth of events, got #{inspect(values)}"
  end

  @tag :tmp_dir
  test "rate/4 divides CDPs by their period_seconds, not the wall delta",
       %{tmp_dir: tmp_dir} do
    %{instance: instance, advance: advance, scrape: scrape} =
      start_pipeline([{:counter, "boundary.rate.count"}], tmp_dir)

    Enum.each(0..(4 * 60), fn _ ->
      :telemetry.execute([:boundary, :rate], %{count: 1}, %{})
      scrape.()
      advance.(1)
    end)

    rates = Exports.rate("boundary.rate.count", :counter, %{}, mobius_instance: instance, from: 0)

    # One event per second sustained → every emitted rate should be
    # close to 1.0/s. PDP pairs are exactly 1.0; CDPs are 59/60 ≈ 0.983
    # (the first sample of each period establishes the baseline and
    # isn't itself counted). The point is that boundary crossings
    # don't produce wild values.
    for {_ts, r} <- rates do
      assert_in_delta r, 1.0, 0.02
    end
  end

  test "aggregate/4 refuses :counter with a pointer at delta/rate" do
    assert_raise ArgumentError, ~r/cannot operate on :counter/, fn ->
      Exports.aggregate("any.counter", :counter, %{}, bucket: :minute)
    end
  end

  test "aggregate/4 refuses :sum with a pointer at delta/rate" do
    assert_raise ArgumentError, ~r/cannot operate on :sum/, fn ->
      Exports.aggregate("any.sum", :sum, %{}, bucket: :minute)
    end
  end
end
