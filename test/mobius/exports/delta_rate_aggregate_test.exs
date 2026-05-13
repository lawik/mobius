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
    {:ok, clock_agent} = Agent.start_link(fn -> 1_700_006_400 end)

    clock_fn = fn -> Agent.get(clock_agent, & &1) end

    advance = fn n ->
      Agent.update(clock_agent, fn t -> t + n end)
    end

    metrics =
      Enum.map(metric_specs, fn
        {:counter, name} -> Telemetry.Metrics.counter(name)
        {:last_value, name} -> Telemetry.Metrics.last_value(name)
      end)

    args = [
      metrics: metrics,
      mobius_instance: instance,
      persistence_dir: tmp_dir,
      clock_fn: clock_fn
    ]

    {:ok, _pid} = start_supervised({Mobius, args})

    scraper_pid = Process.whereis(Module.concat(Mobius.Scraper, instance))

    scrape = fn ->
      send(scraper_pid, :scrape)
      :sys.get_state(scraper_pid)
      :ok
    end

    %{instance: instance, advance: advance, scrape: scrape}
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
    # 1_700_006_400 (offsets 0..59) and 1_700_000_060 (offsets 60..89).
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
end
