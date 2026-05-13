defmodule Mobius.InfoTest do
  @moduledoc """
  Tests for `Mobius.info/0`. Specifically pinned: after the
  reset-on-scrape change to MetricsTable, info/0 for `:summary` metrics
  must read from the consolidator's latest closed-window CDP — the
  metrics table only holds sub-second data and can't usefully describe
  recent latency / response sizes / etc on its own.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  @tag :tmp_dir
  test "info/0 prints summary statistics from the latest closed minute CDP, not the now-degenerate metrics table",
       %{tmp_dir: tmp_dir} do
    metric = Telemetry.Metrics.summary("info_test.lat.ms")
    {:ok, clock_agent} = Agent.start_link(fn -> 1_700_006_400 end)
    clock_fn = fn -> Agent.get(clock_agent, & &1) end
    advance = fn n -> Agent.update(clock_agent, &(&1 + n)) end

    instance = :"info_test_#{System.unique_integer([:positive])}"

    args = [
      metrics: [metric],
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

    # Two full minutes of latency reports. Minute 1 has one outlier at
    # 5000ms; minute 2 is calm at ~50ms. The first minute's CDP closes
    # when the second minute's first scrape arrives.
    for offset <- 0..59 do
      value = if offset == 30, do: 5000, else: 50
      :telemetry.execute([:info_test, :lat], %{ms: value}, %{})
      scrape.()
      advance.(1)
    end

    for _offset <- 0..59 do
      :telemetry.execute([:info_test, :lat], %{ms: 50}, %{})
      scrape.()
      advance.(1)
    end

    # One more scrape to close minute 2.
    scrape.()

    output = capture_io(fn -> Mobius.info(instance) end)

    assert output =~ "Metric Name: info_test.lat.ms"
    # The latest CDP is for minute 2 — no spike, max == 50.
    assert output =~ "summary:"
    assert output =~ "max: 50"

    # If info/0 were still reading the per-tick MetricsTable summary,
    # we'd see at most one report; instead the CDP describes a full
    # minute of data.
    refute output =~ "reports: 1"
  end
end
