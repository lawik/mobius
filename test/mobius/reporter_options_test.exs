defmodule Mobius.ReporterOptionsTest do
  @moduledoc """
  Per-metric `reporter_options: [consolidate: ...]` on `:last_value`.

  The default consolidator for `:last_value` is `:avg`. Users who care
  about peaks (memory pressure, queue depth, response size) can switch
  to `:max`, `:min`, or `:last`.

  Coverage:

    * The Consolidator honors the `:consolidate` field on a metric map
      (unit-level — guarantees the consolidation switch works).
    * The Scraper plumbs `reporter_options[:consolidate]` from each
      metric definition onto every scrape map entry (integration-level
      — guarantees the option survives the trip from definition to
      consolidation).
  """

  use ExUnit.Case, async: false

  alias Mobius.Consolidator

  @args [days: 60, hours: 48, minutes: 120, seconds: 180]

  defp metric(name, value, opts \\ %{}) do
    Map.merge(
      %{name: name, type: :last_value, value: value, tags: %{}, timestamp: 0},
      opts
    )
  end

  defp closed_minute_cdp(state, ts) do
    state.minute
    |> CircularBuffer.to_list()
    |> Enum.find(fn {bucket_ts, _} -> bucket_ts == ts end)
    |> elem(1)
    |> hd()
  end

  describe "Consolidator honors :consolidate on last_value metrics" do
    test "default (no :consolidate field) uses :avg" do
      base = 1_700_006_400

      state =
        Enum.reduce(0..60, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [metric("probe.value", offset)])
        end)

      cdp = closed_minute_cdp(state, base)
      assert_in_delta cdp.value, 29.5, 0.001
    end

    test ":consolidate=:max keeps the peak value over the period" do
      base = 1_700_006_400

      state =
        Enum.reduce(0..60, Consolidator.new(@args), fn offset, st ->
          value = if offset == 30, do: 1000, else: 10

          Consolidator.insert(st, base + offset, [
            metric("probe.value", value, %{consolidate: :max})
          ])
        end)

      cdp = closed_minute_cdp(state, base)
      assert cdp.value == 1000
    end

    test ":consolidate=:min keeps the trough" do
      base = 1_700_006_400

      state =
        Enum.reduce(0..60, Consolidator.new(@args), fn offset, st ->
          value = if offset == 30, do: 5, else: 100

          Consolidator.insert(st, base + offset, [
            metric("probe.value", value, %{consolidate: :min})
          ])
        end)

      cdp = closed_minute_cdp(state, base)
      assert cdp.value == 5
    end

    test ":consolidate=:last keeps the most recent value before the close" do
      base = 1_700_006_400

      state =
        Enum.reduce(0..60, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [
            metric("probe.value", offset * 2, %{consolidate: :last})
          ])
        end)

      # CDP closes when ts=base+60 arrives, so the last sample fed to
      # the accumulator was offset=59 (value=118). Sample at offset=60
      # opens the next minute.
      cdp = closed_minute_cdp(state, base)
      assert cdp.value == 118
    end
  end

  describe "Scraper plumbs reporter_options[:consolidate] onto scrape map entries" do
    @tag :tmp_dir
    test "the :consolidate value is attached to each scrape", %{tmp_dir: tmp_dir} do
      metric_def =
        Telemetry.Metrics.last_value("ropts_probe.value",
          reporter_options: [consolidate: :max]
        )

      instance = :"ropts_plumb_#{System.unique_integer([:positive])}"

      {:ok, clock_agent} = Agent.start_link(fn -> 1_700_006_400 end)
      clock_fn = fn -> Agent.get(clock_agent, & &1) end

      args = [
        metrics: [metric_def],
        mobius_instance: instance,
        persistence_dir: tmp_dir,
        clock_fn: clock_fn
      ]

      {:ok, _pid} = start_supervised({Mobius, args})

      :telemetry.execute([:ropts_probe], %{value: 42}, %{})

      scraper_pid = Process.whereis(Module.concat(Mobius.Scraper, instance))
      send(scraper_pid, :scrape)
      :sys.get_state(scraper_pid)

      latest = Mobius.Scraper.latest_for(instance, "ropts_probe.value", :last_value, %{})
      assert latest.consolidate == :max
    end

    @tag :tmp_dir
    test "metrics without reporter_options don't get a :consolidate field", %{tmp_dir: tmp_dir} do
      metric_def = Telemetry.Metrics.last_value("ropts_plain.value")

      instance = :"ropts_plain_#{System.unique_integer([:positive])}"

      {:ok, clock_agent} = Agent.start_link(fn -> 1_700_006_400 end)
      clock_fn = fn -> Agent.get(clock_agent, & &1) end

      args = [
        metrics: [metric_def],
        mobius_instance: instance,
        persistence_dir: tmp_dir,
        clock_fn: clock_fn
      ]

      {:ok, _pid} = start_supervised({Mobius, args})

      :telemetry.execute([:ropts_plain], %{value: 7}, %{})

      scraper_pid = Process.whereis(Module.concat(Mobius.Scraper, instance))
      send(scraper_pid, :scrape)
      :sys.get_state(scraper_pid)

      latest = Mobius.Scraper.latest_for(instance, "ropts_plain.value", :last_value, %{})
      refute Map.has_key?(latest, :consolidate)
    end
  end
end
