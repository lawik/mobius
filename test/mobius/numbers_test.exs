defmodule Mobius.NumbersTest do
  @moduledoc """
  A single 5000ms latency spike at t=30 pollutes every later
  minute-resolution summary sample with the same `max=5000`. Plotting
  "max latency by minute" charts the lifetime max forever.
  """

  use ExUnit.Case, async: true

  alias Mobius.{Events, MetricsTable, RRD, Summary}

  test "one outlier shows up in every minute-resolution summary sample, forever" do
    table = :"numbers_#{System.unique_integer([:positive])}"
    MetricsTable.init(mobius_instance: table, persistence_dir: "/tmp")
    metric = Telemetry.Metrics.summary("http.latency.ms")
    handler_config = %{table: table, metrics: [metric]}

    t0 = 1_700_000_000

    rrd =
      Enum.reduce(0..299, RRD.new(days: 60, hours: 48, minutes: 120, seconds: 120), fn offset, acc ->
        ts = t0 + offset
        latency = if offset == 30, do: 5000, else: 50

        Events.handle_metrics([:http, :latency], %{ms: latency}, %{}, handler_config)

        [{name, type, value, tags}] = MetricsTable.get_entries(table)
        scrape = [%{timestamp: ts, name: name, type: type, value: value, tags: tags}]
        RRD.insert(acc, ts, scrape)
      end)

    minute_max =
      rrd.minute
      |> CircularBuffer.to_list()
      |> Enum.map(fn {ts, [m]} -> {ts - t0, Summary.calculate(m.value).max} end)

    IO.puts("\n  spike was at t=30s, never repeats. minute-resolution maxes:")
    for {offset, max} <- minute_max, do: IO.puts("    t=#{offset}s → max=#{max}")

    assert Enum.all?(minute_max, fn {_, max} -> max == 5000 end)
  end
end
