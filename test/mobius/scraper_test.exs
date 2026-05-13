defmodule Mobius.ScraperTest do
  use ExUnit.Case, async: false

  alias Mobius.Scraper

  @tag :tmp_dir
  test "scrape uses the injected clock_fn for the timestamp", %{tmp_dir: tmp_dir} do
    # Drive time deterministically. Two scrapes one second apart; both
    # land in the same minute so the consolidator doesn't emit a CDP
    # we'd have to account for in the assertion.
    {:ok, agent} = Agent.start_link(fn -> 1_700_000_000 end)

    clock_fn = fn ->
      Agent.get_and_update(agent, fn t -> {t, t + 1} end)
    end

    metric = Telemetry.Metrics.counter("scraper_test.evt.count")
    instance = :"scraper_test_#{System.unique_integer([:positive])}"

    args = [
      metrics: [metric],
      mobius_instance: instance,
      persistence_dir: tmp_dir,
      clock_fn: clock_fn
    ]

    {:ok, _pid} = start_supervised({Mobius, args})

    :telemetry.execute([:scraper_test, :evt], %{count: 1}, %{})

    # Drive two scrapes manually so we don't depend on wall time.
    scraper_pid = Process.whereis(Module.concat(Scraper, instance))
    send(scraper_pid, :scrape)
    :sys.get_state(scraper_pid)
    send(scraper_pid, :scrape)
    :sys.get_state(scraper_pid)

    metrics = Scraper.all(instance)
    timestamps = Enum.map(metrics, & &1.timestamp)

    # The clock returned 1_700_000_000 and 1_700_000_001, exactly what
    # we asked it to. If the scraper still hit System.system_time, the
    # timestamps would be ~1_750_000_000+ (real wall clock).
    assert timestamps == [1_700_000_000, 1_700_000_001]
  end
end
