defmodule Mobius.ScraperTest do
  use ExUnit.Case, async: false

  alias Mobius.Scraper

  @tag :tmp_dir
  test "scrape reads timestamps from the configured Mobius.Clock", %{tmp_dir: tmp_dir} do
    # Drive time deterministically through `Mobius.Clock.now/0`. Two
    # scrapes one second apart; both land in the same minute so the
    # consolidator doesn't emit a CDP we'd have to account for.
    {:ok, _} = start_supervised({Mobius.ManualClock, 1_700_000_000})

    metric = Telemetry.Metrics.counter("scraper_test.evt.count")
    instance = :"scraper_test_#{System.unique_integer([:positive])}"

    args = [
      metrics: [metric],
      mobius_instance: instance,
      persistence_dir: tmp_dir,
      clock: Mobius.ManualClock
    ]

    {:ok, _pid} = start_supervised({Mobius, args})

    :telemetry.execute([:scraper_test, :evt], %{count: 1}, %{})

    scraper_pid = Process.whereis(Module.concat(Scraper, instance))
    send(scraper_pid, :scrape)
    :sys.get_state(scraper_pid)
    Mobius.ManualClock.advance(1)
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
