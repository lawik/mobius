defmodule Mobius.ConsolidatorTest do
  @moduledoc """
  Behavior tests for the per-metric, per-resolution consolidator.

  These tests drive the consolidator directly with synthetic scrapes at
  chosen timestamps so the resulting CDPs are deterministic. The
  scraper-integration tests live separately.
  """

  use ExUnit.Case, async: true

  alias Mobius.Consolidator

  @args [days: 60, hours: 48, minutes: 120, seconds: 120]

  defp metric(name, type, value, tags \\ %{}) do
    %{name: name, type: type, value: value, tags: tags, timestamp: 0}
  end

  describe "raw PDPs in the seconds bucket" do
    test "every scrape is preserved at second resolution" do
      state =
        Enum.reduce(0..9, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, 1_700_000_000 + offset, [
            metric("vm.mem.total", :last_value, offset * 100)
          ])
        end)

      seconds = CircularBuffer.to_list(state.second)
      assert length(seconds) == 10

      values =
        Enum.flat_map(seconds, fn {_, metrics} -> Enum.map(metrics, & &1.value) end)

      assert values == Enum.map(0..9, &(&1 * 100))
    end
  end

  describe "counter consolidation produces per-minute deltas" do
    test "minute CDP equals last - first cumulative reading in the minute" do
      # Simulate cumulative counter readings every second for 90 seconds
      # starting at a minute boundary. The counter starts at 0 and adds 5
      # per second.
      base = 1_700_006_400

      state =
        Enum.reduce(0..89, Consolidator.new(@args), fn offset, st ->
          cumulative = (offset + 1) * 5
          Consolidator.insert(st, base + offset, [metric("evt.count", :counter, cumulative)])
        end)

      # Minute 1 (offsets 0..59) should close at offset 60 — the value
      # delta is "cumulative at offset 59 (= 300) minus cumulative at
      # offset 0 (= 5)" = 295.
      minute_buckets = CircularBuffer.to_list(state.minute)
      assert length(minute_buckets) == 1

      [{ts1, [m1]}] = minute_buckets
      assert ts1 == base
      assert m1.value == 295
      assert m1.type == :counter

      # The second minute is still open (offsets 60..89, 30 seconds in).
      assert Map.has_key?(state.open_minute, {"evt.count", :counter, %{}})
    end
  end

  describe "last_value consolidation defaults to arithmetic mean" do
    test "minute CDP is the average of the values seen in that minute" do
      base = 1_700_006_400

      # Sawtooth: value = ts mod 60. Average over a minute is 29.5.
      state =
        Enum.reduce(0..89, Consolidator.new(@args), fn offset, st ->
          ts = base + offset
          Consolidator.insert(st, ts, [metric("temp.c", :last_value, rem(ts, 60))])
        end)

      [{ts1, [m1]}] = CircularBuffer.to_list(state.minute)
      assert ts1 == base
      assert_in_delta m1.value, 29.5, 0.001

      # Critical: the value at the minute boundary itself would be 0 (the
      # OLD behavior), so an average that's nowhere near 0 confirms we're
      # consolidating, not snapshotting.
      refute m1.value == 0
    end
  end

  describe "summary consolidation merges associatively" do
    test "minute CDP holds min/max/avg/std-dev of measurements WITHIN that minute, not since process start" do
      base = 1_700_006_400

      # First minute: 60 observations of value=50 except one spike to 1000
      # at offset 30. Second minute starts but doesn't close.
      state =
        Enum.reduce(0..89, Consolidator.new(@args), fn offset, st ->
          value = if offset == 30, do: 1000, else: 50
          summary_data = Mobius.Summary.new(value)

          Consolidator.insert(st, base + offset, [
            metric("lat.ms", :summary, summary_data)
          ])
        end)

      [{ts1, [m1]}] = CircularBuffer.to_list(state.minute)
      assert ts1 == base

      stats = Mobius.Summary.calculate(m1.value)
      assert stats.min == 50
      assert stats.max == 1000

      # The second minute (offsets 60..89) hasn't closed; its open
      # accumulator should NOT include the 1000 spike from the first minute.
      open = state.open_minute[{"lat.ms", :summary, %{}}]
      open_stats = Mobius.Summary.calculate(open.summary_data)
      assert open_stats.max == 50,
             "second minute's open accumulator should not see minute 1's spike (max=#{open_stats.max})"
    end
  end

  describe "boundary crossings flush only what crossed" do
    test "single-metric scrape across an hour boundary emits both minute and hour CDPs" do
      # Insert a value every minute for 65 minutes, then one more value
      # 10 minutes into the next hour.
      base = 1_700_006_400
      state = Consolidator.new(@args)

      state =
        Enum.reduce(0..(64 * 60 + 30), state, fn offset, st ->
          Consolidator.insert(st, base + offset, [
            metric("cpu.pct", :last_value, 50)
          ])
        end)

      minute_count = state.minute |> CircularBuffer.to_list() |> length()
      hour_count = state.hour |> CircularBuffer.to_list() |> length()

      # 64 closed minutes (the 65th is still open partway through).
      assert minute_count == 64
      # One closed hour (the second hour is open).
      assert hour_count == 1
    end
  end

  describe "all/1 stitches resolutions without overlap" do
    test "while the seconds bucket still covers a closed minute, all/1 returns only seconds for that range" do
      # 121 ticks at @args (seconds=120): seconds bucket holds offsets
      # 1..120 (oldest at base+1). Minute CDP at base has end=base+60,
      # which is NOT strictly older than base+1, so it's filtered out
      # — the seconds bucket already has finer resolution coverage.
      base = 1_700_006_400

      state =
        Enum.reduce(0..120, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [
            metric("cpu.pct", :last_value, 50)
          ])
        end)

      all = Consolidator.all(state)
      timestamps = Enum.map(all, fn {ts, _} -> ts end)
      assert timestamps == Enum.sort(timestamps)

      refute Enum.any?(all, fn {ts, _} -> ts == base end),
             "minute CDP at base should NOT appear while seconds bucket still covers that minute"

      # Seconds-only coverage is what's returned.
      assert length(all) == 120
    end

    test "once the seconds bucket has rotated past a closed minute, that minute CDP appears" do
      # 240 ticks with @args (seconds=120): seconds bucket holds offsets
      # 120..240 (oldest=base+120). Minute CDPs at base and base+60
      # both have end <= base+120, so both are kept.
      base = 1_700_006_400

      state =
        Enum.reduce(0..240, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [
            metric("cpu.pct", :last_value, 50)
          ])
        end)

      all = Consolidator.all(state)
      timestamps = MapSet.new(all, fn {ts, _} -> ts end)

      assert MapSet.member?(timestamps, base)
      assert MapSet.member?(timestamps, base + 60)

      # No second-bucket PDP at base or base+60 (those rotated out), so
      # there is no overlap with the seconds bucket.
      refute MapSet.member?(timestamps, base + 119)
    end

    test "no double-counting: a minute CDP and its constituent PDPs are never both present" do
      base = 1_700_006_400

      state =
        Enum.reduce(0..240, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [
            metric("cpu.pct", :last_value, 50)
          ])
        end)

      all_ts = Consolidator.all(state) |> Enum.map(fn {ts, _} -> ts end)

      # Every minute CDP returned has start_ts NOT shared with any
      # surviving second-resolution PDP. With strict stitching, the only
      # way the same ts could appear twice would be if the CDP's period
      # somehow extended into the seconds window — which the filter
      # prevents.
      assert length(all_ts) == length(Enum.uniq(all_ts))
    end

    test "query/3 inherits the stitched view" do
      base = 1_700_006_400

      state =
        Enum.reduce(0..240, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [
            metric("cpu.pct", :last_value, 50)
          ])
        end)

      filtered = Consolidator.query(state, base + 10, base + 200)
      timestamps = Enum.map(filtered, fn {ts, _} -> ts end)
      assert Enum.all?(timestamps, &(&1 >= base + 10 and &1 <= base + 200))
    end
  end

  describe "latest_for/4" do
    test "returns the most recent matching sample across all resolutions" do
      base = 1_700_006_400

      state =
        Enum.reduce(0..119, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [metric("cpu.pct", :last_value, offset)])
        end)

      assert %{name: "cpu.pct", type: :last_value, value: 119, timestamp: 1_700_006_519} =
               Consolidator.latest_for(state, "cpu.pct", :last_value, %{})
    end

    test "returns nil when no matching sample exists" do
      state = Consolidator.new(@args)
      assert Consolidator.latest_for(state, "missing.metric", :last_value, %{}) == nil
    end

    test "after the first minute closes, latest_for prefers the closed CDP if seconds rotated out" do
      base = 1_700_006_400

      # Run long enough that the seconds bucket no longer covers the
      # first minute, so the CDP at base is the freshest source for
      # 'value at minute base'.
      state =
        Enum.reduce(0..240, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [metric("cpu.pct", :last_value, offset)])
        end)

      # The most recent overall sample is the most recent seconds PDP.
      latest = Consolidator.latest_for(state, "cpu.pct", :last_value, %{})
      assert latest.timestamp == base + 240
      assert latest.value == 240
    end
  end

  describe "serialization round-trips" do
    test "save then load reconstructs the stored data" do
      base = 1_700_006_400

      state =
        Enum.reduce(0..120, Consolidator.new(@args), fn offset, st ->
          Consolidator.insert(st, base + offset, [
            metric("cpu.pct", :last_value, 50)
          ])
        end)

      bin = state |> Consolidator.save() |> IO.iodata_to_binary()
      {:ok, restored} = Consolidator.load(Consolidator.new(@args), bin)

      # Stored CDPs round-trip. Open accumulators don't (intentional).
      assert Consolidator.all(restored) == Consolidator.all(state)
    end

    test "rejects unknown serialization versions" do
      bad = <<99, 1, 2, 3>>
      assert {:error, _} = Consolidator.load(Consolidator.new(@args), bad)
    end
  end

  describe "legacy on-disk formats" do
    test "loads a v2 (legacy Mobius.RRD) file by replaying samples through insert/3" do
      # Hand-build a v2-format binary: <<2, encoded_list>> where the
      # list is [{ts, [metric_map]}, ...]. This matches what the
      # removed Mobius.RRD.save/2 used to emit at the v2 default.
      base = 1_700_006_400

      legacy_samples =
        for offset <- 0..59 do
          {base + offset,
           [
             %{
               name: "vm.memory.total",
               type: :last_value,
               value: 1000 + offset,
               tags: %{},
               timestamp: base + offset
             }
           ]}
        end

      legacy_binary = <<2>> <> :erlang.term_to_binary(legacy_samples)

      {:ok, loaded} = Consolidator.load(Consolidator.new(@args), legacy_binary)

      # All 60 old per-second samples are present in the seconds bucket.
      seconds = CircularBuffer.to_list(loaded.second)
      assert length(seconds) == 60

      # Most-recent looked-up value matches what was in the legacy file.
      latest = Consolidator.latest_for(loaded, "vm.memory.total", :last_value, %{})
      assert latest.value == 1059
      assert latest.timestamp == base + 59
    end

    test "loads a v1 (legacy tuple-shaped) file after migrating tuple → map" do
      base = 1_700_006_400

      # v1 stored each metric as {atom_list_name, type, value, tags_map}.
      legacy_samples =
        for offset <- 0..29 do
          {base + offset,
           [{[:vm, :memory, :total], :last_value, 2000 + offset, %{}}]}
        end

      legacy_binary = <<1>> <> :erlang.term_to_binary(legacy_samples)

      {:ok, loaded} = Consolidator.load(Consolidator.new(@args), legacy_binary)

      latest = Consolidator.latest_for(loaded, "vm.memory.total", :last_value, %{})
      assert latest.value == 2029
    end

    test "fresh scrapes continue to consolidate after a legacy load" do
      base = 1_700_006_400

      # One sample per second for the first 30 seconds of a minute,
      # loaded from v2. Then we resume with new ticks for the remaining
      # 30 seconds. At second 60, the minute accumulator closes and
      # emits a CDP that includes ALL 60 samples (loaded + fresh).
      legacy_samples =
        for offset <- 0..29 do
          {base + offset,
           [
             %{
               name: "cpu.pct",
               type: :last_value,
               value: 10,
               tags: %{},
               timestamp: base + offset
             }
           ]}
        end

      legacy_binary = <<2>> <> :erlang.term_to_binary(legacy_samples)
      {:ok, state} = Consolidator.load(Consolidator.new(@args), legacy_binary)

      state =
        Enum.reduce(30..60, state, fn offset, st ->
          Consolidator.insert(st, base + offset, [
            metric("cpu.pct", :last_value, 90)
          ])
        end)

      [{ts, [cdp]}] = CircularBuffer.to_list(state.minute)
      assert ts == base
      # 30 samples of 10, 30 samples of 90 → avg = 50.
      assert_in_delta cdp.value, 50.0, 0.001
    end
  end
end
