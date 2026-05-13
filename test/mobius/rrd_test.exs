defmodule Mobius.RRDTest do
  use ExUnit.Case, async: true

  alias Mobius.RRD

  @args [days: 60, hours: 48, minutes: 120, seconds: 120]

  test "create a new one" do
    buffer = RRD.new(@args)
    assert RRD.all(buffer) == []
  end

  test "insert a scrape" do
    buffer =
      RRD.new(@args)
      |> RRD.insert(1234, :first)
      |> RRD.insert(1235, :second)

    assert RRD.all(buffer) == [{1234, :first}, {1235, :second}]
  end

  test "query for scrapes in a time range" do
    buffer =
      RRD.new(@args)
      |> RRD.insert(1234, :first)
      |> RRD.insert(3000, :second)

    assert RRD.query(buffer, 1000, 2000) == [{1234, :first}]
    assert RRD.query(buffer, 1000, 3000) == [{1234, :first}, {3000, :second}]
    assert RRD.query(buffer, 2000, 3000) == [{3000, :second}]
    assert RRD.query(buffer, 10, 30) == []

    assert RRD.query(buffer, 1000) == [{1234, :first}, {3000, :second}]
    assert RRD.query(buffer, 3000) == [{3000, :second}]
    assert RRD.query(buffer, 3001) == []
  end

  describe "serialize and decode" do
    test "version 1" do
      in_rrd =
        RRD.new(@args)
        |> RRD.insert(1234, [{[:vm, :memory, :total], :last_value, 123, %{}}])
        |> RRD.insert(3000, [{[:vm, :memory, :total], :last_value, 124, %{}}])

      expected_rrd =
        RRD.new(@args)
        |> RRD.insert(1234, [
          %{name: "vm.memory.total", type: :last_value, value: 123, tags: %{}, timestamp: 1234}
        ])
        |> RRD.insert(3000, [
          %{name: "vm.memory.total", type: :last_value, value: 124, tags: %{}, timestamp: 3000}
        ])

      in_rrd_binary = RRD.save(in_rrd, serialization_version: 1) |> IO.iodata_to_binary()
      assert RRD.load(RRD.new(@args), in_rrd_binary) == {:ok, expected_rrd}
    end

    test "version 2" do
      rrd =
        RRD.new(@args)
        |> RRD.insert(1234, [
          %{name: "vm.memory.total", type: :last_value, value: 123, tags: %{}, timestamp: 1234}
        ])
        |> RRD.insert(3000, [
          %{name: "vm.memory.total", type: :last_value, value: 124, tags: %{}, timestamp: 3000}
        ])

      rrd_binary = RRD.save(rrd) |> IO.iodata_to_binary()
      assert RRD.load(RRD.new(@args), rrd_binary) == {:ok, rrd}
    end
  end

  test "fails to load corrupt binaries" do
    empty_tlb = RRD.new(@args)

    bad_version = <<100, 2, 3, 4>>

    assert RRD.load(empty_tlb, bad_version) ==
             {:error, Mobius.DataLoadError.exception(reason: :unsupported_version)}

    bad_term = <<1, 2, 3, 4, 5>>

    assert RRD.load(empty_tlb, bad_term) ==
             {:error, Mobius.DataLoadError.exception(reason: :corrupt)}

    unexpected_term = <<1>> <> :erlang.term_to_binary(:not_a_list)

    assert RRD.load(empty_tlb, unexpected_term) ==
             {:error, Mobius.DataLoadError.exception(reason: :corrupt)}

    unexpected_term2 = <<1>> <> :erlang.term_to_binary([:not_a_tuple])

    assert RRD.load(empty_tlb, unexpected_term2) ==
             {:error, Mobius.DataLoadError.exception(reason: :corrupt)}

    unexpected_term3 = <<1>> <> :erlang.term_to_binary([{:not_a_timestamp, :value}])

    assert RRD.load(empty_tlb, unexpected_term3) ==
             {:error, Mobius.DataLoadError.exception(reason: :corrupt)}
  end

  test "fill up the all buffers" do
    now = 60 * 86400

    # Insert 60 days of records
    buffer =
      Enum.reduce(
        0..(now - 1),
        RRD.new(@args),
        &RRD.insert(&2, &1, &1)
      )

    # Last 2 seconds
    assert Enum.count(RRD.query(buffer, now - 2)) == 2

    # Last 2 minutes (all 120 second resolution samples)
    assert Enum.count(RRD.query(buffer, now - 2 * 60)) == 120

    # Last 3 minutes (3 minute samples and all 120 seconds of samples)
    assert Enum.count(RRD.query(buffer, now - 3 * 60)) == 123

    # Last 2 hours (2 hour samples, 118 minute samples, all 120 second samples)
    assert Enum.count(RRD.query(buffer, now - 2 * 3600)) == 2 + 118 + 120

    # Last 2 days (2 day samples, 46 hour samples, all 120 minute samples and all 120 second samples)
    assert Enum.count(RRD.query(buffer, now - 2 * 86400)) == 2 + 46 + 120 + 120

    # Last 3 days (3 day samples, 48 hour samples, all 120 minute samples and all 120 second samples)
    assert Enum.count(RRD.query(buffer, now - 3 * 86400)) == 3 + 48 + 120 + 120

    # Last 60 days: 59 day samples (one per calendar-day boundary; the
    # very first insert at ts=0 lands in the seconds bucket, not the
    # day bucket), 48 hour samples, 120 minute samples, 120 second samples.
    assert Enum.count(RRD.query(buffer, 0)) == 59 + 48 + 120 + 120
  end

  describe "first-insert bootstrap" do
    test "first insert into a fresh RRD lands in the seconds bucket regardless of wall clock" do
      # Previously, all *_next boundaries defaulted to 0 in new/1, so the
      # first scrape at any ts >= 0 satisfied ts >= day_next and ended up
      # in the day bucket. That corrupted the 'day' archive with samples
      # taken at process-start time across restarts.
      ts = 1_700_000_123
      rrd = RRD.new(@args) |> RRD.insert(ts, :first)

      assert CircularBuffer.to_list(rrd.day) == []
      assert CircularBuffer.to_list(rrd.hour) == []
      assert CircularBuffer.to_list(rrd.minute) == []
      assert CircularBuffer.to_list(rrd.second) == [{ts, :first}]
    end

    test "boundaries align to first ts so subsequent boundary inserts go to the right bucket" do
      # First insert at ts in the middle of an hour. The next minute and
      # hour boundaries are computed from that ts, not from 0.
      ts = 86400 + 14 * 3600 + 32 * 60 + 45

      next_minute_boundary = 86400 + 14 * 3600 + 33 * 60

      rrd =
        RRD.new(@args)
        |> RRD.insert(ts, :first)
        |> RRD.insert(next_minute_boundary, :at_minute)
        |> RRD.insert(next_minute_boundary + 14, :between)

      assert CircularBuffer.to_list(rrd.minute) == [{next_minute_boundary, :at_minute}]

      assert Enum.sort(CircularBuffer.to_list(rrd.second)) == [
               {ts, :first},
               {next_minute_boundary + 14, :between}
             ]
    end

    test "constant 1Hz feed crosses a day boundary cleanly with one day-bucket entry per real boundary" do
      end_ts = 86400 + 100

      rrd =
        Enum.reduce(0..end_ts, RRD.new(@args), fn ts, acc ->
          RRD.insert(acc, ts, ts)
        end)

      day_entries = CircularBuffer.to_list(rrd.day)
      assert day_entries == [{86400, 86400}]
    end
  end
end
