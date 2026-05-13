defmodule Mobius.Consolidator do
  @moduledoc """
  Tiered metric storage with proper consolidation across resolutions.

  Per-metric, per-resolution accumulators aggregate primary data points
  (PDPs) into consolidation data points (CDPs) on each boundary
  crossing, so a "minute sample" represents the full minute of activity
  rather than a single value taken at the :00 second. This replaces
  the simpler decimating buffer used before this version, which only
  ever stored the value at each interval boundary.

  Consolidation per metric type:

  * `:counter` / `:sum` — CDP value is the delta within the period
    (last cumulative reading minus the first), so plotted minute samples
    read as "events in that minute" rather than a forever-rising line.
  * `:last_value` — CDP value defaults to the arithmetic mean over the
    period. Other functions (`:min`, `:max`, `:last`) can be selected
    per metric via `reporter_options: [consolidate: :max]`.
  * `:summary` — CDPs hold a merged `Mobius.Summary.data()` for the
    period. Merge is associative over `accumulated`, `accumulated_sqrd`,
    `reports`, `min`, `max`.

  The seconds bucket stores raw PDPs (one per scrape; the scraper is 1Hz).
  The minute / hour / day buckets store CDPs.
  """

  alias Mobius.Summary

  @serialization_version 3

  @typedoc """
  Identity of a metric stream: name + type + tag map.
  """
  @type metric_key :: {String.t(), Mobius.metric_type(), map()}

  @typedoc """
  Resolution name.
  """
  @type resolution :: :seconds | :minutes | :hours | :days

  @typedoc """
  An open accumulator collecting samples for one metric within one
  bucket period. Closed by `flush_open/2` when ts crosses `end_ts`.
  """
  @type accumulator :: %{
          required(:start_ts) => integer(),
          required(:end_ts) => integer(),
          required(:metric) => Mobius.metric(),
          required(:first_value) => term(),
          required(:last_value) => term(),
          required(:min) => number(),
          required(:max) => number(),
          required(:sum) => number(),
          required(:count) => non_neg_integer(),
          required(:summary_data) => Summary.data() | nil
        }

  @opaque t :: %{
            second: CircularBuffer.t(),
            minute: CircularBuffer.t(),
            hour: CircularBuffer.t(),
            day: CircularBuffer.t(),
            open_minute: %{metric_key => accumulator},
            open_hour: %{metric_key => accumulator},
            open_day: %{metric_key => accumulator}
          }

  @type create_opt :: {resolution(), non_neg_integer()}

  @doc """
  Create a new consolidator.

  Resolution sizes are independent. Defaults:

    * `:seconds` - 180 (three minutes of headroom so the stitched read
      path can always serve the most-recently-closed minute CDP without
      a one-second boundary gap; see `all/1` for the reasoning)
    * `:minutes` - 120
    * `:hours` - 48
    * `:days` - 60
  """
  @spec new([create_opt()]) :: t()
  def new(opts \\ []) do
    %{
      second: CircularBuffer.new(opts[:seconds] || 180),
      minute: CircularBuffer.new(opts[:minutes] || 120),
      hour: CircularBuffer.new(opts[:hours] || 48),
      day: CircularBuffer.new(opts[:days] || 60),
      open_minute: %{},
      open_hour: %{},
      open_day: %{}
    }
  end

  @doc """
  Insert a scrape (list of metrics taken at `ts`).

  Steps:
  1. Append the raw PDPs to the seconds bucket.
  2. For each metric, for each coarser period (minute, hour, day):
     - If the metric's open accumulator for that period has been closed
       by `ts >= end_ts`, flush it to the bucket and start a new one.
     - Otherwise update the open accumulator with the new sample.
  """
  @spec insert(t(), integer(), [Mobius.metric()]) :: t()
  def insert(state, ts, metrics) when is_list(metrics) do
    metrics = Enum.map(metrics, fn m -> Map.put(m, :timestamp, ts) end)

    state
    |> Map.update!(:second, &CircularBuffer.insert(&1, {ts, metrics}))
    |> consolidate(ts, metrics)
  end

  defp consolidate(state, ts, metrics) do
    Enum.reduce(metrics, state, fn metric, st ->
      key = metric_key(metric)

      st
      |> step_period(:open_minute, :minute, 60, key, ts, metric)
      |> step_period(:open_hour, :hour, 3600, key, ts, metric)
      |> step_period(:open_day, :day, 86400, key, ts, metric)
    end)
  end

  defp step_period(state, open_field, bucket_field, period_seconds, key, ts, metric) do
    opens = Map.fetch!(state, open_field)

    case Map.get(opens, key) do
      nil ->
        new_acc = open_accumulator(ts, period_seconds, metric)
        Map.put(state, open_field, Map.put(opens, key, new_acc))

      %{end_ts: end_ts} = acc when ts < end_ts ->
        updated = update_accumulator(acc, metric)
        Map.put(state, open_field, Map.put(opens, key, updated))

      acc ->
        # ts >= acc.end_ts → close it, then open a new one and include
        # the current sample.
        cdp_metric = close_accumulator(acc, period_seconds)

        state =
          Map.update!(state, bucket_field, fn buf ->
            CircularBuffer.insert(buf, {acc.start_ts, [cdp_metric]})
          end)

        new_acc = open_accumulator(ts, period_seconds, metric)
        Map.put(state, open_field, Map.put(opens, key, new_acc))
    end
  end

  # Stamp CDPs with the period they describe. Raw PDPs (seconds bucket)
  # are left unmarked — `period_seconds/1` treats absence as 1.
  #
  # Consumers like `Mobius.Exports.delta/4` rely on this to tell whether
  # a stored `value` is a cumulative reading (raw PDP for :counter /
  # :sum) or already a per-period delta (CDP for the same types).
  defp tag_cdp(metric, period_seconds) do
    Map.put(metric, :period_seconds, period_seconds)
  end

  @doc """
  Resolution period for a stored metric, in seconds.

  Raw PDPs from the seconds bucket are 1Hz scrapes, so they report 1.
  CDPs carry their period explicitly via `:period_seconds`.
  """
  @spec period_seconds(Mobius.metric()) :: pos_integer()
  def period_seconds(%{period_seconds: p}) when is_integer(p) and p > 0, do: p
  def period_seconds(_), do: 1

  defp metric_key(metric), do: {metric.name, metric.type, metric.tags}

  # Period alignment: each period bucket starts at `div(ts, period) * period`
  # and ends at `(div(ts, period) + 1) * period`. So a minute bucket for
  # ts=1_700_006_437 spans [1_700_006_400, 1_700_006_460).
  defp open_accumulator(ts, period_seconds, metric) do
    start_ts = div(ts, period_seconds) * period_seconds
    v = numeric_value(metric)
    {first, count, init_num} = if is_number(v), do: {v, 1, v}, else: {nil, 0, 0}

    %{
      start_ts: start_ts,
      end_ts: start_ts + period_seconds,
      metric: metric,
      first_value: first,
      last_value: first,
      min: init_num,
      max: init_num,
      sum: init_num,
      count: count,
      summary_data: summary_data(metric)
    }
  end

  defp numeric_value(%{type: :summary}), do: nil
  defp numeric_value(%{value: v}), do: v

  defp summary_data(%{type: :summary, value: data}) when is_map(data), do: data
  defp summary_data(_), do: nil

  # Non-numeric samples are skipped from min/max/sum/count so the
  # eventual average isn't biased by them. They still update
  # `last_value` and the summary merge path; only the numeric
  # statistics ignore them.
  defp update_accumulator(acc, metric) do
    v = numeric_value(metric)

    if is_number(v) do
      first = acc.first_value || v

      %{
        acc
        | first_value: first,
          last_value: v,
          min: if(acc.count == 0, do: v, else: min(acc.min, v)),
          max: if(acc.count == 0, do: v, else: max(acc.max, v)),
          sum: acc.sum + v,
          count: acc.count + 1,
          summary_data: merge_summary(acc.summary_data, summary_data(metric))
      }
    else
      %{acc | summary_data: merge_summary(acc.summary_data, summary_data(metric))}
    end
  end

  defp merge_summary(nil, b), do: b
  defp merge_summary(a, nil), do: a

  defp merge_summary(a, b) do
    %{
      min: min(a.min, b.min),
      max: max(a.max, b.max),
      accumulated: a.accumulated + b.accumulated,
      accumulated_sqrd: a.accumulated_sqrd + b.accumulated_sqrd,
      reports: a.reports + b.reports
    }
  end

  defp close_accumulator(acc, period_seconds) do
    base = acc.metric

    cdp_value =
      case base.type do
        :counter -> safe_diff(acc.last_value, acc.first_value)
        :sum -> safe_diff(acc.last_value, acc.first_value)
        :last_value -> consolidate_last_value(acc, base)
        :summary -> acc.summary_data
      end

    %{base | value: cdp_value, timestamp: acc.start_ts}
    |> tag_cdp(period_seconds)
  end

  defp safe_diff(a, b) when is_number(a) and is_number(b), do: a - b
  defp safe_diff(_, _), do: 0

  defp consolidate_last_value(acc, metric) do
    case reporter_consolidate(metric) do
      :max -> acc.max
      :min -> acc.min
      :last -> acc.last_value
      _ when acc.count == 0 -> acc.last_value
      _ -> acc.sum / acc.count
    end
  end

  defp reporter_consolidate(metric) do
    # The scraper plucks :consolidate out of each metric's
    # reporter_options at start and attaches it to the scrape map. If
    # the user didn't set one, default to :avg (arithmetic mean of all
    # values observed in the period).
    Map.get(metric, :consolidate, :avg)
  end

  @doc """
  Return all stored items across all resolutions, sorted by timestamp.

  The four buckets cover overlapping time ranges by design (the seconds
  bucket holds the last N seconds; the minute bucket holds CDPs for
  every closed minute including those still represented in the seconds
  bucket). This function stitches them into a single non-overlapping
  series: the finest available resolution wins for each part of the
  timeline.

  A coarser-resolution CDP is included only when its entire period is
  strictly older than the earliest sample in the next-finer bucket.
  With default sizing (seconds=180, minutes=120, hours=48, days=60),
  every finer bucket always extends past one full period of the coarser
  bucket above it, so no gap appears at boundaries.

  Open accumulators are *not* included — they only appear after their
  period closes.
  """
  @spec all(t()) :: [{integer(), [Mobius.metric()]}]
  def all(state) do
    seconds = CircularBuffer.to_list(state.second)
    minutes = CircularBuffer.to_list(state.minute)
    hours = CircularBuffer.to_list(state.hour)
    days = CircularBuffer.to_list(state.day)

    seconds_lb = oldest_ts(seconds)
    minutes = strictly_older_than(minutes, seconds_lb, 60)
    minutes_lb = oldest_ts(minutes) || seconds_lb
    hours = strictly_older_than(hours, minutes_lb, 3600)
    hours_lb = oldest_ts(hours) || minutes_lb
    days = strictly_older_than(days, hours_lb, 86400)

    days ++ hours ++ minutes ++ seconds
  end

  # CircularBuffer.to_list returns oldest first.
  defp oldest_ts([{ts, _} | _]), do: ts
  defp oldest_ts([]), do: nil

  defp strictly_older_than(items, nil, _period), do: items

  defp strictly_older_than(items, cutoff, period) do
    Enum.filter(items, fn {ts, _} -> ts + period <= cutoff end)
  end

  @doc """
  Return all stored items with timestamps >= `from`, stitched as in `all/1`.
  """
  @spec query(t(), integer()) :: [{integer(), [Mobius.metric()]}]
  def query(state, from) do
    state |> all() |> Enum.drop_while(fn {ts, _} -> ts < from end)
  end

  @doc """
  Return all stored items with timestamps in `[from, to]`, stitched as in `all/1`.
  """
  @spec query(t(), integer(), integer()) :: [{integer(), [Mobius.metric()]}]
  def query(state, from, to) do
    state
    |> all()
    |> Enum.drop_while(fn {ts, _} -> ts < from end)
    |> Enum.take_while(fn {ts, _} -> ts <= to end)
  end

  @doc """
  Return the most recent stored sample for `(name, type, tags)`, or
  `nil` if none has been recorded yet.

  Used by `Mobius.info/0` for `:summary` metrics: the MetricsTable's
  summary row holds only sub-second data (after the per-tick reset),
  so `info/0` queries the consolidator for the most recent closed-
  window CDP — typically the previous minute. Falls through to hour
  or day CDPs if nothing finer exists yet, and finally to the most
  recent seconds-bucket PDP.
  """
  @spec latest_for(t(), Mobius.metric_name(), Mobius.metric_type(), map()) ::
          Mobius.metric() | nil
  def latest_for(state, name, type, tags) do
    state
    |> all()
    |> Enum.reverse()
    |> Enum.find_value(nil, fn {_ts, metrics} ->
      Enum.find(metrics, fn m ->
        m.name == name and m.type == type and m.tags == tags
      end)
    end)
  end

  @doc """
  Serialize state to a binary iolist.

  Each resolution's bucket is encoded separately so that loading does not
  re-feed CDPs through the insert path (which would treat them as raw
  PDPs and corrupt the accumulators). Open accumulators are intentionally
  not persisted; on load they restart empty and the partial period is
  discarded.
  """
  @spec save(t()) :: iolist()
  def save(state) do
    payload = %{
      second: CircularBuffer.to_list(state.second),
      minute: CircularBuffer.to_list(state.minute),
      hour: CircularBuffer.to_list(state.hour),
      day: CircularBuffer.to_list(state.day)
    }

    [@serialization_version, :erlang.term_to_iovec(payload)]
  end

  @doc """
  Load a serialized binary back into a fresh state.

  Three formats are accepted:

    * version 3 — the native consolidator format; restored directly.
    * version 2 — legacy Mobius.RRD format; each stored sample is fed
      through `insert/3`. Old per-second samples land in the seconds
      bucket; old per-minute / per-hour / per-day samples open and
      close accumulators with a single PDP each, so the value at each
      old boundary is preserved as a degenerate CDP and the
      consolidator continues normally from there.
    * version 1 — legacy Mobius.RRD format with tuple-shaped metrics;
      migrated to the v2 map shape, then loaded as above.

  The state passed in supplies the bucket capacities.
  """
  @spec load(t(), binary()) :: {:ok, t()} | {:error, Mobius.DataLoadError.t()}
  def load(state, <<@serialization_version, data::binary>>) do
    data
    |> :erlang.binary_to_term()
    |> do_load(state)
  catch
    _, _ -> {:error, Mobius.DataLoadError.exception(reason: :corrupt, who: state)}
  end

  def load(state, <<2, data::binary>>) do
    data
    |> :erlang.binary_to_term()
    |> do_load_legacy(state)
  catch
    _, _ -> {:error, Mobius.DataLoadError.exception(reason: :corrupt, who: state)}
  end

  def load(state, <<1, data::binary>>) do
    data
    |> :erlang.binary_to_term()
    |> migrate_v1_to_v2()
    |> do_load_legacy(state)
  catch
    _, _ -> {:error, Mobius.DataLoadError.exception(reason: :corrupt, who: state)}
  end

  def load(state, _) do
    {:error, Mobius.DataLoadError.exception(reason: :unsupported_version, who: state)}
  end

  defp do_load(%{second: s, minute: m, hour: h, day: d}, state) do
    loaded = %{
      state
      | second: load_buffer(state.second, s),
        minute: load_buffer(state.minute, m),
        hour: load_buffer(state.hour, h),
        day: load_buffer(state.day, d)
    }

    {:ok, loaded}
  end

  defp do_load(_, _state), do: throw(:bad_payload)

  defp do_load_legacy(data, state) when is_list(data) do
    loaded =
      Enum.reduce(data, state, fn {ts, metrics}, st ->
        insert(st, ts, metrics)
      end)

    {:ok, loaded}
  end

  defp do_load_legacy(_, _state), do: throw(:bad_payload)

  # v1 stored each metric as `{atom_list_name, type, value, tags_map}`;
  # v2 uses `%{name: dotted_string, type:, value:, tags:, timestamp:}`.
  defp migrate_v1_to_v2(data) do
    Enum.map(data, fn {ts, metrics} ->
      metrics =
        Enum.map(metrics, fn {name, type, value, tags} ->
          %{
            name: Enum.join(name, "."),
            type: type,
            value: value,
            tags: tags,
            timestamp: ts
          }
        end)

      {ts, metrics}
    end)
  end

  defp load_buffer(empty, items) do
    Enum.reduce(items, empty, fn item, buf -> CircularBuffer.insert(buf, item) end)
  end
end
