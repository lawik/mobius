defmodule Mobius.Consolidator do
  @moduledoc """
  Tiered metric storage with proper consolidation across resolutions.

  Replaces the decimating behavior of `Mobius.RRD` for metric data.
  Per-metric, per-resolution accumulators aggregate primary data points
  (PDPs) into consolidation data points (CDPs) on each boundary crossing,
  so a "minute sample" represents the full minute of activity rather
  than a single value taken at the :00 second.

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

  Resolution sizes are independent. Defaults match the previous RRD:
  60 days, 48 hours, 120 minutes, 120 seconds.
  """
  @spec new([create_opt()]) :: t()
  def new(opts \\ []) do
    %{
      second: CircularBuffer.new(opts[:seconds] || 120),
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

  defp metric_key(metric), do: {metric.name, metric.type, metric.tags}

  # Period alignment: each period bucket starts at `div(ts, period) * period`
  # and ends at `(div(ts, period) + 1) * period`. So a minute bucket for
  # ts=1_700_006_437 spans [1_700_006_400, 1_700_006_460).
  defp open_accumulator(ts, period_seconds, metric) do
    start_ts = div(ts, period_seconds) * period_seconds

    %{
      start_ts: start_ts,
      end_ts: start_ts + period_seconds,
      metric: metric,
      first_value: numeric_value(metric),
      last_value: numeric_value(metric),
      min: numeric_value(metric) || 0,
      max: numeric_value(metric) || 0,
      sum: numeric_value(metric) || 0,
      count: 1,
      summary_data: summary_data(metric)
    }
  end

  defp numeric_value(%{type: :summary}), do: nil
  defp numeric_value(%{value: v}), do: v

  defp summary_data(%{type: :summary, value: data}) when is_map(data), do: data
  defp summary_data(_), do: nil

  defp update_accumulator(acc, metric) do
    v = numeric_value(metric)

    %{
      acc
      | last_value: v,
        min: if(is_number(v), do: min(acc.min, v), else: acc.min),
        max: if(is_number(v), do: max(acc.max, v), else: acc.max),
        sum: if(is_number(v), do: acc.sum + v, else: acc.sum),
        count: acc.count + 1,
        summary_data: merge_summary(acc.summary_data, summary_data(metric))
    }
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

  defp close_accumulator(acc, _period_seconds) do
    base = acc.metric

    cdp_value =
      case base.type do
        :counter -> acc.last_value - acc.first_value
        :sum -> acc.last_value - acc.first_value
        :last_value -> consolidate_last_value(acc, base)
        :summary -> acc.summary_data
      end

    %{base | value: cdp_value, timestamp: acc.start_ts}
  end

  defp consolidate_last_value(acc, metric) do
    case reporter_consolidate(metric) do
      :max -> acc.max
      :min -> acc.min
      :last -> acc.last_value
      _ -> acc.sum / acc.count
    end
  end

  defp reporter_consolidate(metric) do
    # Metric is the cached %Mobius.metric{} map from the scraper, not the
    # original Telemetry.Metrics struct, so reporter_options can't be
    # inspected here. Per-metric consolidation override will land via a
    # follow-up that threads reporter_options through to the scrape map.
    Map.get(metric, :consolidate, :avg)
  end

  @doc """
  Return all stored items across all resolutions, sorted by timestamp.

  Open accumulators are *not* included — they only appear after their
  period closes.
  """
  @spec all(t()) :: [{integer(), [Mobius.metric()]}]
  def all(state) do
    (CircularBuffer.to_list(state.day) ++
       CircularBuffer.to_list(state.hour) ++
       CircularBuffer.to_list(state.minute) ++
       CircularBuffer.to_list(state.second))
    |> Enum.sort_by(fn {ts, _} -> ts end)
  end

  @doc """
  Return all stored items with timestamps >= `from`.
  """
  @spec query(t(), integer()) :: [{integer(), [Mobius.metric()]}]
  def query(state, from) do
    state |> all() |> Enum.drop_while(fn {ts, _} -> ts < from end)
  end

  @doc """
  Return all stored items with timestamps in `[from, to]`.
  """
  @spec query(t(), integer(), integer()) :: [{integer(), [Mobius.metric()]}]
  def query(state, from, to) do
    state
    |> all()
    |> Enum.drop_while(fn {ts, _} -> ts < from end)
    |> Enum.take_while(fn {ts, _} -> ts <= to end)
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

  The state passed in supplies the bucket capacities; loaded data is
  inserted directly into the matching CircularBuffers.
  """
  @spec load(t(), binary()) :: {:ok, t()} | {:error, Mobius.DataLoadError.t()}
  def load(state, <<@serialization_version, data::binary>>) do
    data
    |> :erlang.binary_to_term()
    |> do_load(state)
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

  defp load_buffer(empty, items) do
    Enum.reduce(items, empty, fn item, buf -> CircularBuffer.insert(buf, item) end)
  end
end
