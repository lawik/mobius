defmodule Mobius.Exports do
  @moduledoc """
  Support retrieving historical data in different formats

  Current formats:

  * CSV
  * Series
  * Line plot
  * Mobius Binary Format (MBF)

  The Mobius Binary Format (MBF) is a format that contains the current state of
  all metrics. This binary format is useful for transferring metric information in
  a format that other services can parse and use. For more details see `mbf/1`.
  """

  alias Mobius.Asciichart
  alias Mobius.Exports.{CSV, Metrics, MobiusBinaryFormat, UnsupportedMetricError}

  @typedoc """
  Options to use when exporting time series metric data

  * `:mobius_instance` - the name of the Mobius instance you are using. Unless
    you specified this in your configuration you should be safe to allow this
    option to default, which is `:mobius_metrics`.
  * `:last` - display data point that have been captured over the last `x`
    amount of time. Where `x` is either an integer or a tuple of
    `{integer(), time_unit()}`. If you only pass an integer the time unit of
    `:seconds` is assumed. By default Mobius will plot the last 3 minutes of
    data.
  * `:from` - the unix timestamp, in seconds, to start querying from
  * `:to` - the unix timestamp, in seconds, to stop querying at
  """
  @type export_opt() ::
          {:mobius_instance, Mobius.instance()}
          | {:from, integer()}
          | {:to, integer()}
          | {:last, integer() | {integer(), Mobius.time_unit()}}

  @typedoc """
  Options for exporting a CSV
  """
  @type csv_export_opt() ::
          export_opt()
          | {:headers, boolean()}
          | {:iodevice, IO.device()}

  @typedoc """
  Metric types that can be exported

  By default you can try to export any `Mobius.metric_type()`, but for the
  summary metric type you can specify which summary type you want to export.
  """
  @type export_metric_type() :: Mobius.metric_type() | {:summary, atom()}

  @doc """
  Generate a CSV for the metric

  Please see `Mobius.Exporters.CSV` for more information.

  ```elixir
  # Return CSV as string
  {:ok, csv_string} = Mobius.Exports.csv("vm.memory.total", :last_value, %{})

  # Write to console
  Mobius.Exports.csv("vm.memory.total", :last_value, %{}, iodevice: :stdio)

  # Write to a file
  file = File.open("mycsv.csv", [:write])
  :ok = Mobius.Exports.csv("vm.memory.total", :last_value, %{}, iodevice: file)
  ```
  """
  @spec csv(binary(), export_metric_type(), map(), [csv_export_opt()]) ::
          :ok | {:ok, binary()} | {:error, UnsupportedMetricError.t()}
  def csv(metric_name, type, tags, opts \\ [])

  def csv(_metric_name, :summary, _tags, _opts) do
    {:error, UnsupportedMetricError.exception(metric_type: :summary)}
  end

  def csv(metric_name, type, tags, opts) do
    metrics = get_metrics(metric_name, type, tags, opts)
    export_opts = build_exporter_opts(metric_name, type, tags, opts)
    CSV.export_metrics(metrics, export_opts)
  end

  @doc """
  Generates a series that contains the value of the metric
  """
  @spec series(String.t(), export_metric_type(), map(), [export_opt()]) :: [integer()]
  def series(metric_name, type, tags, opts \\ []) do
    metric_name
    |> get_metrics(type, tags, opts)
    |> Enum.map(& &1.value)
  end

  @doc """
  Retrieve the raw metric data from the history store for a given metric.

  Output will be a list of metric values, which will be in the format, eg:
    `%{type: :last_value, value: 12, tags: %{interface: "eth0"}, timestamp: 1645107424}`

    If there are tags for the metric you can pass those in the third argument:

  ```elixir
  Mobius.Exports.metrics("vm.memory.total", :last_value, %{some: :tag})
  ```

  By default the filter will display the last 3 minutes of metric history.

  However, you can pass the `:from` and `:to` options to look at a specific
  range of time.

  ```elixir
  Mobius.Exports.metrics("vm.memory.total", :last_value, %{}, from: 1630619212, to: 1630619219)
  ```

  You can also filter data over the last `x` amount of time. Where x is an
  integer. When there is no `time_unit()` provided the unit is assumed to be
  `:second`.

  Retrieving data over the last 30 seconds:

  ```elixir
  Mobius.Exports.metrics("vm.memory.total", :last_value, %{}, last: 30)
  ```

  Retrieving data over the last 2 hours:

  ```elixir
  Mobius.Exports.metrics("vm.memory.total", :last_value, %{}, last: {2, :hour})
  ```

  Retrieving summary data can be performed by specifying the type: :summary - however, this returns
  value data in the form of a map, which cannot be plotted or csv exported. To reduce the output to
  a single metric value, use the form: {:summary, :summary_metric}

  ```elixir
  Mobius.Exports.metrics("vm.memory.total", {:summary, :average}, %{}, last: {2, :hour})
  ```
  """
  @spec metrics(Mobius.metric_name(), Mobius.metric_type(), map(), [export_opt()] | keyword()) ::
          [Mobius.metric()]
  def metrics(metric_name, type, tags, opts \\ []) do
    Metrics.export(metric_name, type, tags, opts)
  end

  @doc """
  Pairwise delta of a cumulative metric.

  `:counter` and `:sum` metrics are stored as running totals, so two
  consecutive stored samples differ by "events in the interval between
  them". `delta/4` returns that difference for each adjacent pair.

  Returns a list of `{timestamp, delta}` tuples. The first stored sample
  has no predecessor, so the result has one fewer entry than `metrics/4`.

  ```elixir
  Mobius.Exports.delta("http.request.count", :counter, %{}, last: {5, :minute})
  # => [{1700000060, 12}, {1700000120, 19}, ...]
  ```

  Only `:counter` and `:sum` are valid; other types raise.
  """
  @spec delta(Mobius.metric_name(), :counter | :sum, map(), [export_opt()]) ::
          [{integer(), number()}]
  def delta(metric_name, type, tags, opts \\ []) when type in [:counter, :sum] do
    metric_name
    |> metrics(type, tags, opts)
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [prev, curr] -> {curr.timestamp, curr.value - prev.value} end)
  end

  @doc """
  Per-second rate of a cumulative metric.

  Same shape as `delta/4` but divides each delta by the elapsed time
  since the previous sample. Returns `{timestamp, events_per_second}`
  tuples as floats.

  Useful for plotting "requests per second" or "bytes per second" from a
  counter/sum without doing the diff by hand.

  ```elixir
  Mobius.Exports.rate("http.request.count", :counter, %{}, last: {5, :minute})
  # => [{1700000060, 0.2}, {1700000120, 0.31666...}, ...]
  ```
  """
  @spec rate(Mobius.metric_name(), :counter | :sum, map(), [export_opt()]) ::
          [{integer(), float()}]
  def rate(metric_name, type, tags, opts \\ []) when type in [:counter, :sum] do
    metric_name
    |> metrics(type, tags, opts)
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [prev, curr] ->
      seconds = curr.timestamp - prev.timestamp
      if seconds > 0 do
        {curr.timestamp, (curr.value - prev.value) / seconds}
      else
        {curr.timestamp, 0.0}
      end
    end)
  end

  @typedoc """
  How to combine multiple metric samples that fall into the same bucket.

  Used by `aggregate/4`:

  * `:avg` - arithmetic mean of `:value` over the bucket
  * `:min` - minimum `:value`
  * `:max` - maximum `:value`
  * `:last` - the most recent `:value` in the bucket
  * `:sum` - the sum of `:value` over the bucket
  """
  @type aggregate_function() :: :avg | :min | :max | :last | :sum

  @doc """
  Re-bucket stored samples and apply an aggregation function per bucket.

  Mobius stores samples at the resolution the scraper observed. Use
  `aggregate/4` to combine samples into coarser buckets at read time —
  for example, "max of vm.memory.total per minute over the last hour."

  Options:

  * `:bucket` - the bucket size as `Mobius.time_unit()` or `{n, unit}`
    (e.g. `:minute`, `{5, :minute}`). Required.
  * `:function` - one of `t:aggregate_function/0`. Defaults to `:avg`.
  * All `t:export_opt/0` options (`:from`, `:to`, `:last`, `:mobius_instance`)
    are also accepted.

  Returns `{bucket_start_ts, aggregated_value}` tuples sorted by time.
  Buckets with no samples are omitted.

  ```elixir
  Mobius.Exports.aggregate("vm.memory.total", :last_value, %{},
    bucket: :minute, function: :max, last: {1, :hour})
  ```
  """
  @spec aggregate(Mobius.metric_name(), Mobius.metric_type(), map(),
          [{:bucket, Mobius.time_unit() | {pos_integer(), Mobius.time_unit()}}
           | {:function, aggregate_function()}
           | export_opt()]) :: [{integer(), number()}]
  def aggregate(metric_name, type, tags, opts) do
    bucket_seconds = bucket_seconds!(Keyword.fetch!(opts, :bucket))
    function = Keyword.get(opts, :function, :avg)

    metric_name
    |> metrics(type, tags, opts)
    |> Enum.group_by(fn m -> div(m.timestamp, bucket_seconds) * bucket_seconds end)
    |> Enum.map(fn {bucket_start, samples} ->
      values = Enum.map(samples, & &1.value)
      {bucket_start, apply_aggregate(function, values, samples)}
    end)
    |> Enum.sort_by(fn {ts, _} -> ts end)
  end

  defp bucket_seconds!(:second), do: 1
  defp bucket_seconds!(:minute), do: 60
  defp bucket_seconds!(:hour), do: 3600
  defp bucket_seconds!(:day), do: 86400
  defp bucket_seconds!({n, unit}) when is_integer(n) and n > 0, do: n * bucket_seconds!(unit)

  defp apply_aggregate(:avg, values, _), do: Enum.sum(values) / length(values)
  defp apply_aggregate(:sum, values, _), do: Enum.sum(values)
  defp apply_aggregate(:min, values, _), do: Enum.min(values)
  defp apply_aggregate(:max, values, _), do: Enum.max(values)

  defp apply_aggregate(:last, _values, samples) do
    samples
    |> Enum.max_by(& &1.timestamp)
    |> Map.get(:value)
  end

  defp get_metrics(metric_name, type, tags, opts) do
    filter_metrics_opts =
      opts
      |> Keyword.put_new(:mobius_instance, :mobius)
      |> Keyword.take([:metic_name, :type, :tags, :mobius_instance, :from, :to, :last])

    metrics(metric_name, type, tags, filter_metrics_opts)
  end

  defp build_exporter_opts(metric_name, type, tags, opts) do
    opts
    |> Keyword.put_new(:metric_name, metric_name)
    |> Keyword.put_new(:type, type)
    |> Keyword.put_new(:tags, Map.keys(tags))
  end

  @doc """
  Plot the metric name to the screen

  This takes the same arguments as for filter_metrics, eg:

  If there are tags for the metric you can pass those in the second argument:

  ```elixir
  Mobius.Exports.plot("vm.memory.total", :last_value, %{some: :tag})
  ```

  By default the plot will display the last 3 minutes of metric history.

  However, you can pass the `:from` and `:to` options to look at a specific
  range of time.

  ```elixir
  Mobius.Exports.plot("vm.memory.total", :last_value, %{}, from: 1630619212, to: 1630619219)
  ```

  You can also plot data over the last `x` amount of time. Where x is an
  integer. When there is no `time_unit()` provided the unit is assumed to be
  `:second`.

  Plotting data over the last 30 seconds:

  ```elixir
  Mobius.Export.plot("vm.memory.total", :last_value, %{}, last: 30)
  ```

  Plotting data over the last 2 hours:

  ```elixir
  Mobius.Export.plot("vm.memory.total", :last_value, %{}, last: {2, :hour})
  ```

  Retrieving summary data can be performed by specifying type of the form:
    `{:summary, :summary_metric}`

  ```elixir
  Mobius.Exports.metrics("vm.memory.total", {:summary, :average}, %{}, last: {2, :hour})
  ```
  """
  @spec plot(Mobius.metric_name(), export_metric_type(), map(), [export_opt()]) ::
          :ok | {:error, UnsupportedMetricError.t()}
  def plot(metric_name, type, tags \\ %{}, opts \\ [])

  def plot(_metric_name, :summary, _tags, _opts) do
    {:error, UnsupportedMetricError.exception(metric_type: :summary)}
  end

  def plot(metric_name, type, tags, opts) do
    series = series(metric_name, type, tags, opts)

    case Asciichart.plot(series, height: 12) do
      {:ok, plot} ->
        chart = [
          "\t\t",
          IO.ANSI.yellow(),
          "Metric Name: ",
          metric_name,
          IO.ANSI.reset(),
          ", ",
          IO.ANSI.cyan(),
          "Tags: #{inspect(tags)}",
          IO.ANSI.reset(),
          "\n\n",
          plot
        ]

        IO.puts(chart)

      error ->
        error
    end
  end

  @type mfb_export_opt() :: {:out_dir, Path.t()} | export_opt()

  @doc """
  Export all metrics in the Mobius Binary Format (MBF)

  This is mostly useful when you want to share metric data with different
  networked services.

  The binary format is `<<version, metric_data::binary>>`

  The first byte is the version number of the following metric data. Currently,
  the version number is `1`.

  The metric data binary is the type of `[Mobius.metric()]` encoded in Binary
  ERlang Term format (BERT) and compressed (using Zlib compression).

  Optionally, `to_mbf/1` can write the binary to a file using the `:out_dir`
  option.

  ```elixir
  Mobius.Exports.to_mbf(out_dir: "/my/dir")
  ```

  The generated file is returned as `{:ok, filename}`. The format of the
  file name is `YYYYMMDDHHMMSS-metrics.mbf`.

  See `Mobius.Exports.parse_mbf/1` to parse a binary in MBF.
  """
  @spec mbf([mfb_export_opt()]) :: binary() | {:ok, Path.t()} | {:error, Mobius.FileError.t()}
  def mbf(opts \\ []) do
    mobius_instance = opts[:mobius_instance] || :mobius

    mobius_instance
    |> Mobius.Scraper.all()
    |> Enum.reject(fn metric -> metric.type == :summary end)
    |> MobiusBinaryFormat.to_iodata()
    |> maybe_write_file(opts)
  end

  defp maybe_write_file(iodata, opts) do
    case opts[:out_dir] do
      nil ->
        IO.iodata_to_binary(iodata)

      out_dir ->
        file_name = gen_mbf_file_name()
        out_file = Path.join(out_dir, file_name)
        write_file(out_file, iodata)
    end
  end

  defp write_file(file, iodata) do
    case File.write(file, iodata) do
      :ok ->
        {:ok, file}

      {:error, reason} ->
        {:error, Mobius.FileError.exception(reason: reason, file: file, operation: "write")}
    end
  end

  defp gen_mbf_file_name() do
    "#{file_timestamp()}-metrics.mbf"
  end

  defp file_timestamp() do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: <<?0, ?0 + i>>
  defp pad(i), do: to_string(i)

  @doc """
  Parse the mobius binary format into a list of metrics
  """
  @spec parse_mbf(binary()) ::
          {:ok, [Mobius.metric()]} | {:error, Mobius.Exports.MBFParseError.t()}
  def parse_mbf(binary) do
    MobiusBinaryFormat.parse(binary)
  end
end
