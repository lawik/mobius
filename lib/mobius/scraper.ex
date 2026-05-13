defmodule Mobius.Scraper do
  @moduledoc false

  use GenServer
  require Logger

  alias Mobius.{Consolidator, MetricsTable}

  @interval 1_000

  @doc """
  Start the scraper server
  """
  @spec start_link([Mobius.arg()]) :: GenServer.on_start()
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: name(args[:mobius_instance]))
  end

  defp name(mobius_instance) do
    Module.concat(__MODULE__, mobius_instance)
  end

  @typedoc """
  Options to pass to the all call

  * `:from` - the unix timestamp, in seconds, to start querying form
  * `:to` - the unix timestamp, in seconds, to query to
  """
  @type all_opt() :: {:from, integer()} | {:to, integer()}

  @doc """
  Get all the records
  """
  @spec all(Mobius.instance(), [all_opt()]) :: [Mobius.metric()]
  def all(instance, opts \\ []) do
    GenServer.call(name(instance), {:get, opts})
  end

  @doc """
  Persist the metrics to disk
  """
  @spec save(Mobius.instance()) :: :ok | {:error, reason :: term()}
  def save(instance), do: GenServer.call(name(instance), :save)

  @doc """
  Return the most recent stored sample for the given metric, or nil if
  none has been recorded yet.
  """
  @spec latest_for(Mobius.instance(), Mobius.metric_name(), Mobius.metric_type(), map()) ::
          Mobius.metric() | nil
  def latest_for(instance, metric_name, type, tags) do
    GenServer.call(name(instance), {:latest_for, metric_name, type, tags})
  end

  @impl GenServer
  def init(args) do
    _ = :timer.send_interval(@interval, self(), :scrape)
    Process.flag(:trap_exit, true)

    state =
      args
      |> state_from_args()
      |> make_database(args)

    {:ok, state}
  end

  defp state_from_args(args) do
    args
    |> Keyword.take([:mobius_instance, :persistence_dir])
    |> Enum.into(%{})
    |> Map.put(:clock, clock_fn(args[:clock]))
    |> Map.put(:reporter_options, reporter_options_map(args[:metrics] || []))
  end

  # Pick the time source for scrape timestamps.
  #
  # If the configured `:clock` module implements the optional
  # `Mobius.Clock.now/0` callback, the scraper reads time through it.
  # Otherwise — including the typical NervesTime case where the module
  # only implements `synchronized?/0` — fall back to the system clock.
  defp clock_fn(nil), do: &default_clock/0

  defp clock_fn(module) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :now, 0) do
      &module.now/0
    else
      &default_clock/0
    end
  end

  defp default_clock, do: System.system_time(:second)

  # Index reporter_options by metric name for O(1) lookup at scrape time.
  # Only :last_value metrics currently honor any reporter_option
  # (`consolidate: :avg | :max | :min | :last`); other metric types
  # have fixed consolidation semantics, so we don't bother reading
  # theirs.
  defp reporter_options_map(metrics) do
    Map.new(metrics, fn metric ->
      {Enum.join(metric.name, "."), metric.reporter_options || []}
    end)
  end

  defp make_database(state, args) do
    rrd =
      args[:database]
      |> load_data(state)

    Map.put(state, :database, rrd)
  end

  defp load_data(database, state) do
    with {:ok, contents} <- File.read(file(state)),
         {:ok, rrd} <- Consolidator.load(database, contents) do
      rrd
    else
      {:error, :enoent} ->
        database

      {:error, %Mobius.DataLoadError{} = error} ->
        Logger.warning(Exception.message(error))

        database
    end
  end

  defp file(state) do
    Path.join(state.persistence_dir, "history")
  end

  defp to_metrics_list(timestamped_metrics) do
    Enum.flat_map(timestamped_metrics, fn {_, metrics} ->
      metrics
    end)
  end

  @impl GenServer
  def handle_call({:get, opts}, _from, state) do
    case Keyword.get(opts, :from) do
      nil ->
        metrics =
          state.database
          |> Consolidator.all()
          |> to_metrics_list()

        {:reply, metrics, state}

      from ->
        {:reply, query_database(from, state, opts), state}
    end
  end

  def handle_call(:save, _from, state) do
    {:reply, save_to_persistence(state), state}
  end

  def handle_call({:latest_for, metric_name, type, tags}, _from, state) do
    {:reply, Consolidator.latest_for(state.database, metric_name, type, tags), state}
  end

  defp query_database(from, state, opts) do
    case opts[:to] do
      nil ->
        Consolidator.query(state.database, from)
        |> to_metrics_list()

      to ->
        Consolidator.query(state.database, from, to)
        |> to_metrics_list()
    end
  end

  @impl GenServer
  def handle_info(:scrape, state) do
    case MetricsTable.snapshot_for_scrape(state.mobius_instance) do
      [] ->
        {:noreply, state}

      scrape ->
        ts = state.clock.()
        scrape = scrape_to_metrics_list(ts, scrape, state.reporter_options)
        database = Consolidator.insert(state.database, ts, scrape)

        {:noreply, %{state | database: database}}
    end
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    save_to_persistence(state)
  end

  defp scrape_to_metrics_list(ts, scrape, reporter_options) do
    Enum.map(scrape, fn {name, type, value, tags} ->
      base = %{
        timestamp: ts,
        name: name,
        type: type,
        value: value,
        tags: tags
      }

      case Keyword.get(reporter_options[name] || [], :consolidate) do
        nil -> base
        fun -> Map.put(base, :consolidate, fun)
      end
    end)
  end

  # Write our database to persistent storage
  defp save_to_persistence(state) do
    contents = Consolidator.save(state.database)

    case File.write(file(state), contents) do
      :ok ->
        :ok

      error ->
        Logger.warning("Failed to save metrics history because #{inspect(error)}")

        error
    end
  end
end
