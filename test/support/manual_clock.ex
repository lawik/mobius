defmodule Mobius.ManualClock do
  @moduledoc """
  Deterministic clock for tests.

  Implements `Mobius.Clock` (both `synchronized?/0` and `now/0`) on top
  of an `Agent` so a test can drive scrape timestamps by hand. Pass
  this module as `:clock` in the Mobius args; start it in the test
  with `start_supervised/1` so ExUnit tears it down between tests.

      {:ok, _} = start_supervised({Mobius.ManualClock, 1_700_006_400})

      {:ok, _pid} =
        start_supervised(
          {Mobius,
           metrics: [...],
           mobius_instance: :my_instance,
           persistence_dir: tmp_dir,
           clock: Mobius.ManualClock}
        )

      Mobius.ManualClock.advance(10)

  Always synchronized; the `synchronized?/0` callback is a constant
  `true` so the TimeServer treats it as already-set.

  Module-level state means only one ManualClock can be active per node
  at a time, so tests that use it must be `async: false`.
  """

  @behaviour Mobius.Clock

  use Agent

  @doc """
  Start the manual clock with `initial` as the current timestamp
  (unix seconds).
  """
  @spec start_link(integer()) :: Agent.on_start()
  def start_link(initial) when is_integer(initial) do
    Agent.start_link(fn -> initial end, name: __MODULE__)
  end

  @doc false
  def child_spec(initial) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [initial]}
    }
  end

  @doc """
  Replace the current timestamp.
  """
  @spec set(integer()) :: :ok
  def set(ts) when is_integer(ts), do: Agent.update(__MODULE__, fn _ -> ts end)

  @doc """
  Advance the clock by `n` seconds (may be negative).
  """
  @spec advance(integer()) :: :ok
  def advance(n) when is_integer(n), do: Agent.update(__MODULE__, &(&1 + n))

  @impl Mobius.Clock
  def now, do: Agent.get(__MODULE__, & &1)

  @impl Mobius.Clock
  def synchronized?, do: true
end
