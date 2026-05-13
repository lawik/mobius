defmodule Mobius.Clock do
  @moduledoc """
  Behaviour for Mobius to check if the clock is set and, optionally, to
  read the current time.

  On systems that need to set the time after boot events, metrics might
  report a nonsensical timestamp. Providing a clock implementation allows Mobius
  to make time adjustments on data received before the clock was set.

  If no clock implementation is provided no time adjustments will be made.

  For Nerves devices, [NervesTime](https://hex.pm/packages/nerves_time) can be
  used to track time synchronization.

  ```elixir
  {Mobius, clock: NervesTime}
  ```

  The time adjustments are best effort and might not be 100% exact, but this should
  only affect events that take place during the early stages of system boot.

  ## Time source

  A clock module may also implement `now/0` to act as the time source
  for the scraper. When present, every scrape timestamp comes from the
  clock module instead of `System.system_time/1`. The default — and
  what the built-in `NervesTime` integration uses — leaves `now/0`
  unimplemented and the scraper falls back to the system clock.

  This hook exists for specialized cases: deterministic tests, a
  clock backed by a monotonic / uptime source during early boot, or a
  replay harness that needs to drive synthetic time.
  """

  @doc """
  Callback to check if the clock is synchronized
  """
  @callback synchronized?() :: boolean()

  @doc """
  Optional callback returning the current unix time in seconds.

  When implemented, the scraper uses this in place of
  `System.system_time(:second)` to timestamp samples.
  """
  @callback now() :: integer()

  @optional_callbacks now: 0
end
