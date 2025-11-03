defmodule TheoryCraft.TestHelpers.TestEventProducer do
  @moduledoc """
  A GenStage producer for testing that automatically emits predefined events.

  This producer sends all events immediately when demand is received, then
  completes normally. Useful for testing GenStage pipelines with predictable
  event sequences.

  ## Examples

      # Start with events
      {:ok, producer} = TestEventProducer.start_link(events: [event1, event2, event3])

      # Events will be sent automatically when demand is triggered
      GenStage.demand(producer, :forward)

  """
  use GenStage

  ## Public API

  @doc """
  Starts a TestEventProducer with predefined events.

  ## Parameters

    - `opts` - Keyword list with:
      - `:events` - List of events to emit (required)

  """
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    events = Keyword.fetch!(opts, :events)
    GenStage.start_link(__MODULE__, events)
  end

  ## GenStage callbacks

  @impl true
  def init(events) do
    {:producer, %{events: events, sent: false}}
  end

  @impl true
  def handle_demand(_demand, %{events: events, sent: false} = state) do
    # Send all events then schedule stop
    send(self(), :stop_producer)
    {:noreply, events, %{state | sent: true}}
  end

  @impl true
  def handle_demand(_demand, %{sent: true} = state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_info(:stop_producer, state) do
    {:stop, :normal, state}
  end
end
