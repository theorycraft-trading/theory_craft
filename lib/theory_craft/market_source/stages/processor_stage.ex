defmodule TheoryCraft.MarketSource.ProcessorStage do
  @moduledoc """
  A GenStage producer_consumer that wraps a `TheoryCraft.Processor` behaviour.

  This stage acts as a transformation layer in a GenStage pipeline, applying
  a Processor's stateful transformation to incoming MarketEvent structs while
  maintaining backpressure and demand-driven processing.

  ## Implementation

  The ProcessorStage preserves the Processor behaviour API (`init/1` and `next/2`)
  while adding GenStage capabilities. It maintains the processor's state internally
  and applies transformations to each event as they flow through the pipeline.

  ## Examples

      # Start a ProcessorStage with TickToBarProcessor
      {:ok, processor_stage} = ProcessorStage.start_link(
        {TickToBarProcessor, [data: "ticks", timeframe: "m5"]},
        subscribe_to: [feed_stage]
      )

      # With named stage
      {:ok, processor_stage} = ProcessorStage.start_link(
        {MyProcessor, [option: :value]},
        subscribe_to: [upstream_stage],
        name: :my_processor
      )

  ## Options

  The stage requires:
    - `processor_spec` - A tuple `{processor_module, processor_opts}` where:
      - `processor_module` implements the `TheoryCraft.Processor` behaviour
      - `processor_opts` are passed to `processor_module.init/1`
    - `:subscribe_to` - List of upstream stages to consume from

  Additional GenStage/GenServer options:
    - `:name` - Register the stage with a name
    - `:timeout`, `:debug`, `:spawn_opt`, `:hibernate_after` - GenServer options

  Subscription options (control backpressure and buffering):
    - `:min_demand` - Minimum demand for subscription
    - `:max_demand` - Maximum demand for subscription
    - `:buffer_size` - Size of the buffer (default: 10000)
    - `:buffer_keep` - Whether to keep `:first` or `:last` items when buffer is full (default: `:last`)

  """

  use GenStage

  require Logger

  alias TheoryCraft.MarketSource.{Processor, StageHelpers}
  alias TheoryCraft.Utils

  @typedoc """
  Options for starting a ProcessorStage.

  Stage-specific options:
  - `:subscribe_to` - List of producers to subscribe to (required)

  GenStage/GenServer options:
  - `:name` - Register the stage with a name
  - `:timeout` - Timeout for GenServer.start_link
  - `:debug` - Debug options
  - `:spawn_opt` - Options for spawning the process
  - `:hibernate_after` - Hibernate after inactivity

  Subscription options:
  - `:min_demand` - Minimum demand
  - `:max_demand` - Maximum demand
  - `:buffer_size` - Size of the buffer (default: 10000)
  - `:buffer_keep` - Whether to keep `:first` or `:last` (default: `:last`)
  """
  @type start_option ::
          {:subscribe_to, [GenStage.stage() | {GenStage.stage(), keyword()}]}
          | {:name, atom()}
          | {:timeout, timeout()}
          | {:debug, Keyword.t()}
          | {:spawn_opt, Process.spawn_opt()}
          | {:hibernate_after, timeout()}
          | {:min_demand, non_neg_integer()}
          | {:max_demand, non_neg_integer()}
          | {:buffer_size, non_neg_integer()}
          | {:buffer_keep, :first | :last}

  ## Public API

  @doc """
  Starts a ProcessorStage as a GenStage producer_consumer.

  ## Parameters

    - `processor_spec` - A tuple `{processor_module, processor_opts}` where
      `processor_module` implements the `TheoryCraft.Processor` behaviour
    - `opts` - Keyword list of GenStage options (must include `:subscribe_to`)

  ## Returns

    - `{:ok, pid}` on success
    - `{:error, reason}` on failure

  ## Examples

      # Basic usage
      {:ok, stage} = ProcessorStage.start_link(
        {TickToBarProcessor, [data: "ticks", timeframe: "m5"]},
        subscribe_to: [feed_stage]
      )

      # With named stage and custom demand
      {:ok, stage} = ProcessorStage.start_link(
        {MyProcessor, [option: :value]},
        subscribe_to: [upstream_stage],
        name: :my_processor,
        min_demand: 5,
        max_demand: 50
      )

      # With buffer configuration
      {:ok, stage} = ProcessorStage.start_link(
        {MyProcessor, [option: :value]},
        subscribe_to: [upstream_stage],
        buffer_size: 5000,
        buffer_keep: :first
      )

  """
  @spec start_link(Processor.spec(), [start_option()]) :: GenServer.on_start()
  def start_link(processor_spec, opts) do
    {processor_module, processor_opts} = Utils.normalize_spec(processor_spec)
    gen_stage_opts = StageHelpers.extract_gen_stage_opts(opts)

    GenStage.start_link(__MODULE__, {processor_module, processor_opts, opts}, gen_stage_opts)
  end

  ## GenStage callbacks

  @impl true
  def init({processor_module, processor_opts, stage_opts}) do
    case processor_module.init(processor_opts) do
      {:ok, processor_state} ->
        Logger.debug("""
        ProcessorStage starting with processor_module=#{inspect(processor_module)}
        """)

        state =
          StageHelpers.init_tracking_state(%{
            processor_module: processor_module,
            processor_state: processor_state
          })

        subscribe_to = Keyword.fetch!(stage_opts, :subscribe_to)

        subscription_opts =
          stage_opts
          |> StageHelpers.extract_subscription_opts()
          |> Keyword.merge(subscribe_to: subscribe_to)

        {:producer_consumer, state, subscription_opts}

      error ->
        Logger.error("""
        ProcessorStage: Failed to initialize processor #{inspect(processor_module)}: #{inspect(error)}
        """)

        {:stop, {:processor_init_error, error}}
    end
  rescue
    exception ->
      Logger.error("""
      ProcessorStage: Processor #{inspect(processor_module)} raised exception during init: #{inspect(exception)}
      #{Exception.format_stacktrace(__STACKTRACE__)}
      """)

      {:stop, {:processor_init_error, exception}}
  end

  @impl true
  def handle_subscribe(:producer, _opts, from, state) do
    StageHelpers.handle_producer_subscribe(state, from)
  end

  @impl true
  def handle_subscribe(:consumer, _opts, from, state) do
    StageHelpers.handle_consumer_subscribe(state, from)
  end

  @impl true
  def handle_cancel(_reason, from, state) do
    StageHelpers.handle_cancel_with_termination(state, from, "ProcessorStage")
  end

  @impl true
  def handle_info(:stop, state) do
    StageHelpers.handle_stop_message(state, "ProcessorStage")
  end

  @impl true
  def handle_events(events, _from, state) do
    %{processor_module: processor_module, processor_state: processor_state} = state

    {new_events, new_processor_state} =
      Enum.map_reduce(events, processor_state, fn event, acc_state ->
        try do
          case processor_module.next(event, acc_state) do
            {:ok, output_event, new_state} ->
              {output_event, new_state}

            error ->
              Logger.error("""
              ProcessorStage: Processor #{inspect(processor_module)} returned error: #{inspect(error)}
              """)

              {event, acc_state}
          end
        rescue
          exception ->
            Logger.error("""
            ProcessorStage: Processor #{inspect(processor_module)} raised exception: #{inspect(exception)}
            #{Exception.format_stacktrace(__STACKTRACE__)}
            """)

            {event, acc_state}
        end
      end)

    new_state = %{state | processor_state: new_processor_state}

    {:noreply, new_events, new_state}
  end
end
