defmodule TheoryCraft.TestProcessors do
  @moduledoc false
  # Test processor modules for testing Processor behavior

  defmodule SimpleProcessor do
    @moduledoc false
    # A simple test processor that returns a constant value

    @behaviour TheoryCraft.MarketSource.Processor

    @impl true
    def init(opts) do
      constant = Keyword.get(opts, :constant, 42.0)
      data_name = Keyword.fetch!(opts, :data)
      name = Keyword.fetch!(opts, :name)
      {:ok, %{constant: constant, data_name: data_name, name: name}}
    end

    @impl true
    def next(event, state) do
      %{constant: constant, name: name} = state
      new_data = Map.put(event.data, name, constant)
      new_event = %{event | data: new_data}
      {:ok, new_event, state}
    end
  end
end
