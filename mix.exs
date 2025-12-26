defmodule TheoryCraft.MixProject do
  use Mix.Project

  @version "0.1.0-dev"
  @source_url "https://github.com/theorycraft-trading/theory_craft"
  @homepage_url "https://theorycraft-trading.com"

  def project() do
    [
      app: :theory_craft,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      elixirc_options: [warnings_as_errors: true],
      # Docs
      name: "TheoryCraft",
      source_url: @source_url,
      homepage_url: @homepage_url,
      description:
        "An open-source, event-driven backtesting and execution engine for quantitative trading",
      package: package(),
      docs: docs()
    ]
  end

  def application() do
    [
      extra_applications: [:logger],
      mod: {TheoryCraft.Application, []}
    ]
  end

  def cli() do
    [
      preferred_envs: [ci: :test]
    ]
  end

  def aliases() do
    [
      tidewave:
        "run --no-halt -e 'Agent.start(fn -> Bandit.start_link(plug: Tidewave, port: 4000) end)'",
      ci: ["format", "credo", "test"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps() do
    [
      {:nimble_csv, "~> 1.3"},
      {:nimble_parsec, "~> 1.4"},
      {:gen_stage, "~> 1.3"},

      ## Dev
      {:tidewave, "~> 0.5", only: :dev},
      {:bandit, "~> 1.0", only: :dev},
      {:ex_doc, "~> 0.39", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},

      ## Tests
      {:tzdata, "~> 1.1", only: :test}
    ]
  end

  defp package() do
    [
      files: ["lib", "mix.exs", "README.md", "CHANGELOG.md", "LICENSE"],
      licenses: ["Apache-2.0"],
      links: %{
        "Website" => @homepage_url,
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/v#{@version}/CHANGELOG.md"
      },
      maintainers: ["DarkyZ aka NotAVirus"]
    ]
  end

  defp docs() do
    [
      main: "readme",
      logo: "logo.png",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: ["README.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"]
    ]
  end
end
