defmodule B4.DatabaseSupervisor do
  use Supervisor, restart: :transient

  alias B4.{KeydirOwner, Writer}

  def start_link(%{directory: directory, options: _options} = args) do
    Supervisor.start_link(__MODULE__, args, name: name(directory))
  end

  @impl true
  def init(%{directory: _directory, options: _options} = init_arg) do
    children = [
      {KeydirOwner, init_arg},
      {Writer, init_arg}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def stop(directory) do
    Supervisor.stop(name(directory), :shutdown)
  end

  def name(directory) do
    :"#{__MODULE__}-#{directory}"
  end
end
