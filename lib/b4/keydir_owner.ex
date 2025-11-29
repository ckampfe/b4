defmodule B4.KeydirOwner do
  @moduledoc false

  use GenServer

  alias B4.{Files, Keydir}

  @nominal_chunk_size 2 ** 13

  defmodule State do
    @enforce_keys [:directory, :tid]
    defstruct [:directory, :tid]
  end

  def start_link(%{directory: directory} = args) do
    GenServer.start_link(__MODULE__, args, name: name(directory))
  end

  def get_keydir_tid(directory) do
    :persistent_term.get({:tid, directory})
  end

  @impl GenServer
  def init(%{directory: directory} = _init_arg) do
    tid = Keydir.new()

    :ok = :persistent_term.put({:tid, directory}, tid)

    database_files = Files.all_database_files(directory)

    Enum.each(database_files, fn path ->
      Files.apply_file_to_keydir(path, tid, @nominal_chunk_size)
    end)

    {:ok, %State{directory: directory, tid: tid}}
  end

  def name(directory) do
    :"#{__MODULE__}-#{directory}"
  end

  @impl GenServer
  def terminate(_reason, %State{directory: directory}) do
    :persistent_term.erase({:tid, directory})
  end
end
