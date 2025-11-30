defmodule B4.KeydirOwner do
  @moduledoc false

  use GenServer

  alias B4.{Files, Keydir, Writer}

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

  def merge(directory, timeout \\ 5_000) do
    GenServer.call(name(directory), :merge, timeout)
  end

  @impl GenServer
  def init(%{directory: directory} = _init_arg) do
    tid = Keydir.new()

    :ok = :persistent_term.put({:tid, directory}, tid)

    database_files = Files.all_database_files(directory)

    Enum.each(database_files, fn path ->
      Files.apply_file_to_keydir(path, tid)
    end)

    {:ok, %State{directory: directory, tid: tid}}
  end

  @impl GenServer
  def handle_call(:merge, _from, %State{directory: directory} = state) do
    # TODO do merge here
    # two sets:
    # old read file set
    # new read file set
    # iterate all keys in old read set,
    # IFF the key is in the keydir AND IFF ID == ID in keydir:
    #   keep the key
    #   add key to new read file set
    #   update keydir
    # ELSE
    #   skip
    # END
    write_file_id = Writer.write_file_id(directory)

    read_only_database_files = Files.read_only_database_files(directory, write_file_id)

    Enum.each(read_only_database_files, fn path ->
      nil
    end)

    # - read every entry in the read_only_database_files
    # - if the key is in keydir and the id == current id:
    #     keep the key, add to new read file set, update keydir
    #   else
    #     skip
    #   end

    {:reply, :ok, state}
  end

  def name(directory) do
    :"#{__MODULE__}-#{directory}"
  end

  @impl GenServer
  def terminate(_reason, %State{directory: directory}) do
    :persistent_term.erase({:tid, directory})
  end
end
