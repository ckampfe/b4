defmodule B4.Writer do
  use GenServer
  alias B4.{Keydir, KeydirOwner}

  defmodule State do
    @enforce_keys [:directory, :tid, :write_file, :file_id, :file_position, :target_file_size]
    defstruct [:directory, :tid, :write_file, :file_id, :file_position, :target_file_size]
  end

  def start_link(%{directory: directory} = args) do
    GenServer.start_link(__MODULE__, args, name: name(directory))
  end

  def new_write_file(directory) do
    latest_file_id = latest_b4_file_id(directory)
    file_id = latest_file_id + 1

    {:ok, write_file} =
      :file.open(Path.join([directory, "#{file_id}.b4"]), [:binary, :raw, :append])

    {:ok, %{write_file: write_file, file_id: file_id}}
  end

  @impl GenServer
  def init(%{directory: directory, options: [target_file_size: target_file_size]}) do
    {:ok, %{write_file: write_file, file_id: file_id}} = new_write_file(directory)

    tid = KeydirOwner.get_keydir_tid(directory)

    {:ok,
     %State{
       tid: tid,
       directory: directory,
       write_file: write_file,
       file_id: file_id,
       file_position: 0,
       target_file_size: target_file_size
     }}
  end

  def insert_sync(directory, key, value) do
    GenServer.call(name(directory), {:insert, key, value})
  end

  def delete_sync(directory, key) do
    GenServer.call(name(directory), {:delete, key})
  end

  @impl GenServer
  def handle_call(
        {:insert, key, value},
        _from,
        %State{
          directory: directory,
          tid: tid,
          write_file: write_file,
          file_id: file_id,
          file_position: file_position,
          target_file_size: target_file_size
        } =
          state
      ) do
    {:ok, %{write_file: write_file, file_id: file_id}} =
      if file_position >= target_file_size do
        {:ok, ret} = new_write_file(directory)
        {:ok, Map.put(ret, :file_position, 0)}
      else
        {:ok, %{write_file: write_file, file_id: file_id, file_position: file_position}}
      end

    entry_id = UUIDv7.bingenerate()

    serialized_key = :erlang.term_to_binary(key)
    serialized_value = :erlang.term_to_binary(value)

    key_size = size_as_u32_bytes(serialized_key)
    value_size = size_as_u32_bytes(serialized_value)

    entry_without_crc32 = [entry_id, key_size, value_size, serialized_key, serialized_value]

    crc32 =
      entry_without_crc32
      |> :erlang.crc32()
      |> int_to_u32_bytes()

    entry = [crc32 | entry_without_crc32]

    :ok = :file.write(write_file, entry)

    entry_size = :erlang.iolist_size(entry)

    true = Keydir.insert(tid, key, file_id, entry_size, file_position, entry_id)

    {:reply, :ok,
     %{state | file_position: state.file_position + entry_size, write_file: write_file}}
  end

  def handle_call(
        {:delete, key},
        _from,
        %State{
          directory: directory,
          tid: tid,
          write_file: write_file,
          file_position: file_position,
          file_id: file_id,
          target_file_size: target_file_size
        } =
          state
      ) do
    {:ok, %{write_file: write_file, file_id: file_id}} =
      if file_position >= target_file_size do
        {:ok, ret} = new_write_file(directory)
        {:ok, Map.put(ret, :file_position, 0)}
      else
        {:ok, %{write_file: write_file, file_id: file_id, file_position: file_position}}
      end

    entry_id = UUIDv7.bingenerate()

    serialized_key = :erlang.term_to_binary(key)
    serialized_value = :erlang.term_to_binary(delete_value())

    key_size = size_as_u32_bytes(serialized_key)
    value_size = size_as_u32_bytes(serialized_value)

    entry_without_crc32 = [entry_id, key_size, value_size, serialized_key, serialized_value]

    crc32 =
      entry_without_crc32
      |> :erlang.crc32()
      |> int_to_u32_bytes()

    entry = [crc32 | entry_without_crc32]

    :ok = :file.write(write_file, entry)

    entry_size = :erlang.iolist_size(entry)

    Keydir.delete(tid, key)

    {:reply, :ok,
     %{
       state
       | file_position: state.file_position + entry_size,
         file_id: file_id,
         write_file: write_file
     }}
  end

  def latest_b4_file_id(directory) do
    [directory, "*.b4"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.map(fn path ->
      filename_without_extension = Path.basename(path, ".b4")
      {i, _} = Integer.parse(filename_without_extension)
      i
    end)
    |> Enum.max(fn -> 0 end)
  end

  def delete_value do
    :__b4_delete
  end

  def size_as_u32_bytes(bytes) when is_binary(bytes) do
    bytes
    |> byte_size()
    |> int_to_u32_bytes()
  end

  def int_to_u32_bytes(integer) when is_integer(integer) do
    <<integer::unsigned-integer-32>>
  end

  def name(directory) do
    :"#{__MODULE__}-#{directory}"
  end
end
