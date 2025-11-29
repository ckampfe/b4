defmodule B4.KeydirOwner do
  @moduledoc false

  use GenServer

  alias B4.{Keydir, Writer}

  # crc32 + u128 UUIDv7 + u32 key_size + u32 value_size
  @header_size 4 + 16 + 4 + 4
  @nominal_chunk_size 2 ** 13
  @delete_value Writer.delete_value()

  defmodule State do
    @enforce_keys [:tid]
    defstruct [:tid]
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

    database_files = all_b4_database_files(directory)

    Enum.each(database_files, fn path ->
      apply_file_to_keydir(path, tid, @nominal_chunk_size)
    end)

    {:ok, %State{tid: tid}}
  end

  def apply_file_to_keydir(path, tid, nominal_chunk_size) do
    path
    |> File.stream!(nominal_chunk_size)
    |> Enum.reduce_while(%{buffer: <<>>, file_position: 0}, fn
      chunk, %{buffer: buffer, file_position: file_position} ->
        buffer = buffer <> chunk

        case apply_chunk_to_keydir(buffer, tid, path, file_position) do
          {:ok, :need_more_input} ->
            {:cont,
             %{
               buffer: buffer,
               file_position: file_position
             }}

          # if we get a bad crc32, the db is corrupt,
          # we do not have the necessary information to repair,
          # so we are done loading entries from this file
          {:error, :bad_crc32} ->
            {:halt, :ok}
        end
    end)
  end

  def apply_chunk_to_keydir(<<>>, _tid, _path, _file_position) do
    {:ok, :need_more_input}
  end

  def apply_chunk_to_keydir(
        <<_crc32::binary-4, entry_id::unsigned-big-integer-128, key_size::unsigned-big-integer-32,
          value_size::unsigned-big-integer-32, key_bytes::bytes-size(key_size),
          value_bytes::bytes-size(value_size), rest::binary>> = buffer,
        tid,
        path,
        file_position
      ) do
    case :erlang.binary_to_term(value_bytes) do
      # it's a delete, remove it from the keydir
      @delete_value ->
        Keydir.delete(tid, :erlang.binary_to_term(key_bytes))

        apply_chunk_to_keydir(
          rest,
          tid,
          path,
          file_position + @header_size + key_size + value_size
        )

      # it's not a delete, decode the key and insert into keydir
      _insert_value ->
        key = :erlang.binary_to_term(key_bytes)
        {file_id, _} = Integer.parse(Path.basename(path, ".b4"))

        entry_size = @header_size + key_size + value_size

        <<on_disk_crc32::32-big-integer, rest_of_header_bytes::bytes-size(@header_size - 4),
          kv_bytes::bytes-size(key_size + value_size), _rest::binary>> =
          buffer

        challenge_crc32 = :erlang.crc32([rest_of_header_bytes, kv_bytes])

        ^on_disk_crc32 = challenge_crc32

        if on_disk_crc32 == challenge_crc32 do
          true =
            Keydir.insert(tid, key, file_id, entry_size, file_position, entry_id)

          apply_chunk_to_keydir(rest, tid, path, file_position + entry_size)
        else
          {:error, :bad_crc32}
        end
    end
  end

  def apply_chunk_to_keydir(_buffer) do
    {:ok, :need_more_input}
  end

  def all_b4_database_files(directory) do
    [directory, "*.b4"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.sort(fn a, b ->
      {a_int, _} = Path.basename(a, ".b4") |> Integer.parse()
      {b_int, _} = Path.basename(b, ".b4") |> Integer.parse()
      a_int <= b_int
    end)
  end

  def name(directory) do
    :"#{__MODULE__}-#{directory}"
  end
end
