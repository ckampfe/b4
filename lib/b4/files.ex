defmodule B4.Files do
  alias B4.{Keydir, Writer}

  # crc32 + u128 UUIDv7 + u32 key_size + u32 value_size
  @header_size 4 + 16 + 4 + 4
  @delete_value Writer.delete_value()
  # TODO tune this value for larger dbs?
  # 16 KiB
  @chunk_size 2 ** 14

  def stream_entries(path) do
    Stream.resource(
      fn ->
        {:ok, file} = :file.open(path, [:raw, :binary, :read])
        {file, <<>>, 0}
      end,
      fn {file, buf, position} ->
        case IO.binread(file, @chunk_size) do
          data when is_binary(data) ->
            buf = <<buf::binary, data::binary>>
            {remaining_buf, position, entries} = parse_entries(buf, position)
            {entries, {file, remaining_buf, position}}

          {:error, _e} ->
            # TODO figure this out
            {:halt, {file, buf, position}}

          :eof ->
            # TODO figure this out
            {:halt, {file, buf, position}}
        end
      end,
      fn {file, _buf, _position} -> :file.close(file) end
    )
  end

  def parse_entries(buffer, position) do
    parse_entries(buffer, position, [])
  end

  def parse_entries(
        <<
          crc32::unsigned-big-integer-32,
          entry_id::unsigned-big-integer-128,
          key_size::unsigned-big-integer-32,
          value_size::unsigned-big-integer-32,
          key_bytes::bytes-size(key_size),
          value_bytes::bytes-size(value_size),
          rest::binary
        >>,
        position,
        entries
      ) do
    entry_id_bytes = <<entry_id::unsigned-big-integer-128>>
    key_size_bytes = <<key_size::unsigned-big-integer-32>>
    value_size_bytes = <<value_size::unsigned-big-integer-32>>

    challenge_crc32 =
      :erlang.crc32([
        entry_id_bytes,
        key_size_bytes,
        value_size_bytes,
        key_bytes,
        value_bytes
      ])

    parse_entries(
      rest,
      position + @header_size + key_size + value_size,
      [
        %{
          entry: %{
            crc32: crc32,
            entry_id: entry_id,
            key_size: key_size,
            value_size: value_size,
            key_bytes: key_bytes,
            value_bytes: value_bytes
          },
          meta: %{
            position: position,
            crc32_valid?: crc32 == challenge_crc32
          }
        }
        | entries
      ]
    )
  end

  def parse_entries(remaining_bytes, position, entries) do
    {remaining_bytes, position, Enum.reverse(entries)}
  end

  def apply_file_to_keydir(path, tid) do
    path
    |> stream_entries()
    |> Enum.map(fn %{
                     entry: %{
                       entry_id: entry_id,
                       key_size: key_size,
                       value_size: value_size,
                       value_bytes: value_bytes,
                       key_bytes: key_bytes
                     },
                     meta: %{crc32_valid?: crc32_valid?, position: position}
                   } ->
      value = :erlang.binary_to_term(value_bytes)

      case value do
        @delete_value ->
          Keydir.delete(tid, :erlang.binary_to_term(key_bytes))

        _insert_value ->
          key = :erlang.binary_to_term(key_bytes)

          {file_id, _} = Integer.parse(Path.basename(path, ".b4"))

          entry_size = @header_size + key_size + value_size

          if crc32_valid? do
            true =
              Keydir.insert(tid, key, file_id, entry_size, position, entry_id)
          else
            {:error, :bad_crc32}
          end
      end
    end)
  end

  def read_only_database_files(directory, write_file_id) do
    directory
    |> all_database_files()
    |> Enum.filter(fn path ->
      Path.basename(path, ".b4") != "#{write_file_id}"
    end)
  end

  def all_database_files(directory) do
    [directory, "*.b4"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.sort(fn a, b ->
      {a_int, _} = Path.basename(a, ".b4") |> Integer.parse()
      {b_int, _} = Path.basename(b, ".b4") |> Integer.parse()
      a_int <= b_int
    end)
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
end
