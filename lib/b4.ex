defmodule B4 do
  alias B4.{DatabaseSupervisor, DatabasesSupervisor, Keydir, KeydirOwner, Writer}

  def new(directory, options \\ [target_file_size: 2 ** 31]) do
    case DatabasesSupervisor.start_database(directory, options) do
      {:ok, _pid} -> :ok
      other -> other
    end
  end

  def insert(directory, key, value) do
    Writer.insert_sync(directory, key, value)
  end

  def fetch(directory, key) do
    tid = KeydirOwner.get_keydir_tid(directory)

    with {_, {:ok, {^key, file_id, entry_size, file_position, _entry_id}}} <-
           {:keydir_fetch, Keydir.fetch(tid, key)},
         path = Path.join([directory, "#{file_id}.b4"]),
         {:ok, file} <- :file.open(path, [:binary, :raw, :read]),
         {:ok,
          <<
            header_bytes::binary-28,
            rest::binary
          >>} <- :file.pread(file, file_position, entry_size),
         <<disk_crc32::integer-big-32, rest_of_header_bytes::binary-24>> =
           header_bytes,
         <<_id::integer-big-128, key_size::integer-big-32, value_size::integer-big-32>> =
           rest_of_header_bytes,
         <<key_bytes::bytes-size(key_size), value_bytes::bytes-size(value_size)>> = rest,
         challenge_crc32 =
           :erlang.crc32([rest_of_header_bytes, key_bytes, value_bytes]) do
      if disk_crc32 == challenge_crc32 do
        {:ok, :erlang.binary_to_term(value_bytes)}
      else
        {:error, :crc32_bad_match}
      end
    else
      {:keydir_fetch, :error} -> :not_found
      e -> e
    end

    # case Keydir.fetch(tid, key) do
    #   {:ok, {^key, file_id, entry_size, file_position, _entry_id}} ->
    #     path = Path.join([directory, "#{file_id}.b4"])

    #     {:ok, file} = :file.open(path, [:binary, :raw, :read])

    #     {:ok,
    #      <<
    #        header_bytes::binary-28,
    #        rest::binary
    #      >>} = :file.pread(file, file_position, entry_size)

    # <<disk_crc32::integer-big-32, rest_of_header_bytes::binary-24>> =
    #   header_bytes

    # <<_id::integer-big-128, key_size::integer-big-32, value_size::integer-big-32>> =
    #   rest_of_header_bytes

    # <<key_bytes::bytes-size(key_size), value_bytes::bytes-size(value_size)>> = rest

    # challenge_crc32 =
    #   :erlang.crc32([rest_of_header_bytes, key_bytes, value_bytes])

    # ^disk_crc32 = challenge_crc32

    #   {:ok, :erlang.binary_to_term(value_bytes)}

    # :error ->
    #   :error
    # end
  end

  def delete(directory, key) do
    Writer.delete_sync(directory, key)
  end

  def keys(directory) do
    tid = KeydirOwner.get_keydir_tid(directory)
    Keydir.keys(tid)
  end

  def close(directory) do
    DatabaseSupervisor.stop(directory)
  end
end
