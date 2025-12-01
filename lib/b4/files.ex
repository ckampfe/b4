defmodule B4.Files do
  alias B4.{Keydir, KeydirOwner, Writer}

  # crc32 + u128 UUIDv7 + u32 key_size + u32 value_size
  @header_size 4 + 16 + 4 + 4
  @delete_value Writer.delete_value()
  # TODO tune this value for larger dbs?
  # 16 KiB
  @chunk_size 2 ** 14

  # stale reads are possible,
  # because it is possible that a new write (and keydir update) occurs *after*
  # we fetch the mapping from the keydir.
  #
  # there really is no way to ensure that a reader *always*
  # sees the latest version of a key, no matter what, because
  # there is no trivial mechanism to atomically, mutably update the current version of
  # the data for a key and the filesystem at the exact same time, atomically.
  # this database is append-only (with the exception of merges),
  # so new data can be (and is) written concurrently with reads.
  #
  # a write is not considered "committed" until the keydir has been updated
  # after the entry has been written to disk, meaning that it's possible
  # there is a new entry on disk that could theoretically be read, but
  # readers would not know about it because the keydir has not been updated.
  #
  # the old data still exists on disk, so there are multiple versions of the
  # data that exist (assuming no merge has occurred), but the
  # data is not guaranteed to be the latest
  #
  # this is a fundamental tradeoff we make with being an append-only database,
  # and shouldn't actually be a problem in practice,
  # as read-after-write should always be consistent, since the keydir *always*
  # points to the latest entry for a given key,
  # and as mentioned before, a write is not considered committed until
  # the keydir has been updated.
  #
  # all this said, once a write is committed for version N of a key,
  # reads from that point on are consistent, and will always
  # read entry data where the version of that data is greater than or equal
  # to N of the data
  #
  # reads are also isolated, meaning they will always read exactly
  # one verison of the data, even if that version is N or N+1.
  # data will never be "striped", or contain information from multiple writes,
  # because erlang's ETS (and our usage of it) guarantees that no partial
  # results can be seen: a given read only ever sees exactly one
  # mapping to a given location on disk.
  def fetch(directory, key, retries, error) do
    if retries >= 5 do
      {:error, :max_fetch_retries_exceeded, error}
    else
      tid = KeydirOwner.get_keydir_tid(directory)

      with {_, {:ok, {^key, file_id, entry_size, file_position, _entry_id}}} <-
             {:keydir_fetch, Keydir.fetch(tid, key)},
           path = Path.join([directory, "#{file_id}.b4"]),
           {_, {:ok, file}} <- {:file_open, :file.open(path, [:binary, :raw, :read])},
           {_,
            {:ok,
             <<
               header_bytes::binary-28,
               rest::binary
             >>}, _} <- {:read, :file.pread(file, file_position, entry_size), file},
           <<disk_crc32::integer-big-32, rest_of_header_bytes::binary-24>> =
             header_bytes,
           <<_id::integer-big-128, key_size::integer-big-32, value_size::integer-big-32>> =
             rest_of_header_bytes,
           <<key_bytes::bytes-size(key_size), value_bytes::bytes-size(value_size)>> = rest,
           challenge_crc32 =
             :erlang.crc32([rest_of_header_bytes, key_bytes, value_bytes]) do
        :file.close(file)

        if disk_crc32 == challenge_crc32 do
          {:ok, :erlang.binary_to_term(value_bytes)}
        else
          {:error, :crc32_bad_match}
        end
      else
        {:keydir_fetch, :error} ->
          :not_found

        {:file_open, {:error, :enoent} = e} ->
          # if this happens, a migration has migrated the data for this
          # key, deleting its data file in between reading the mapping from the keydir
          # and executing the :file.open/2 call.
          # this is safe to retry, as the keydir will have been updated,
          # but it's possible we want to put a retry limit here to
          # intentionally crash the supervision tree for this db
          # if the retry limit is exceeded, indicating that the read files
          # have been corrupted somehow, possibly by some other activity on the machine,
          # or a faulty backup restore, or disk corruption, etc.
          #
          # further, fetching is an ~O(1) operation,
          fetch(directory, key, retries + 1, e)

        {:file_read, {:error, _e} = error, file} ->
          :file.close(file)
          fetch(directory, key, retries + 1, error)

        e ->
          e
      end
    end
  end

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
