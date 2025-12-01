defmodule B4.KeydirOwner do
  @moduledoc false

  use GenServer

  alias B4.{Files, Keydir, Writer}

  defmodule State do
    @enforce_keys [:directory, :tid, :target_file_size]
    defstruct [:directory, :tid, :target_file_size]
  end

  def start_link(%{directory: directory} = args) do
    GenServer.start_link(__MODULE__, args, name: name(directory))
  end

  def get_keydir_tid(directory) do
    :persistent_term.get({:b4_keydir_tid, directory})
  end

  def merge(directory, timeout \\ 5_000) do
    GenServer.call(name(directory), :merge, timeout)
  end

  @impl GenServer
  def init(%{directory: directory, options: options} = _init_arg) do
    tid = Keydir.new()

    :ok = :persistent_term.put({:b4_keydir_tid, directory}, tid)

    database_files = Files.all_database_files(directory)

    Enum.each(database_files, fn path ->
      Files.apply_file_to_keydir(path, tid)
    end)

    {:ok, %State{directory: directory, tid: tid, target_file_size: options[:target_file_size]}}
  end

  @impl GenServer
  def handle_call(
        :merge,
        _from,
        %State{directory: directory, tid: tid, target_file_size: target_file_size} = state
      ) do
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
    #
    # at given any time, there can only be ONE live entry
    # for a given key,
    # meaning any other entries for a given key are
    # therefor old, and free for deletion
    current_write_file_id = Writer.write_file_id(directory)

    read_only_database_files = Files.read_only_database_files(directory, current_write_file_id)

    {:ok, %{write_file: merge_write_file, file_id: merge_write_file_id}} =
      Writer.new_write_file(directory)

    merge_state =
      Enum.reduce(read_only_database_files, %{merge_file_ids: MapSet.new()}, fn path, outer_acc ->
        acc_for_file =
          path
          |> Files.stream_entries()
          |> Enum.reduce(
            %{
              merge_write_file: merge_write_file,
              merge_write_file_id: merge_write_file_id,
              merge_file_ids: MapSet.new(),
              merge_write_file_position: 0
            },
            fn %{
                 entry: %{
                   crc32: crc32,
                   entry_id: on_disk_entry_id,
                   key_size: key_size,
                   value_size: value_size,
                   key_bytes: key_bytes,
                   value_bytes: value_bytes
                 },
                 meta: %{}
               },
               %{
                 merge_write_file: merge_write_file,
                 merge_write_file_id: merge_write_file_id,
                 merge_write_file_position: merge_write_file_position
               } =
                 acc ->
              key = :erlang.binary_to_term(key_bytes)

              case Keydir.fetch(tid, key) do
                {:ok, {_key, _file_id, _entry_size, _file_position, keydir_entry_id}}
                when keydir_entry_id == on_disk_entry_id ->
                  {:ok,
                   %{
                     merge_write_file: merge_write_file,
                     merge_write_file_id: merge_write_file_id,
                     merge_write_file_position: merge_write_file_position
                   }} =
                    if merge_write_file_position >= target_file_size do
                      :file.close(merge_write_file)

                      {:ok, %{write_file: merge_write_file, file_id: merge_write_file_id}} =
                        Writer.new_write_file(directory)

                      {:ok,
                       %{
                         merge_write_file: merge_write_file,
                         merge_write_file_id: merge_write_file_id,
                         merge_write_file_position: 0
                       }}
                    else
                      {:ok,
                       %{
                         merge_write_file: merge_write_file,
                         merge_write_file_id: merge_write_file_id,
                         merge_write_file_position: merge_write_file_position
                       }}
                    end

                  entry =
                    [
                      Writer.int_to_u32_bytes(crc32),
                      Writer.int_to_u128_bytes(on_disk_entry_id),
                      Writer.int_to_u32_bytes(key_size),
                      Writer.int_to_u32_bytes(value_size),
                      key_bytes,
                      value_bytes
                    ]

                  # TODO should we be fsyncing here?
                  :ok = :file.write(merge_write_file, entry)

                  entry_size = :erlang.iolist_size(entry)

                  true =
                    Keydir.insert(
                      tid,
                      key,
                      merge_write_file_id,
                      entry_size,
                      merge_write_file_position,
                      on_disk_entry_id
                    )

                  %{
                    acc
                    | merge_write_file: merge_write_file,
                      merge_write_file_id: merge_write_file_id,
                      merge_write_file_position: acc.merge_write_file_position + entry_size,
                      merge_file_ids: MapSet.put(acc.merge_file_ids, merge_write_file_id)
                  }

                # the entry isn't in the keydir,
                # so it isn't live anymore,
                # so skip it
                :error ->
                  acc

                # the ids for the given key didn't match,
                # so they are old version of that key,
                # so we ignore them
                _ ->
                  acc
              end
            end
          )

        Map.update!(outer_acc, :merge_file_ids, fn merge_file_ids ->
          MapSet.union(merge_file_ids, acc_for_file[:merge_file_ids])
        end)
      end)

    # fsync all merge files
    Enum.each(merge_state[:merge_file_ids], fn merge_file_id ->
      merge_file_path = Path.join([directory, "#{merge_file_id}.b4"])
      {:ok, f} = :file.open(merge_file_path, [:raw, :binary, :read])
      :ok = :file.sync(f)
      :ok = :file.close(f)
    end)

    # |> IO.inspect(label: "merge file acc state")

    # deleting these files happens AFTER
    # the keydir has been updated with the new location
    # on disk for the given entry,
    # so this data is "dead",
    # and there are no readers of it,
    # barring some reader that has initiated some incredibly
    # slow read prior to the merge starting.
    #
    # TODO
    # this is a possibility, but not a huge one,
    # so we should probably guard for this in the future in some way.
    Enum.each(read_only_database_files, fn old_read_only_file ->
      File.rm!(old_read_only_file)
      # IO.inspect("deleted #{old_read_only_file}")
    end)

    # sequence is possible:
    # - read begins
    # - read fetches mapping for k that points to entry in f1
    # - merge begins
    # - merge migrates on-disk entry for k to f2
    # - merge updates keydir for k to point to f2
    # - merge deletes f1
    # - read attempts to read entry from f1
    # - read fails (file not found, etc.)
    #
    # for a given k, from the POV of an observer, one of two things MUST be true:
    # 1. if k exists in keydir: file must exist, so key must be readable
    # 2. if k does not exist in keydir: k is :not_found
    #
    # for #1, reads have no side effects, so we can safely retry them:
    # a read that is retried will return either:
    # 1. the new, updated location of data for k (for example, after an insert or a migration)
    # 2. :not_found, in the case that the key has been deleted in the keydir

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
