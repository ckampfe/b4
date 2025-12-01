defmodule B4 do
  alias B4.{DatabaseSupervisor, DatabasesSupervisor, Files, Keydir, KeydirOwner, Writer}

  @doc """
  Create a database in `directory` with the given options.

  Options:
    - `target_file_size`: when the size of the current write file reaches this size,
      the system will attempt to close it and start a fresh one.
      Defaults to `2 ** 31` bytes (2 GiB).
    - `sync_strategy`: when to flush the operating system write buffers to disk.
      If `:every_write`, an fsync is performed after every single insert and delete.
      This is slower, but safer in the event of e.g., power loss, whole system failure, etc.
      If any other value, or unset, allow the operating system to flush when it wants to,
      which is faster but not resilient to power loss, system failure, etc.
      Defaults to `:every_write`
  """
  def new(directory, options \\ [target_file_size: 2 ** 31, sync_strategy: :every_write]) do
    case DatabasesSupervisor.start_database(directory, options) do
      {:ok, _pid} -> :ok
      other -> other
    end
  end

  @doc """
  Insert `value` for `key`.

  Old versions are preserved on disk until you call `merge/2`,
  at which point all non-live data on disk is destroyed.

  Key and value can be any Elixir terms that are safe to
  serialize and deserialize with `:erlang.term_to_binary/1` and
  `:erlang.binary_to_term/1`.
  """
  def insert(directory, key, value) do
    Writer.insert_sync(directory, key, value)
  end

  @doc """
  Like `Map.fetch/2`.

  Get `value` for `key`, returning `{:ok, value}`, `:not_found`, or `{:error, error}`.
  """
  def fetch(directory, key) do
    Files.fetch(directory, key, 0, nil)
  end

  @doc """
  Delete the given key from the dataset.
  The key and its previous inserts still exist on disk
  until you call `merge/2`.
  """
  def delete(directory, key) do
    Writer.delete_sync(directory, key)
  end

  @doc """
  All live keys in the database.
  """
  def keys(directory) do
    tid = KeydirOwner.get_keydir_tid(directory)
    Keydir.keys(tid)
  end

  @doc """
  Rewrite the current set of "read" files into a new set
  that contains only live keys.
  """
  def merge(directory, timeout \\ 15_000) do
    with {_, :ok} <- {:set_merge_in_progress, Writer.set_merge_in_progress(directory, true)},
         {_, :ok} <- {:merge_action, KeydirOwner.merge(directory, timeout)},
         {_, :ok} <- {:unset_merge_in_process, Writer.set_merge_in_progress(directory, false)} do
      :ok
    else
      {_action, e} ->
        Writer.set_merge_in_progress(directory, false)
        {:error, e}
    end
  end

  @doc """
  Shut down the supervision tree for the given database.
  """
  def close(directory) do
    DatabaseSupervisor.stop(directory)
  end
end
