defmodule B4.Keydir do
  @moduledoc false

  def new do
    :ets.new(:b4_table, [
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: :auto
    ])
  end

  def insert(tid, key, file_id, entry_size, file_position, entry_id)
      when is_integer(entry_size) and is_integer(file_position) do
    :ets.insert(
      tid,
      {key, file_id, entry_size, file_position, entry_id}
    )
  end

  def fetch(tid, key) do
    case :ets.lookup(tid, key) do
      [{^key, _file_id, _entry_size, _file_position, _entry_id} = mapping] ->
        {:ok, mapping}

      [] ->
        :error
    end
  end

  def keys(tid) do
    tid
    |> :ets.match({:"$1", :_, :_, :_, :_})
    |> Enum.map(fn [k] -> k end)
  end

  def delete(tid, key) do
    :ets.delete(tid, key)
  end
end
