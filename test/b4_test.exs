defmodule B4Test do
  use ExUnit.Case
  doctest B4

  setup_all do
    Temp.track!()
    :ok
  end

  setup do
    dir = Temp.mkdir!()
    %{dir: dir}
  end

  test "trivial roundtrip", %{dir: dir} do
    assert :ok = B4.new(dir)
    assert :ok = B4.insert(dir, "a", "b")
    assert {:ok, "b"} = B4.fetch(dir, "a")
  end

  test "loads existing data from one file", %{dir: dir} do
    assert :ok = B4.new(dir)
    assert :ok = B4.insert(dir, "a", "b")
    assert {:ok, "b"} = B4.fetch(dir, "a")

    assert :ok = B4.close(dir)
    assert :ok = B4.new(dir)
    assert {:ok, "b"} = B4.fetch(dir, "a")
  end

  test "loads from multiple files", %{dir: dir} do
    Enum.each([{"a", "b"}, {"c", "d"}, {"e", "f"}], fn {k, v} ->
      assert :ok = B4.new(dir)
      assert :ok = B4.insert(dir, k, v)
      assert {:ok, ^v} = B4.fetch(dir, k)
      assert :ok = B4.close(dir)
    end)

    assert :ok = B4.new(dir)
    assert {:ok, "b"} = B4.fetch(dir, "a")
    assert {:ok, "d"} = B4.fetch(dir, "c")
    assert {:ok, "f"} = B4.fetch(dir, "e")
  end

  test "loads from multiple files with deletes", %{dir: dir} do
    assert :ok = B4.new(dir)
    assert :ok = B4.insert(dir, "a", "b")
    assert {:ok, "b"} = B4.fetch(dir, "a")
    assert :ok = B4.close(dir)

    # DELETE HERE, for "c" key
    assert :ok = B4.new(dir)
    assert :ok = B4.insert(dir, "c", "d")
    assert {:ok, "d"} = B4.fetch(dir, "c")
    assert :ok = B4.delete(dir, "c")
    assert :error = B4.fetch(dir, "c")
    assert :ok = B4.close(dir)

    assert :ok = B4.new(dir)
    assert :ok = B4.insert(dir, "e", "f")
    assert {:ok, "f"} = B4.fetch(dir, "e")
    assert :error = B4.fetch(dir, "c")
    assert :ok = B4.close(dir)

    assert :ok = B4.new(dir)
    assert {:ok, "b"} = B4.fetch(dir, "a")
    assert :error = B4.fetch(dir, "c")
    assert {:ok, "f"} = B4.fetch(dir, "e")
  end

  test "loads large values", %{dir: dir} do
    assert :ok = B4.new(dir)
    large_1 = :rand.bytes(20000)
    large_2 = :rand.bytes(20000)
    large_3 = :rand.bytes(20000)

    assert :ok = B4.insert(dir, "a", large_1)
    assert :ok = B4.insert(dir, "b", large_2)

    assert {:ok, ^large_1} = B4.fetch(dir, "a")
    assert {:ok, ^large_2} = B4.fetch(dir, "b")

    assert :ok = B4.insert(dir, "b", large_3)
    assert {:ok, ^large_3} = B4.fetch(dir, "b")
  end

  test "creates new write file when target_file_size", %{dir: dir} do
    target_file_size = 50
    assert :ok = B4.new(dir, target_file_size: target_file_size)
    assert Enum.count(File.ls!(dir)) == 1

    assert :ok = B4.insert(dir, "a", :rand.bytes(50))
    assert Enum.count(File.ls!(dir)) == 1

    # file creation happens on the subsequent write above the limit
    assert :ok = B4.insert(dir, "a", :rand.bytes(50))
    assert Enum.count(File.ls!(dir)) == 2

    assert :ok = B4.insert(dir, "a", :rand.bytes(50))
    assert Enum.count(File.ls!(dir)) == 3

    assert Enum.all?(File.ls!(dir), fn path ->
             %{size: size} = File.stat!(Path.join([dir, path]))
             size >= target_file_size
           end)
  end

  test "keys", %{dir: dir} do
    assert :ok = B4.new(dir)
    assert [] = B4.keys(dir)

    assert :ok = B4.insert(dir, "a", "b")
    assert ["a"] = B4.keys(dir)

    assert :ok = B4.insert(dir, "c", "d")
    assert ["a", "c"] = Enum.sort(B4.keys(dir))

    assert :ok = B4.delete(dir, "a")
    assert ["c"] = Enum.sort(B4.keys(dir))

    assert :ok = B4.delete(dir, "c")
    assert [] = Enum.sort(B4.keys(dir))
  end
end
