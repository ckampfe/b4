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
    assert :not_found = B4.fetch(dir, "c")
    assert :ok = B4.close(dir)

    assert :ok = B4.new(dir)
    assert :ok = B4.insert(dir, "e", "f")
    assert {:ok, "f"} = B4.fetch(dir, "e")
    assert :not_found = B4.fetch(dir, "c")
    assert :ok = B4.close(dir)

    assert :ok = B4.new(dir)
    assert {:ok, "b"} = B4.fetch(dir, "a")
    assert :not_found = B4.fetch(dir, "c")
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

  test "simple merge", %{dir: dir} do
    Enum.each(1..4, fn i ->
      assert :ok = B4.new(dir)
      assert :ok = B4.insert(dir, "a", i)
      assert :ok = B4.close(dir)
    end)

    # File.ls!(dir) |> IO.inspect(label: "preexisting read files")
    assert Enum.count(File.ls!(dir)) == 4

    # Enum.each(File.ls!(dir), fn file ->
    #   Path.join([dir, file])
    #   |> File.stat!()
    #   |> IO.inspect()
    # end)

    assert :ok = B4.new(dir)
    # one more file because opening the db always creates a fresh writer file
    assert Enum.count(File.ls!(dir)) == 5
    # File.ls!(dir) |> IO.inspect(label: "all files pre merge")
    assert :ok = B4.merge(dir)
    # merging should:
    # - create 1 new merge file (for this particular dataset)
    # - delete the 4 previous read files
    # - leave the current write file untouched
    # for a total of 2 files
    assert Enum.count(File.ls!(dir)) == 2
    # File.ls!(dir) |> IO.inspect(label: "all files post merge")
    assert :ok = B4.close(dir)

    assert :ok = B4.new(dir)
    # we should now have:
    # - the new current write file
    # - the previous write file
    # - the merge file
    assert Enum.count(File.ls!(dir)) == 3
    assert ["a"] = B4.keys(dir)
    assert {:ok, 4} = B4.fetch(dir, "a")
  end

  test "simple merge with deletes", %{dir: dir} do
    Enum.each(1..4, fn i ->
      assert :ok = B4.new(dir)
      assert :ok = B4.insert(dir, "a", i)
      assert :ok = B4.insert(dir, "b", i)
      assert :ok = B4.close(dir)
    end)

    # File.ls!(dir) |> IO.inspect(label: "preexisting read files")
    assert Enum.count(File.ls!(dir)) == 4

    # Enum.each(File.ls!(dir), fn file ->
    #   Path.join([dir, file])
    #   |> File.stat!()
    #   |> IO.inspect()
    # end)

    assert :ok = B4.new(dir)
    assert :ok = B4.delete(dir, "a")

    # one more file because opening the db always creates a fresh writer file
    assert Enum.count(File.ls!(dir)) == 5
    # File.ls!(dir) |> IO.inspect(label: "all files pre merge")
    assert :ok = B4.merge(dir)
    # merging should:
    # - create 1 new merge file (for this particular dataset)
    # - delete the 4 previous read files
    # - leave the current write file untouched
    # for a total of 2 files
    assert Enum.count(File.ls!(dir)) == 2
    # File.ls!(dir) |> IO.inspect(label: "all files post merge")
    assert :ok = B4.close(dir)

    assert :ok = B4.new(dir)
    # we should now have:
    # - the new current write file
    # - the previous write file
    # - the merge file
    assert Enum.count(File.ls!(dir)) == 3
    assert ["b"] = B4.keys(dir)
    assert :not_found = B4.fetch(dir, "a")
    assert {:ok, 4} = B4.fetch(dir, "b")
  end

  test "separate databases remain independent", %{dir: dir1} do
    dir2 = Temp.mkdir!()

    assert :ok = B4.new(dir1)
    assert :ok = B4.new(dir2)

    assert :ok = B4.insert(dir1, "a", 1)
    assert :ok = B4.insert(dir2, "b", 2)

    assert ["a"] = B4.keys(dir1)
    assert ["b"] = B4.keys(dir2)

    assert {:ok, 1} = B4.fetch(dir1, "a")
    assert {:ok, 2} = B4.fetch(dir2, "b")

    assert :not_found = B4.fetch(dir1, "b")
    assert :not_found = B4.fetch(dir2, "a")

    assert :ok = B4.delete(dir1, "b")
    assert {:ok, 2} = B4.fetch(dir2, "b")
  end
end
