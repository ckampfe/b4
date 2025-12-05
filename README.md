# B4

[![Elixir CI](https://github.com/ckampfe/b4/actions/workflows/elixir.yml/badge.svg)](https://github.com/ckampfe/b4/actions/workflows/elixir.yml)

## What is this

B4 is an implementation of [Bitcask](https://en.wikipedia.org/wiki/Bitcask).

Bitcask is a fast, immutable, append-only log-structured hash table. It's a database that makes some interesting tradeoffs to achieve low read and write latency.

This README is not a full technical reference for Bitcask. For that, see the wiki or [the paper](https://riak.com/assets/bitcask-intro.pdf).

You can associate a key and a value. You can fetch the value for a key. You can delete that key. You can see all of the live keys.

You can also merge (garbage collect) the on-disk database files, which is important because Bitcask is append only, meaning data is not overwritten on disk, but rather accumlates over time. Because of this, old tuples stick around on disk until they are "merged", which discards dead tuples and preserves live tuples in a new, minimal set of files on disk.


Using it looks like this:

```elixir
$ iex -S mix
iex(1)> B4.new(File.cwd!())
:ok
iex(2)> B4.insert(File.cwd!(), "hello", "world")
:ok
iex(3)> B4.fetch(File.cwd!(), "hello")
{:ok, "world"}
iex(4)> B4.fetch(File.cwd!(), "does not exist")
:not_found
iex(5)> B4.delete(File.cwd!(), "hello")
:ok
iex(6)> B4.fetch(File.cwd!(), "hello")
:not_found
```

The public API looks like this:

```elixir
def new(directory, options \\ [target_file_size: 2 ** 31, sync_strategy: :every_write])
def insert(directory, key, value)
def fetch(directory, key)
def delete(directory, key)
def keys(directory)
def merge(directory, timeout \\ 15_000)
def close(directory)
```

## Design

This is Elixir, so databases in B4 are structured as a supervision tree of processes that manage database operations:

![B4 process supervision tree](https://i.imgur.com/xWlFn7R.png)

A database is a directory, and that directory contains the data files for the database. A directory can contain exactly one database.

The database files on disk exist as two sets: the current write file, and the read files. The write file is the file the database is currently writing to. It is only ever appended to, never never overwritten. It is rotated out and becomes a read file once it reaches a configured size. So, the read files are the previous write files that have been rotated out, exactly like log rotation.

B4 can start an arbitrary number of databases. Databases do not know about and cannot interfere with each other.

Each database requires only 3 processes and 1 ETS table.

`DatabasesSupervisor` starts a `DatabaseSupervisor` per database.

Each `DatabaseSupervisor` supervises a `Writer` and `KeydirOwner` process.

The `Writer` process ensures all write access to the database is serialized. Writing (inserting and deleting) is always single threaded.

`KeydirOwner` starts and owns the ETS table that maps in-memory keys to on-disk values.

Reading is uncoordinated and can be done with arbitrary concurrency (e.g. with `Task.async` or similar).

This is because in Bitcask, all data files on disk except the current writer file are immutable. Once a writer file is rotated out and becomes a read-only file, it is never changed (except through the "merge" process that eventually removes dead tuples). Because of this, arbitrary readers can read from the on-disk database files without fear that they will read partially written data.

### Writing

Writing to a database involves appending append a record to a file disk and then updating the in-memory "keydir" structure (ETS table) that associates a key with the location of that data in that file on disk. It look slike `K -> (file_id, file_offset, data_length)`. The "keydir" is updated after the write to disk has completed. This means that the write is considered committed only when the keydir has been updated. Because ETS guarantees isolation, no intermediate updates to the keydir can be seen, so if a given key exists in the keydir, that data exists on disk.

### Reading

Reading is the inverse. Readers look up a key in the keydir, which provides them with the file, offset, and length of data. They then perform an `:erlang.pread` (offset read) at that location, loading the data into memory, deserialize it, and return it to the caller. This makes reads in B4 effectively constant time, subject to the time complexity of the ETS keydir read itself.

### Merging

In merging, the `KeydirOwner` process reads through all read files in the database and rewrites their contents into a new set of read files, preserving only live data. All old inserts and deletes are discarded. This process then updates the keydir with the new locations of any data on disk, which causes any readers to obseve the data at its new locations.

Because the read files on disk are immutable and the keydir is only updated once data has been moved, this process is transparent to both readers and writers, who may continue to access the database throughout the merge process.

There exists a small window of time during the swapping from the previous read file set to the new read file set *before* the keydir points to the newest data where it is possible that a reader could attempt a read of a now-deleted file. This is handled with retries, since the keydir is eventually updated with the new data location. This is not specificied in the Bitcask paper, but is rather a choice I made in lieu of file locking that would halt reads outright. Note that durability is not affected since the old data is only removed after the new data has been written, but readers may not yet be able to retrieve the location of the newest data.

Writing is not affected by the merge process - other than pausing write file rotation until the merge is complete - since the merge process does not touch the current active write file.
