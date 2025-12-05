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

