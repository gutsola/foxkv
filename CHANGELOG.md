# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial open source release
- Comprehensive documentation

## [0.1.0] - 2026-03-24

### Added

#### Core Features
- **Redis Protocol Compatibility**: Full RESP protocol implementation
- **String Commands**: Complete set of string operations (GET, SET, INCR, DECR, APPEND, etc.)
- **Hash Commands**: Full hash support (HSET, HGET, HGETALL, HINCRBY, etc.)
- **List Commands**: All list operations including blocking commands (LPUSH, RPOP, BLPOP, etc.)
- **Set Commands**: Complete set operations (SADD, SINTER, SUNION, etc.)
- **Sorted Set Commands**: Full zset support (ZADD, ZRANGE, ZRANK, etc.)

#### Persistence
- **AOF (Append-Only File)**: Command logging for durability
- **RDB Snapshots**: Point-in-time database snapshots
- **AOF Rewrite**: Background AOF file rewriting

#### Replication
- **Master-Replica Replication**: Asynchronous data replication
- **Partial Resynchronization**: PSYNC support for efficient reconnection
- **Replica Configuration**: Flexible replicaof configuration

#### Server Features
- **Connection Management**: CLIENT commands, connection limits
- **ACL Support**: Access Control Lists for security
- **Configuration**: Runtime configuration with CONFIG commands
- **Monitoring**: INFO commands

#### Performance
- **Async I/O**: Built on Tokio for high concurrency
- **Lock-free Storage**: DashMap for concurrent access
- **Zero-copy Parsing**: Efficient RESP parsing

### Technical Details

#### Dependencies
- `tokio`: Async runtime
- `dashmap`: Concurrent hash map
- `bytes`: Efficient byte handling
- `ahash`: Fast hashing
- `smallvec`: Stack-allocated vectors

#### Supported Platforms
- Linux (x86_64, musl)
- Windows (x86_64)
- macOS (x86_64, Apple Silicon)

[Unreleased]: https://github.com/gutsola/foxkv/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/gutsola/foxkv/releases/tag/v0.1.0
