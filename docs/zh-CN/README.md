# FoxKV

<p align="center">
  <img src="https://img.shields.io/badge/Rust-000000?style=for-the-badge&logo=rust&logoColor=white" alt="Rust"/>
  <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white" alt="Redis Compatible"/>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
  <a href="https://github.com/gutsola/foxkv/releases"><img src="https://img.shields.io/github/v/release/gutsola/foxkv" alt="GitHub release"></a>
  <a href="https://github.com/gutsola/foxkv/actions"><img src="https://img.shields.io/github/actions/workflow/status/gutsola/foxkv/ci.yml" alt="CI"></a>
  <a href="https://crates.io/crates/foxkv"><img src="https://img.shields.io/crates/v/foxkv.svg" alt="Crates.io"></a>
</p>

<p align="center">
  <b>🚀 一个用 Rust 编写的高性能、兼容 Redis 协议的内存键值存储</b>
</p>

<p align="center">
  <a href="#特性">特性</a> •
  <a href="#快速开始">快速开始</a> •
  <a href="#安装">安装</a> •
  <a href="#文档">文档</a> •
  <a href="#基准测试">基准测试</a> •
  <a href="#贡献">贡献</a>
</p>

***

## ✨ 特性

- **🔌 兼容 Redis 协议**: 可直接替代 Redis，支持所有标准 Redis 客户端
- **⚡ 高性能**: 基于 Rust 和 Tokio 构建，实现最大吞吐量和低延迟
- **📊 丰富的数据类型**: 支持 String、Hash、List、Set、Sorted Set 及完整命令
- **💾 持久化**: 支持 AOF（追加日志文件）和 RDB 快照
- **🔄 复制**: 主从复制实现高可用
- **🔒 ACL 支持**: 访问控制列表实现细粒度安全控制
- **☁️ 云原生**: 支持 Docker 和 Kubernetes 轻松部署

## 🚀 快速开始

### 使用 Cargo

```bash
# 克隆仓库
git clone https://github.com/gutsola/foxkv.git
cd foxkv

# 构建并运行
cargo run --release --bin foxkv
```

### 使用 Docker

**快速开始:**

```bash
# 使用 Docker 运行
docker run -d --name foxkv -p 6379:6379 gutsola/foxkv:latest
```

### 使用 Redis CLI 连接

```bash
# FoxKV 默认运行在 6379 端口
redis-cli -p 6379

# 测试连接
127.0.0.1:6379> PING
PONG

# 设置和获取键值
127.0.0.1:6379> SET mykey "Hello FoxKV"
OK
127.0.0.1:6379> GET mykey
"Hello FoxKV"
```

## 📦 安装

### 从源码安装

**前置要求:**

- Rust 1.91+ ([从 rustup.rs 安装](https://rustup.rs))

**构建:**

```bash
# 克隆仓库
git clone https://github.com/gutsola/foxkv.git
cd foxkv

# 构建发布版本
cargo build --release --bin foxkv

# 二进制文件位于: target/release/foxkv
```

### 平台特定构建

**Windows:**

```powershell
cargo run --release --bin foxkv
```

**Linux (musl 目标):**

```powershell
$env:RUSTFLAGS='-Clinker=rust-lld'
cargo build --release --bin foxkv --target x86_64-unknown-linux-musl
```

### 配置

使用 `--config` 选项指定配置文件:

```bash
./foxkv --config /path/to/redis.conf
```

如果未指定配置文件，FoxKV 将:
1. 检查当前目录是否存在 `redis.conf` 文件
2. 如果存在则加载该文件
3. 否则使用默认设置



## 💡 使用示例

### 字符串操作

```bash
redis-cli -p 6379

# 基本字符串操作
SET user:1 "Alice"
GET user:1
APPEND user:1 " Smith"
STRLEN user:1

# 数值操作
SET counter 100
INCR counter
INCRBY counter 50
DECR counter
```

## 🏗️ 架构

```
┌─────────────────────────────────────────────────────────┐
│                    FoxKV 架构                            │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│  │   客户端     │    │   客户端     │    │   客户端     │ │
│  │  (Redis)    │    │  (Redis)    │    │  (Redis)    │ │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘ │
│         │                  │                  │        │
│         └──────────────────┼──────────────────┘        │
│                            │                          │
│  ┌─────────────────────────┴─────────────────────────┐ │
│  │              TCP 服务器 (Tokio)                    │ │
│  │         ┌─────────────────────────┐               │ │
│  │         │    RESP 协议            │               │ │
│  │         │      解析器             │               │ │
│  │         └─────────────────────────┘               │ │
│  └─────────────────────────┬─────────────────────────┘ │
│                            │                          │
│  ┌─────────────────────────┴─────────────────────────┐ │
│  │              命令处理器                            │ │
│  │    ┌─────────┐ ┌─────────┐ ┌─────────┐           │ │
│  │    │ String  │ │  Hash   │ │  List   │           │ │
│  │    │  命令   │ │  命令   │ │  命令   │           │ │
│  │    └─────────┘ └─────────┘ └─────────┘           │ │
│  │    ┌─────────┐ ┌─────────┐ ┌─────────┐           │ │
│  │    │  Set    │ │  ZSet   │ │  Conn   │           │ │
│  │    │  命令   │ │  命令   │ │  命令   │           │ │
│  │    └─────────┘ └─────────┘ └─────────┘           │ │
│  └─────────────────────────┬─────────────────────────┘ │
│                            │                          │
│  ┌─────────────────────────┴─────────────────────────┐ │
│  │              存储引擎                              │ │
│  │         ┌─────────────────────┐                   │ │
│  │         │     DashMap         │                   │ │
│  │         │  (并发哈希表)        │                   │ │
│  │         └─────────────────────┘                   │ │
│  └───────────────────────────────────────────────────┘ │
│                            │                           │
│  ┌─────────────────────────┴─────────────────────────┐ │
│  │              持久化层                              │ │
│  │    ┌─────────────┐      ┌─────────────┐          │ │
│  │    │    AOF      │      │    RDB      │          │ │
│  │    │  (追加日志)  │      │  (快照)     │          │ │
│  │    └─────────────┘      └─────────────┘          │ │
│  └───────────────────────────────────────────────────┘ │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 📊 基准测试

与 Redis 的性能对比（单节点，16 线程）:

| 操作  | FoxKV        | Redis 7.0    |
| ----- | ------------ | ------------ |
| SET   | 180K ops/sec | 200K ops/sec |
| GET   | 220K ops/sec | 250K ops/sec |
| LPUSH | 160K ops/sec | 180K ops/sec |
| HSET  | 150K ops/sec | 170K ops/sec |

*在 AMD Ryzen 9 5900X, 32GB RAM 上测试*

运行基准测试:

```bash
cargo bench
```

## 📚 文档

- [架构设计](architecture.md)

## 🐳 Docker 部署

### 快速开始

```bash
# 运行单节点
docker run -d --name foxkv -p 6379:6379 gutsola/foxkv:latest
```

### 环境变量

| 变量       | 说明                                     | 默认值  |
| ---------- | ---------------------------------------- | ------- |
| `RUST_LOG` | 日志级别 (如 `debug`, `info`, `warn`)     | `info`  |

FoxKV 使用 `env_logger` 记录日志。设置 `RUST_LOG=debug` 启用调试日志。

## 🤝 贡献

欢迎贡献！请查看 [CONTRIBUTING.md](../../CONTRIBUTING.md) 了解指南。

### 贡献者快速开始

```bash
# Fork 并克隆
git clone https://github.com/gutsola/foxkv.git
cd foxkv

# 运行测试
cargo test

# 带日志运行
RUST_LOG=debug cargo run --bin foxkv
```

## 🗺️ 路线图

- [x] 核心 Redis 数据类型和命令
- [x] AOF 持久化
- [x] RDB 快照
- [x] 主从复制
- [ ] 集群模式
- [ ] Lua 脚本
- [ ] Streams 数据类型
- [ ] Redis 模块 API

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](../../LICENSE) 文件了解详情。

## 🙏 致谢

- 灵感来自 [Redis](https://redis.io/) - 原始的内存数据存储
- 基于 [Tokio](https://tokio.rs/) 构建 - Rust 的异步运行时
- 存储由 [DashMap](https://github.com/xacrimon/dashmap) 驱动 - 并发哈希表

## 📬 联系方式

- **Issues**: [GitHub Issues](https://github.com/gutsola/foxkv/issues)
- **Discussions**: [GitHub Discussions](https://github.com/gutsola/foxkv/discussions)

***

<p align="center">
  用 ❤️ 和 Rust 构建
</p>
