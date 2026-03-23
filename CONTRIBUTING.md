# Contributing to FoxKV

First off, thank you for considering contributing to FoxKV! It's people like you that make FoxKV such a great tool.

## Code of Conduct

This project and everyone participating in it is governed by our commitment to provide a friendly, safe, and welcoming environment for all.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to see if the problem has already been reported. When you are creating a bug report, please include as many details as possible:

- **Use a clear and descriptive title**
- **Describe the exact steps to reproduce the problem**
- **Provide specific examples to demonstrate the steps**
- **Describe the behavior you observed and what behavior you expected**
- **Include code samples and command output**

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:

- **Use a clear and descriptive title**
- **Provide a step-by-step description of the suggested enhancement**
- **Provide specific examples to demonstrate the enhancement**
- **Explain why this enhancement would be useful**

### Pull Requests

1. Fork the repository
2. Create a new branch from `main` (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run the tests (`cargo test`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Development Setup

### Prerequisites

- [Rust](https://rustup.rs/) 1.80 or higher
- [Git](https://git-scm.com/)
- [Redis CLI](https://redis.io/docs/getting-started/installation/) (for testing)

### Building

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/foxkv.git
cd foxkv

# Build the project
cargo build

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run --bin foxkv
```

### Project Structure

```
foxkv/
├── src/
│   ├── bin/              # Binary entry points
│   ├── command/          # Redis command implementations
│   │   ├── shared/       # Shared command utilities
│   │   └── types/        # Command type definitions
│   ├── config/           # Configuration parsing
│   ├── persistence/      # AOF and RDB persistence
│   ├── replication/      # Master-replica replication
│   ├── resp/             # RESP protocol implementation
│   ├── server/           # TCP server and connection handling
│   ├── storage/          # Storage engine (DashMap)
│   ├── app_context.rs    # Application context
│   ├── lib.rs            # Library exports
│   └── main.rs           # Main entry point
├── docs/                 # Documentation
└── tests/                # Integration tests
```

## Coding Guidelines

### Rust Style Guide

We follow the standard Rust style guidelines:

- Use `rustfmt` for code formatting: `cargo fmt`
- Use `clippy` for linting: `cargo clippy -- -D warnings`
- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)

### Code Organization

- **Commands**: Each Redis command should be implemented in `src/command/types/`
- **Storage**: Use the storage abstraction in `src/storage/`
- **Error Handling**: Use `Result` types and the `?` operator
- **Async**: Use Tokio for async operations

### Testing

- Write unit tests for new functionality
- Add integration tests for command implementations
- Ensure all tests pass before submitting PR

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_name
```

### Documentation

- Add doc comments (`///`) for public APIs
- Update the README if adding new features
- Add examples for complex functionality

## Commit Message Guidelines

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only changes
- `style`: Code style changes (formatting, semicolons, etc.)
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `perf`: Performance improvement
- `test`: Adding or correcting tests
- `chore`: Changes to build process or auxiliary tools

Example:
```
feat: add support for ZRANGEBYLEX command

Implement the ZRANGEBYLEX command for sorted sets to allow
lexicographical range queries.
```

## Review Process

1. **Automated Checks**: CI will run tests and linting
2. **Code Review**: Maintainers will review your code
3. **Changes**: Address any requested changes
4. **Merge**: Once approved, your PR will be merged

## Community

- **Issues**: [GitHub Issues](https://github.com/gutsola/foxkv/issues)
- **Discussions**: [GitHub Discussions](https://github.com/gutsola/foxkv/discussions)

## Questions?

Feel free to open an issue with your question or reach out in discussions.

Thank you for contributing! 🎉
