# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in FoxKV, please report it responsibly.

### How to Report

**Please do not open public issues for security vulnerabilities.**

Instead, please email security concerns to: **security@foxkv.dev** (or create a private security advisory on GitHub)

Include the following information:
- Description of the vulnerability
- Steps to reproduce (if applicable)
- Potential impact
- Suggested fix (if any)

### Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 1 week
- **Fix and Release**: Depends on severity
  - Critical: 1-2 weeks
  - High: 2-4 weeks
  - Medium/Low: Next scheduled release

### Security Best Practices

When deploying FoxKV:

1. **Network Security**
   - Bind to localhost (127.0.0.1) if only local access is needed
   - Use firewall rules to restrict access
   - Enable TLS for remote connections (when available)

2. **Authentication**
   - Use `requirepass` to set a strong password
   - Use ACL to restrict command access

3. **Persistence**
   - Set appropriate file permissions on AOF/RDB files
   - Regular backups

## Security Features

- ACL (Access Control Lists) for fine-grained permissions
- Password authentication
- Command renaming/disabling

## Known Limitations

- No built-in TLS support (planned for future release)
- No encryption at rest (rely on OS-level encryption)

## Acknowledgments

We thank the following security researchers who have responsibly disclosed vulnerabilities:

*None yet - be the first!*
