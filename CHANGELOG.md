# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.12.0] - Unreleased
### Added
- The following exceptions: `ValidationError`, `PersistencyDirectoryNotFoundError`,
  `InterfaceFileNotFoundError`, `InterfaceFileDecodeError`, `InterfaceNotFoundError`,
  `JWTGenerationError`.

### Fixed
- Sending zero payloads for endpoints in property interfaces was unsetting the property.

## [0.11.0] - 2023-03-16
### Added
- Initial Astarte Device SDK release
