# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.12.1] - Unreleased

### Added
- Adding or removing interfaces from a device while the device is connected.
  If an interface is added or removed the new device introspection is immediately sent to Astarte.

### Changed
- Callbacks should be set using the `set_events_callbacks` method instead of setting the attributes
  directly.
- `asyncio` loop is argument of `set_events_callbacks` instead of class constructor.

### Fixed
- False values on boolean endpoints for server owned interfaces are correctly processed.

## [0.12.0] - 2023-07-31
### Added
- The following exceptions: `ValidationError`, `PersistencyDirectoryNotFoundError`,
  `InterfaceFileNotFoundError`, `InterfaceFileDecodeError`, `InterfaceNotFoundError`,
  `JWTGenerationError`.
- Persistency support for properties. Server and device properties values are now stored in
  non volatile memory.
  - The new `AstarteDatabase` abstract class has been created. This class can be derived to provide
    a custom database implementation for caching Astarte properties.
  - An optional `database` parameter has been added to the constructor of the `Device`
    class. It can be used to pass a custom database implementation that will be used
    to cache properties.
    If no custom database is specified, a native SQLite database will be used to store the
    properties in a subdirectory of the `persistency_dir`.

### Fixed
- Sending zero payloads for endpoints in property interfaces was unsetting the property.

### Removed
- Drop support for Python 3.7.

## [0.11.0] - 2023-03-16
### Added
- Initial Astarte device SDK release
