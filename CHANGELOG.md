# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Changed
- **BC break**: Upgrade to Flysystem v2. See
  [migration guide](https://flysystem.thephpleague.com/docs/upgrade-from-1.x/).
- **BC break**: The PDO connection must use the exception error mode,
  `\PDO::ERRMODE_EXCEPTION`, which is the default since PHP v8.0. The
  constructor throws `\InvalidArgumentException` for any other error mode.
  Database errors are thrown as `\PDOException`, rather than returning `false`.
- **BC break**: The `PdoAdapter` constructor no longer accepts `null` for the
  config; omit the argument to use the default configuration.
- **BC break**: Expiry date is always set & retrieved using \DateTimeImmutable.
- **BC break**: `deleteExpired()` no longer accepts `null`; omit the argument to
  use the current time.
- Expired files are excluded by the DB queries, rather than being filtered after
  they have been read.
- Writing a new file with an expiry in the past does not store the file.
- **BC break**: DB schema adds a unique index on `path` and an index on
  `expiry`. Paths are case-sensitive on MySQL, using the `utf8mb4_bin`
  collation. See the README for updating existing tables.
- `createDir()` returns the existing directory, rather than creating a
  duplicate.
### Fixed
- Delete file storage chunks for expired files when calling `deleteExpired()`.
- `has()` and `listContents()` no longer include expired files.
- A file can be written to a path where a previous file has expired, rather
  than failing because the expired file still exists.
- Temporary files are removed when writing or updating a file fails, or when
  updating a file that does not exist.
- Update the last modified time on SQLite when a file is overwritten, moved or
  has its visibility changed. MySQL already did this with `ON UPDATE`.
### Removed
- Remove support for PHP versions <= v8.0 as they are no longer
  [actively supported](https://php.net/supported-versions.php) by the PHP project.

## [2.2.0] - 2026-10-13
### Changed
- Set optional expiry date using \DateTimeImmutable. Setting the expiry value as
  a string is deprecated. Reading the file metadata will still return a string
  for the expiry date, to avoid a BC break.

## [2.1.0] - 2022-09-01
### Added
- Allow `rename()` to work on directories where no specific directory entry exists. 
### Changed
- DB schema has been updated to include the optional `expiry` and `meta` columns.
- DB schema character set has been updated to use `utf8mb4` instead of the
  deprecated `utf8`.
- DB schema for Sqlite updated to store current timestamp. Sqlite always runs as
  UTC, so this is converted when retrieving the metadata.
### Fixed
- Type error when using `deleteExpired()`.
- Error in response data when writing a new file with additional metadata.

## [2.0.0] - 2022-08-22
### Added
- Add support for PHP v8
- Type declarations have been added to all properties, method parameters and
  return types where possible.
### Changed
- **BC break**: `copy()` and `rename()` no longer return metadata array,
  instead return boolean to match the interface.
### Removed
- **BC break**: Remove support for PHP versions <= v7.3 as they are no longer
  [actively supported](https://php.net/supported-versions.php) by the PHP project

## [1.1.0] - 2018-03-24
### Added
- Allow associating expiry times and metadata with a file.
### Fixed
- Extraction of highly compressed data causes memory issues.
- Improved temporary file handling so files are not left after operation.

## [1.0.0] - 2017-05-15
Stable release
### Changed
- Improve the generation of temporary filename. HT @glensc

## [0.0.3] - 2016-03-10
Development release
