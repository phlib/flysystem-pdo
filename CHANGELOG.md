# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
- Type declarations have been added to all method parameters and return types
  where possible.
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
