# Mongita CHANGELOG

## [1.0.1] - 2021-06-21
Mongita was not actually compatible with Python 3.6. Bumped python_requires to 3.7. No other consequential changes.

## [1.0.0] - 2021-05-07
Version bump to 1.0.0. Despite apparent usage, no major bugs have been reported. The public API should not experience breaking changes moving forward so 1.0.0 seems appropriate.
### Added
- mongitasync command

### Fixed
- Drop database was only dropping every other collection


## [0.2.0] - 2021-04-29
### Added
- Add clone to cursor
- Add ReadConcern / WriteConcern dummies
- Add tests for mongoengine (still requires monkeypatching)

### Fixed
- Unimplemented parameter warning wasn't doing string interpolation correctly

## [0.1.3] - 2021-04-26
### Added
- Added support for in-list equality. E.g. usually when you have a query like
  {"key": "val"}, you mean doc["key"] == "val" but when doc["key"] is a list,
  this can also be "val" in doc["key"].

## [0.1.2] - 2021-04-23
### Added
- License added to package deployment

## [0.1.1] - 2021-04-21
### Changed
- Removed repr from result classes to be more inline with pymongo

## [0.1.0] - 2021-04-19
First release
