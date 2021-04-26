# Mongita CHANGELOG

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
