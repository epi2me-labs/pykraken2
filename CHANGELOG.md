# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.0.1]
### Changed
- Initial release
### Known issues
- Client receives raw output stream from kraken2. The iterator is over chunks
  of the output stream and may not correspond to complete records.
- A race condition may be observed in the server causing it to not respond
  to the end of a clients data when output from kraken.
- The server will hang if a client does not correctly finish its transaction.

