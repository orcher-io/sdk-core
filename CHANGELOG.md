# Changelog

## [0.10.0](https://github.com/orcher-io/sdk-core/compare/v0.9.2...v0.10.0) (2026-10-08)


### ⚠ BREAKING CHANGES

* `orcher_sdk_core::proto` now re-exports orcher-proto 0.2, so a crate that also depends on orcher-proto directly moves to 0.2 with it.

### Features

* build against orcher-proto 0.2, with a cancellation cleanup limit and the worker protocol version ([#20](https://github.com/orcher-io/sdk-core/issues/20)) ([fd8cf1a](https://github.com/orcher-io/sdk-core/commit/fd8cf1ab851d856af6af86638e9d77a6419fc2c3))

## [0.9.2](https://github.com/orcher-io/sdk-core/compare/v0.9.1...v0.9.2) (2026-10-08)


### Bug Fixes

* use TLS for https:// addresses without a TlsConfig and name transport error causes ([#18](https://github.com/orcher-io/sdk-core/issues/18)) ([40893aa](https://github.com/orcher-io/sdk-core/commit/40893aa51bccfc9bf37c7251d3925675445356f4))

## [0.9.1](https://github.com/orcher-io/sdk-core/compare/v0.9.0...v0.9.1) (2026-10-07)


### Bug Fixes

* let a running registration driver be stopped so the worker deregisters ([#15](https://github.com/orcher-io/sdk-core/issues/15)) ([408af46](https://github.com/orcher-io/sdk-core/commit/408af46f1ebf316c072802472f075c6c63daeaab))

## [0.9.0](https://github.com/orcher-io/sdk-core/compare/v0.8.2...v0.9.0) (2026-10-05)


### ⚠ BREAKING CHANGES

* `ExecutionResult` has a new field, `reached_steps`, and is `#[non_exhaustive]`; build one with `ExecutionResult::success` or `ExecutionResult::failed`. A non-deterministic activation is retried rather than reported to the engine as a non-retryable `NonDeterminismError` failure.

### Bug Fixes

* catch a replay that leaves recorded steps behind, and retry it instead of failing the run ([#13](https://github.com/orcher-io/sdk-core/issues/13)) ([5323169](https://github.com/orcher-io/sdk-core/commit/532316924fe0d19d9a6a98397676a0b1c4446450))


### Documentation

* read the version badge from the registry with a short cache ([#11](https://github.com/orcher-io/sdk-core/issues/11)) ([ebccb3c](https://github.com/orcher-io/sdk-core/commit/ebccb3c75a790bd70d6d1a21d6fd34179153dbe1))

## [0.8.2](https://github.com/orcher-io/sdk-core/compare/v0.8.1...v0.8.2) (2026-10-04)


### Bug Fixes

* show the README banner on the package registries ([#9](https://github.com/orcher-io/sdk-core/issues/9)) ([77a3117](https://github.com/orcher-io/sdk-core/commit/77a31171f613d0575429260e56e1e0f69e01435a))

## [0.8.1](https://github.com/orcher-io/sdk-core/compare/v0.8.0...v0.8.1) (2026-10-04)


### Bug Fixes

* send and receive messages up to 32 MiB and fail what is too large instead of resending it ([#7](https://github.com/orcher-io/sdk-core/issues/7)) ([8fb58d8](https://github.com/orcher-io/sdk-core/commit/8fb58d8ba007f0c2c64afe2edb0237abef336c4e))


### Changes

* decrement the worker metrics without the deprecated fetch_update ([#2](https://github.com/orcher-io/sdk-core/issues/2)) ([f5ee77c](https://github.com/orcher-io/sdk-core/commit/f5ee77c6a88989b61ccb627620c01ecb33bef3fb))


### Documentation

* rebuild the README in the ORCHER template ([#1](https://github.com/orcher-io/sdk-core/issues/1)) ([5787874](https://github.com/orcher-io/sdk-core/commit/57878742b3f9589ac333ed5862010977fce4871e))

## Changelog
