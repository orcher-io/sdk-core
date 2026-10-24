# Changelog

## [0.22.0](https://github.com/orcher-io/sdk-core/compare/v0.21.0...v0.22.0) (2026-10-24)


### Features

* report step-id schemes and assign the kernel's ids to runs bound to it ([#138](https://github.com/orcher-io/sdk-core/issues/138)) ([2096c74](https://github.com/orcher-io/sdk-core/commit/2096c74727fb1a5074ab167f624a03a9cfd1d4a1))


### Chores

* release 0.22.0 ([37fc290](https://github.com/orcher-io/sdk-core/commit/37fc290e8a045aed7ad4ee7b9adacb29a72c6965))

## [0.21.0](https://github.com/orcher-io/sdk-core/compare/v0.20.1...v0.21.0) (2026-10-24)


### Features

* answer client calls as encoded client protocol messages ([#136](https://github.com/orcher-io/sdk-core/issues/136)) ([5e14156](https://github.com/orcher-io/sdk-core/commit/5e141562319de0c2495a57ca6a9fb045bc438a76))

## [0.20.1](https://github.com/orcher-io/sdk-core/compare/v0.20.0...v0.20.1) (2026-10-23)


### Bug Fixes

* honour the workflow client's timeout on every call ([#134](https://github.com/orcher-io/sdk-core/issues/134)) ([1706708](https://github.com/orcher-io/sdk-core/commit/17067080f519f249fef300f187bd99a9a5b12858))

## [0.20.0](https://github.com/orcher-io/sdk-core/compare/v0.19.0...v0.20.0) (2026-10-22)


### ⚠ BREAKING CHANGES

* narrow core's protocol re-export to the kernel and host protocols ([#132](https://github.com/orcher-io/sdk-core/issues/132))
* start workflows and manage namespaces with core's own types ([#131](https://github.com/orcher-io/sdk-core/issues/131))
* connect clients and workers with core's own options ([#130](https://github.com/orcher-io/sdk-core/issues/130))
* report refused calls with core's own status ([#129](https://github.com/orcher-io/sdk-core/issues/129))
* drivers, pollers, the state module, the request types and the old module paths are no longer public; use orcher_sdk_core::Host, the kernel and the clients. kernel::check_activation replaces
* orcher-sdk-host is no longer published; depend on orcher-sdk-core and use orcher_sdk_core::Host instead.

### Features

* give language bindings the host's work as encoded messages ([#128](https://github.com/orcher-io/sdk-core/issues/128)) ([e21ab77](https://github.com/orcher-io/sdk-core/commit/e21ab7702b03e7fdc3c3f9702627466001751f00))


### Bug Fixes

* report a worker's load in its heartbeat ([#125](https://github.com/orcher-io/sdk-core/issues/125)) ([481242c](https://github.com/orcher-io/sdk-core/commit/481242cdb4a04adb959f8d5f2539f69fe8700ff8))


### Changes

* connect clients and workers with core's own options ([#130](https://github.com/orcher-io/sdk-core/issues/130)) ([672a0fd](https://github.com/orcher-io/sdk-core/commit/672a0fdd51132914c890b1dd5fa0a4c0c8f1b2ad))
* convert to and from the wire in one module ([#127](https://github.com/orcher-io/sdk-core/issues/127)) ([e7f8023](https://github.com/orcher-io/sdk-core/commit/e7f802399d0985f634f2aec588df6270d55d7fde))
* make the worker machinery private to core ([#126](https://github.com/orcher-io/sdk-core/issues/126)) ([da5fff0](https://github.com/orcher-io/sdk-core/commit/da5fff0966dfce352497adf2e6e6122a116ddcf7))
* narrow core's protocol re-export to the kernel and host protocols ([#132](https://github.com/orcher-io/sdk-core/issues/132)) ([a9dbf31](https://github.com/orcher-io/sdk-core/commit/a9dbf315077d3725a54590c1a86ac665ffb293f5))
* report refused calls with core's own status ([#129](https://github.com/orcher-io/sdk-core/issues/129)) ([99a71d5](https://github.com/orcher-io/sdk-core/commit/99a71d58dbbbb660ef39be690b2fd62bbbb041a5))
* run the worker host from core ([#123](https://github.com/orcher-io/sdk-core/issues/123)) ([d9edc5f](https://github.com/orcher-io/sdk-core/commit/d9edc5fd92f4ea37275fb7770245f77f5a19d1a6))
* start workflows and manage namespaces with core's own types ([#131](https://github.com/orcher-io/sdk-core/issues/131)) ([37a989d](https://github.com/orcher-io/sdk-core/commit/37a989d70b0a86ea7145464d5b20838a49867b02))


### Documentation

* add an upgrade guide for 0.20 and bring the README up to date ([#133](https://github.com/orcher-io/sdk-core/issues/133)) ([d3a74a3](https://github.com/orcher-io/sdk-core/commit/d3a74a30f76e571dd5d0d0e68149f5f736559056))

## [0.19.0](https://github.com/orcher-io/sdk-core/compare/v0.18.0...v0.19.0) (2026-10-21)


### Features

* ask for as many items as there is room for in one poll ([#119](https://github.com/orcher-io/sdk-core/issues/119)) ([1518df2](https://github.com/orcher-io/sdk-core/commit/1518df2b6ceb1fa35b5f6c34769d0c7ac2addbca))
* poll only with room to run what the poll brings ([#117](https://github.com/orcher-io/sdk-core/issues/117)) ([a15bf63](https://github.com/orcher-io/sdk-core/commit/a15bf63c2ae28d34933677468ddcee47abad0eb7))
* send the heartbeats of tasks due together in one call ([#120](https://github.com/orcher-io/sdk-core/issues/120)) ([1746177](https://github.com/orcher-io/sdk-core/commit/17461778332645b0fe04748ad6294e569bce40c2))


### Documentation

* say a move to a newer orcher-proto is a minor release ([#121](https://github.com/orcher-io/sdk-core/issues/121)) ([59476e9](https://github.com/orcher-io/sdk-core/commit/59476e9407b400ad193bd856ab5f1c949a0b177f))

## [0.18.0](https://github.com/orcher-io/sdk-core/compare/v0.17.1...v0.18.0) (2026-10-20)


### ⚠ BREAKING CHANGES

* the driver and poller configurations gain a wire field (the worker's WireMetrics; each has its own by default), connect_channel takes the metrics to record into and returns a WireChannel, and the clients built on a worker's channels are generic over WireChannel instead of Channel.

### Features

* count the work a worker hands its language and reports back ([#116](https://github.com/orcher-io/sdk-core/issues/116)) ([24aa6a2](https://github.com/orcher-io/sdk-core/commit/24aa6a2b85816ea5ecb93f9659ae87f89118e91c))
* measure a worker's calls to the engine on the wire ([#115](https://github.com/orcher-io/sdk-core/issues/115)) ([5f045e5](https://github.com/orcher-io/sdk-core/commit/5f045e5d9f861905d1a3b017982d439f63395f37))
* scale each driver's pollers with the work their polls bring ([#113](https://github.com/orcher-io/sdk-core/issues/113)) ([a2d9c5a](https://github.com/orcher-io/sdk-core/commit/a2d9c5afa7acafcee2b200cb61b5c7bde074cda1)), closes [#34](https://github.com/orcher-io/sdk-core/issues/34)

## [0.17.1](https://github.com/orcher-io/sdk-core/compare/v0.17.0...v0.17.1) (2026-10-19)


### Features

* carry a workflow failure's non-retryable mark to the engine ([#103](https://github.com/orcher-io/sdk-core/issues/103)) ([84f189c](https://github.com/orcher-io/sdk-core/commit/84f189c7501660960c4f2adc9b9c8bd6aa96224d)), closes [#102](https://github.com/orcher-io/sdk-core/issues/102)
* hold a failing activation that left a recorded step behind ([#104](https://github.com/orcher-io/sdk-core/issues/104)) ([71ac27d](https://github.com/orcher-io/sdk-core/commit/71ac27d51dcbd5b21b56083971ac3b4775266f5d))

## [0.17.0](https://github.com/orcher-io/sdk-core/compare/v0.16.0...v0.17.0) (2026-10-18)


### ⚠ BREAKING CHANGES

* ExecutionRequest.continue_as_new is now restart_fresh, the name ExecutionResult already uses for a restart. It is still always false from ExecutionRequest::new; a request serialized without it reads as false.

### Bug Fixes

* name ExecutionRequest's restart flag restart_fresh ([#101](https://github.com/orcher-io/sdk-core/issues/101)) ([b9ea22d](https://github.com/orcher-io/sdk-core/commit/b9ea22dd3d0b9e3d514fe86225dae772c36b04d1))


### Documentation

* name orcher-proto 0.9 in the 0.16.0 changelog ([#99](https://github.com/orcher-io/sdk-core/issues/99)) ([31b9c91](https://github.com/orcher-io/sdk-core/commit/31b9c910dad4ea610c39416c3d1287c5cbafce0f))

## [0.16.0](https://github.com/orcher-io/sdk-core/compare/v0.15.1...v0.16.0) (2026-10-17)


### ⚠ BREAKING CHANGES

* builds on orcher-proto 0.9, whose Resolution gains sequence (0.8) and whose kernel RestartFresh gains retry_policy (0.9); an SDK depending on orcher-proto directly must move to 0.9 in the same change.
* StartChildWorkflowCommand and RestartFreshCommand gain retry_policy (a new WorkflowRetryPolicy). Both now implement Default, as does OrphanPolicy (Cancel), so build them with ..Default::default().

### Features

* let every bridge command be built with room for new fields ([#98](https://github.com/orcher-io/sdk-core/issues/98)) ([7c60de9](https://github.com/orcher-io/sdk-core/commit/7c60de90c3e1d7b97a976005b4ff10c6f77b57de)), closes [#97](https://github.com/orcher-io/sdk-core/issues/97)
* number each resolution in the order the run observes it ([#92](https://github.com/orcher-io/sdk-core/issues/92)) ([c2d239e](https://github.com/orcher-io/sdk-core/commit/c2d239e955dbe8c5e15d593d6092e19241e1a927))


### Bug Fixes

* build proto messages with room for new fields and send child retry policies ([#95](https://github.com/orcher-io/sdk-core/issues/95)) ([79c4025](https://github.com/orcher-io/sdk-core/commit/79c4025ecb2dab4866eb115a831f172a85d2f5d6))

## [0.15.1](https://github.com/orcher-io/sdk-core/compare/v0.15.0...v0.15.1) (2026-10-15)


### Bug Fixes

* settle a cancelled step as cancelled ([#88](https://github.com/orcher-io/sdk-core/issues/88)) ([6771e49](https://github.com/orcher-io/sdk-core/commit/6771e49c100b0c8c2d032a08a9bab552e3e07859)), closes [#87](https://github.com/orcher-io/sdk-core/issues/87)

## [0.15.0](https://github.com/orcher-io/sdk-core/compare/v0.14.1...v0.15.0) (2026-10-14)


### ⚠ BREAKING CHANGES

* builds on orcher-proto 0.7 (RestartFresh gains execution_timeout); an SDK depending on orcher-proto directly must move to 0.7 in the same change. DEFAULT_RETRY_ATTEMPTS is removed: attempts have no default, 0 is unlimited.

### Bug Fixes

* carry a kernel answer to the engine without losing what the SDK said ([#85](https://github.com/orcher-io/sdk-core/issues/85)) ([7283ed5](https://github.com/orcher-io/sdk-core/commit/7283ed56c82227e2f1e2254e887a0a01a4a4f764))

## [0.14.1](https://github.com/orcher-io/sdk-core/compare/v0.14.0...v0.14.1) (2026-10-14)


### Features

* let a worker set its poll counts and heartbeat interval through the host ([#83](https://github.com/orcher-io/sdk-core/issues/83)) ([ba0ad50](https://github.com/orcher-io/sdk-core/commit/ba0ad5018aabe9059cbec533a76c00609518e0a5))

## [0.14.0](https://github.com/orcher-io/sdk-core/compare/v0.13.1...v0.14.0) (2026-10-13)


### ⚠ BREAKING CHANGES

* builds on orcher-proto 0.6, whose Resolution gains the ChildStarted variant; an SDK depending on orcher-proto directly must move to 0.6 in the same change.

### Features

* resolve a child the run started before it settles ([#81](https://github.com/orcher-io/sdk-core/issues/81)) ([89807c4](https://github.com/orcher-io/sdk-core/commit/89807c433eb25f1996d21e7a2317a9d5580c843c))

## [0.13.1](https://github.com/orcher-io/sdk-core/compare/v0.13.0...v0.13.1) (2026-10-13)


### Features

* drain the worker host on shutdown ([#73](https://github.com/orcher-io/sdk-core/issues/73)) ([bf5b10e](https://github.com/orcher-io/sdk-core/commit/bf5b10ee63b582c31febcad4c74f144c60980dc4))
* run actors in the host ([#71](https://github.com/orcher-io/sdk-core/issues/71)) ([8cb086f](https://github.com/orcher-io/sdk-core/commit/8cb086f21ce2a75dbe8934987f3b4c3ccb14a762))


### Bug Fixes

* count steps sent again as reached in a kernel answer ([#78](https://github.com/orcher-io/sdk-core/issues/78)) ([45a827e](https://github.com/orcher-io/sdk-core/commit/45a827eb795bca0286c5dfa6de73fd6341f78808))

## [0.13.0](https://github.com/orcher-io/sdk-core/compare/v0.12.0...v0.13.0) (2026-10-13)


### ⚠ BREAKING CHANGES

* the proto types sdk-core uses and re-exports are now orcher-proto 0.5's. An SDK that also depends on orcher-proto directly must move to 0.5 in the same change. sdk-rust, sdk-py and sdk-ts all compile unchanged with orcher-sdk-core 0.13 and orcher-proto 0.5, with one orcher-proto in each build.
* the proto types sdk-core uses and re-exports are now orcher-proto 0.5's. An SDK that also depends on orcher-proto directly must move to 0.5 in the same change. sdk-rust, sdk-py and sdk-ts all compile unchanged with orcher-sdk-core 0.13 and orcher-proto 0.5, with one orcher-proto in each build.

### Features

* build on orcher-proto 0.5 ([#67](https://github.com/orcher-io/sdk-core/issues/67)) ([aecb919](https://github.com/orcher-io/sdk-core/commit/aecb919c95fbfcc372f3b2c32c5d769aa51526ed))
* hand tasks to the language by pull ([#65](https://github.com/orcher-io/sdk-core/issues/65)) ([27cc6c8](https://github.com/orcher-io/sdk-core/commit/27cc6c8dc9cced11542307dccab160b62fcb6d65))
* hand workflow activations to the language through the kernel protocol ([#68](https://github.com/orcher-io/sdk-core/issues/68)) ([ea774e6](https://github.com/orcher-io/sdk-core/commit/ea774e6841ad5c1a32c7fcb0d50276939a790dd1))
* run worker sessions in the host ([#70](https://github.com/orcher-io/sdk-core/issues/70)) ([f077e95](https://github.com/orcher-io/sdk-core/commit/f077e95f795b9ba72be3e41af71ca446d92e8891))


### Bug Fixes

* stop a session queue's poller when its last session ends ([#69](https://github.com/orcher-io/sdk-core/issues/69)) ([b9d3d9f](https://github.com/orcher-io/sdk-core/commit/b9d3d9f6aa26ec8072097b930449b75a618ab316))

## [0.12.0](https://github.com/orcher-io/sdk-core/compare/v0.11.0...v0.12.0) (2026-10-12)


### ⚠ BREAKING CHANGES

* the proto types sdk-core uses and re-exports are now orcher-proto 0.4's. An SDK that also depends on orcher-proto directly must move to 0.4 in the same change. sdk-rust, sdk-py and sdk-ts all compile unchanged with orcher-sdk-core 0.12 and orcher-proto 0.4, with one orcher-proto in each build.

### Features

* add the worker host crate with its configuration and lifecycle ([#64](https://github.com/orcher-io/sdk-core/issues/64)) ([f5de4ff](https://github.com/orcher-io/sdk-core/commit/f5de4ff08d7fc5940ed593e8d7838f9bd866e7a8))
* build on orcher-proto 0.4 ([#63](https://github.com/orcher-io/sdk-core/issues/63)) ([2887950](https://github.com/orcher-io/sdk-core/commit/2887950bb5e08a42ae0624a5d6568979e8f7902e))
* define the kernel's step-id scheme for new runs ([#59](https://github.com/orcher-io/sdk-core/issues/59)) ([a6765a4](https://github.com/orcher-io/sdk-core/commit/a6765a462d6ea64ef44bb3f2a5e7aa8fc4c4cc08))
* give every core error a machine-readable code ([#61](https://github.com/orcher-io/sdk-core/issues/61)) ([1e9256b](https://github.com/orcher-io/sdk-core/commit/1e9256b6294307b643c685873c90f8fdacded12b))
* give runs one deterministic random source in the kernel ([#60](https://github.com/orcher-io/sdk-core/issues/60)) ([6efd63d](https://github.com/orcher-io/sdk-core/commit/6efd63dc653e90aeaf43286f57459b24467bdfcb))
* resolve an activation's history in the kernel ([#54](https://github.com/orcher-io/sdk-core/issues/54)) ([3c9a34a](https://github.com/orcher-io/sdk-core/commit/3c9a34a299e8af0bd6c880dc57b03a2df7e5ef41))
* say what differed in a determinism violation ([#58](https://github.com/orcher-io/sdk-core/issues/58)) ([70bda76](https://github.com/orcher-io/sdk-core/commit/70bda76fe11d1ed454beb43a05b56764005c4f8a))


### Bug Fixes

* catch reordered steps and changed timer durations on replay ([#57](https://github.com/orcher-io/sdk-core/issues/57)) ([69460ad](https://github.com/orcher-io/sdk-core/commit/69460ad2821c73d79c1a104c1b6a487aa69ffb66)), closes [#14](https://github.com/orcher-io/sdk-core/issues/14)


### Documentation

* bind step-id schemes and history lineage at first activation ([#55](https://github.com/orcher-io/sdk-core/issues/55)) ([3930b49](https://github.com/orcher-io/sdk-core/commit/3930b49acb3e49ab2d632d3dc1900e7326ed7cc3))
* design the shared worker host ([#62](https://github.com/orcher-io/sdk-core/issues/62)) ([4109739](https://github.com/orcher-io/sdk-core/commit/4109739ffabf329f013c2875beb6cc192aa7ecaa))

## [0.11.0](https://github.com/orcher-io/sdk-core/compare/v0.10.1...v0.11.0) (2026-10-12)


### ⚠ BREAKING CHANGES

* the proto types sdk-core uses and re-exports are now orcher-proto 0.3's. An SDK that also depends on orcher-proto directly must move to 0.3 in the same change, or its build holds two versions of the generated types that do not match. sdk-rust, sdk-py and sdk-ts all compile unchanged with orcher-sdk-core 0.11 and orcher-proto 0.3.

### Features

* build on orcher-proto 0.3 ([#53](https://github.com/orcher-io/sdk-core/issues/53)) ([c348eb6](https://github.com/orcher-io/sdk-core/commit/c348eb66e8988e6577500bf9af26c02eb1f9a160))
* decide in core which work drivers a worker starts ([#49](https://github.com/orcher-io/sdk-core/issues/49)) ([5918217](https://github.com/orcher-io/sdk-core/commit/5918217e92bb6094c51a8283c50ae098156ef857))
* record workflow activations to replay runs without an engine ([#52](https://github.com/orcher-io/sdk-core/issues/52)) ([8bd5011](https://github.com/orcher-io/sdk-core/commit/8bd50111f947101c1b0004b5db2c55c09340a086))
* say how a workflow ended when it returns no result ([#47](https://github.com/orcher-io/sdk-core/issues/47)) ([7725a68](https://github.com/orcher-io/sdk-core/commit/7725a68259d5899d04afc1c86afdbb3cbeae4cd7)), closes [#16](https://github.com/orcher-io/sdk-core/issues/16)


### Bug Fixes

* report a proxy's error page as the engine being unavailable ([#48](https://github.com/orcher-io/sdk-core/issues/48)) ([41043ed](https://github.com/orcher-io/sdk-core/commit/41043edf8261ee01121c82342bbb20c324010e1c)), closes [#6](https://github.com/orcher-io/sdk-core/issues/6)


### Documentation

* design the v1 kernel protocol ([#51](https://github.com/orcher-io/sdk-core/issues/51)) ([7cf5aee](https://github.com/orcher-io/sdk-core/commit/7cf5aee43ef76ad62eaf149985ac0d56d458117b))

## [0.10.1](https://github.com/orcher-io/sdk-core/compare/v0.10.0...v0.10.1) (2026-10-11)


### Bug Fixes

* never print an API key or mTLS private key in Debug output ([#45](https://github.com/orcher-io/sdk-core/issues/45)) ([b9657f2](https://github.com/orcher-io/sdk-core/commit/b9657f2cf444664c453b36c14e5d196c11be1ba1))
* return every float in a payload as the same number ([#46](https://github.com/orcher-io/sdk-core/issues/46)) ([8384be2](https://github.com/orcher-io/sdk-core/commit/8384be23da07a9d7187e6977ccec53eba06871dc)), closes [#4](https://github.com/orcher-io/sdk-core/issues/4)


### Changes

* give worker capacity one home in worker/governor ([#23](https://github.com/orcher-io/sdk-core/issues/23)) ([d4966ab](https://github.com/orcher-io/sdk-core/commit/d4966ab1417b13e2785a16549f63fae8483e3678))
* organise sources by role (state, protocol, worker drivers, transport, payload) ([#22](https://github.com/orcher-io/sdk-core/issues/22)) ([a1d7f62](https://github.com/orcher-io/sdk-core/commit/a1d7f62120559df1fc46a80b2e3bfb41e6663a8b))

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
