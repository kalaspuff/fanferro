# 0011: Real AWS is the reference for the in-memory broker and the error mapping

Status: Accepted (amends [0005](0005-in-memory-backend.md) and [0006](0006-ministack-for-end-to-end-tests.md))

## Context

The in-memory broker was modelled on the AWS documentation and checked against MiniStack, which
the end-to-end tests use. Probing real AWS, MiniStack, LocalStack and the broker side by side
(`scripts/probe_backends.py`, `scripts/probe_aws_cli.sh`) showed that the emulators differ from
AWS in many places where fanferro depends on the behaviour (validation, receipt handles, limits,
SNS delivery, error codes), and that the broker followed MiniStack, or neither, in some of them.

## Decision

* Real AWS is the reference. The in-memory broker follows what the probes recorded on AWS
  ([docs/parity](../parity/README.md)): the same outcome, error code, HTTP status and text, where
  fanferro's users can see them.
* An emulator is never the reference. A behaviour an emulator does not reproduce is tested on the
  in-memory broker only, with a comment naming the finding in `docs/parity`.
* A difference fanferro cannot remove (credentials, accounts, an emulator's bug) is recorded in
  `docs/parity` with its reason.
* When the probes change or AWS behaves differently, a new run is recorded under `docs/parity/`
  (dated) and the findings are updated.

## Consequences

* Applications' tests on `fanferro.testing.memory_client()` fail where production on AWS would,
  for example when they create a queue again right after deleting it.
* Some end-to-end tests cannot run on MiniStack; the in-memory broker carries them.
