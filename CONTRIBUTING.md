# Contributing to avro4k

Thanks for your interest in avro4k! Contributions are welcome: bug reports, feature requests, fixes, performance improvements and documentation.

avro4k is maintained by a small number of people on their free time. Reviewing a pull request takes real effort, so this guide explains what we expect **before** you open one. Pull requests that don't follow it may be closed without review.

## TL;DR

1. **Open (or find) an issue first** and wait for a maintainer to accept it before writing code. Typo fixes and small documentation tweaks are the only exception.
2. **One pull request = one concern**, linked to its issue.
3. **Disclose any AI assistance** in the pull request description.
4. **You must understand and own your change**: you answer review comments yourself, in your own words.
5. Run `./gradlew actionsBeforeCommit check` locally before pushing.

## Issues first

Before opening a pull request, open an issue (or comment on an existing one) describing the problem and the solution you have in mind. Wait until a maintainer confirms that:

- the problem is real and in scope for avro4k,
- the proposed approach fits the library design (public API, binary compatibility, Avro spec compliance, performance).

This avoids wasted effort on both sides. Pull requests changing code without a linked, accepted issue will be closed.

Exceptions that don't need an issue: typo fixes, broken links, and small clarifications in documentation.

## AI-assisted contributions

We don't ban AI tools: many of us use them. But we've seen a growing number of pull requests opened by autonomous agents or generated with little human involvement, and they cost maintainers a lot of time for little value. So the rules are:

- **Disclosure is mandatory.** If an AI tool (Copilot, Claude, ChatGPT, Cursor, an autonomous agent, …) wrote a meaningful part of the code, tests, docs or the pull request description, say so in the pull request template, with the tool and what it was used for. Simple autocompletion doesn't need to be disclosed.
- **A human is accountable.** The pull request must be opened by a human who has read, understood and tested every line of it. "The AI did it" is not an answer to a review comment.
- **Answer reviews yourself.** Review discussions are between humans. Don't paste AI-generated replies to review comments; we'd rather read short, imperfect answers in your own words.
- **No autonomous agents.** Pull requests, issues or comments opened by bots or agents acting without a human in the loop are not accepted. Accounts behaving like this may be blocked.
- **Keep it focused.** No drive-by refactorings, mass renames, reformatting, or "improvements" unrelated to the linked issue.

Undisclosed AI-generated contributions, or authors who can't explain their own change, will be closed without further review.

## Development

### Requirements

- JDK 17+ to run the build (libraries target JVM 11 through Gradle toolchains)
- No local Gradle installation needed, use the wrapper (`./gradlew`).

### Project layout

| Module                        | Description                                              |
|-------------------------------|----------------------------------------------------------|
| `core`                        | The avro4k library itself                                |
| `confluent-kafka-serializer`  | Kafka serializers/deserializers for the schema registry  |
| `kotlin-generator`            | Kotlin code generation from Avro schemas                 |
| `gradle-plugin`               | Gradle plugin wrapping the generator                     |
| `bom`                         | Bill of materials                                        |
| `benchmark`                   | JMH benchmarks                                           |
| `example`                     | Usage examples                                           |

### Before pushing

```shell
./gradlew actionsBeforeCommit check
```

`actionsBeforeCommit` formats the code (Spotless + ktlint) and updates the public API dumps (`apiDump`). `check` runs the tests and verifies the API dumps.

- **Public API changes** are visible in the `*.api` files. Commit them. Any change there is reviewed carefully: breaking changes require a strong justification and a discussion in the issue first.
- **Tests are required** for bug fixes (a test that fails without the fix) and new features.
- **Performance-sensitive changes** to encoding/decoding should come with benchmark results (see [benchmark](benchmark/README.md)).
- **Documentation**: update the README (or the module README) when you change user-facing behavior.

### Commits and pull requests

- Use [Conventional Commits](https://www.conventionalcommits.org/) for the pull request title, scoped by module when relevant, e.g. `feat(core): support kotlin.time.Instant` or `fix(gradle-plugin): ...`. The title is used for the squash-merge commit and the release notes.
- Fill in the pull request template entirely.
- Keep pull requests small. Large changes should be split, after agreeing on a plan in the issue.
- Don't force-push once a review has started, so reviewers can see what changed.

## Reporting bugs

Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md) and include a minimal reproducer: the Kotlin classes, the Avro schema and the avro4k / Kotlin / kotlinx-serialization versions. A reproducer as a failing test is the best bug report.

## Security issues

Please don't open a public issue for security vulnerabilities. Use [GitHub private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability) on this repository instead.

## License

By contributing, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE), the license of this project.
