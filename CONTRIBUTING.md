# Contributing to avro4k

Thanks for your interest in avro4k! Contributions are welcome: bug reports, feature requests, fixes, performance improvements and documentation.

avro4k is maintained by a small number of people on their free time. Reviewing a pull request takes real effort, so this guide explains what we expect **before** you open one. Pull requests that don't follow it may be closed without review.

## TL;DR

1. **Prefer opening (or finding) an issue first** to agree on the problem and the approach before writing code.
2. **One pull request = one concern**, linked to its issue when there is one.
3. **Disclose any AI assistance** in the pull request description.
4. **You must understand and own your change**: explain why you chose this approach over simpler alternatives, and answer review comments yourself, in your own words.
5. **Keep it simple**: the right balance between performance, readability and maintenance, not the most clever code.
6. **Run the whole test suite locally** with `./gradlew actionsBeforeCommit check` before pushing, and make sure it passes.

## Issues first

We recommend opening an issue (or commenting on an existing one) describing the problem and the solution you have in mind before writing code, and waiting for a maintainer to confirm that:

- the problem is real and in scope for avro4k,
- the proposed approach fits the library design (public API, binary compatibility, Avro spec compliance, performance).

This avoids wasted effort on both sides, especially for new features and public API changes.

Opening a pull request directly is fine too: good-faith contributions are welcome. In that case, the pull request description must explain the problem it solves, as an issue would. Be aware that a pull request without prior discussion is more likely to be asked for a different approach, or to be closed if it's out of scope.

Typo fixes, broken links and small documentation clarifications never need an issue.

## Keep it simple

avro4k is maintained on free time, so every line of code has a maintenance cost. We look for the right compromise between performance, readability and maintainability, not the most optimized or clever code.

- **Prefer the straightforward solution**, using the standard library, kotlinx-serialization or Apache Avro, over custom helpers re-implementing what they already do.
- **Optimize only with evidence.** A micro-optimization is welcome when a [benchmark](benchmark/README.md) shows a real gain on a hot path. Otherwise, it's added complexity for nothing.
- **Don't add abstractions "for later".** Solve the problem at hand.

For example, a hand-written helper computing the UTF-8 byte length of a string, just to avoid `string.encodeToByteArray().size` and its short-lived array, is extra code to maintain and test (surrogate pairs, invalid characters, …) for a gain that the JVM mostly absorbs. Unless a benchmark proves the opposite, the one-liner wins.

In the pull request description, **explain the choices you made** and the simpler alternatives you considered. If you can't explain why the extra complexity is worth it, it probably isn't.

## AI-assisted contributions

We don't ban AI tools: many of us use them. But we've seen a growing number of pull requests opened by autonomous agents or generated with little human involvement, and they cost maintainers a lot of time for little value. So the rules are:

- **Disclosure is mandatory.** If an AI tool (Copilot, Claude, ChatGPT, Cursor, an autonomous agent, …) wrote a meaningful part of the code, tests, docs or the pull request description, say so in the pull request template, with the tool and what it was used for. Simple autocompletion doesn't need to be disclosed.
- **A human is accountable.** The pull request must be opened by a human who has read, understood and tested every line of it. "The AI did it" is not an answer to a review comment.
- **Answer reviews yourself.** Review discussions are between humans. Don't paste AI-generated replies to review comments; we'd rather read short, imperfect answers in your own words.
- **No autonomous agents.** Pull requests, issues or comments opened by bots or agents acting without a human in the loop are not accepted. Accounts behaving like this may be blocked.
- **Keep it focused.** No drive-by refactorings, mass renames, reformatting, or "improvements" unrelated to the problem being solved.

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
- **Run the tests yourself, all of them.** `check` runs the full test suite of every module, not only the ones you touched: a change in `core` can break the generator, the gradle plugin or the Kafka serializer. Don't rely on CI to find failures for you, and don't open a pull request with failing tests. Never disable, skip or weaken an existing test to make the build green: if a test needs to change, explain why in the pull request.
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
