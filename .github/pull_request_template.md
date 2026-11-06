<!--
Thanks for contributing! Please read CONTRIBUTING.md first.
Pull requests that don't fill in this template may be closed without review.
-->

## Linked issue

<!-- Required (except typo/doc fixes). Ideally discussed with a maintainer first, but you can open the issue together with this pull request. -->
Closes #

## What and why

<!-- Explain the change in your own words: the problem, your approach, the simpler alternatives you considered and why you didn't pick them. -->

## How it was tested

<!-- New/updated tests, manual checks, benchmark results for performance-sensitive changes. -->

## AI assistance

<!-- Mandatory. Pick one. Simple autocompletion doesn't count. -->
- [ ] No AI tool was used for this contribution.
- [ ] AI tools were used. Tool(s) and what for: <!-- e.g. "Claude, to draft the tests" -->

## Checklist

- [ ] I am a human, I have read and understood every line of this change, and I will answer review comments myself.
- [ ] The change is limited to the problem it solves (no unrelated refactoring or reformatting).
- [ ] I chose the simplest solution that works; any extra complexity (e.g. optimizations) is justified above, with benchmarks if performance-related.
- [ ] I ran the full test suite locally with `./gradlew actionsBeforeCommit check` and it passes.
- [ ] I added tests covering this change, and I didn't disable, skip or weaken any existing test.
- [ ] Public API changes (`*.api` files) are committed and intended.
- [ ] Documentation is updated if user-facing behavior changed.
