# Contributing

This is primarily a personal project aimed at learning and education.

That being said, I am open to contributors and users!

While we are pre-major release, the expectation should be that everything will break frequently!

Best place to start is with the issues, either raising questions or contributing to discussion.

# Branch conventions

This project uses Git Flow, with the exception that releases are cut directly from master.

- Master branch is protected for releases as discussed in [releases.md](RELEASE.md).
- Dev branch is where work is merged into until ready for release.
- Working branches should follow the naming conventions mentioned below
  - feature/ - adding functionality to project
  - bugfix/ - addressing issues with current process
  - docs/ - for documentation improvements
  - infra/ - for CI/CD and tooling
  - hotfix/ - only for fixing broken releases
  - experiment/ - for testing and experiments, not to be merged (formalize as feature)

Using these branch names helps [pr-labeler](.github/pr-labeler.yml) to automatically label pull requests, which in turn helps [release-drafter](.github/release-drafter.md) create nice release notes related to the pull request information.
