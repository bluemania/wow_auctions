# Pricer for WoW Auctions

[![Tests](https://github.com/bluemania/wow_auctions/workflows/Tests/badge.svg)](https://github.com/bluemania/wow_auctions/actions?workflow=Tests)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Codecov](https://codecov.io/gh/bluemania/wow_auctions/branch/master/graph/badge.svg)](https://codecov.io/gh/bluemania/wow_auctions)
[![PyPI](https://img.shields.io/pypi/v/pricer.svg)](https://pypi.org/project/pricer/)
[![Documentation Status](https://readthedocs.org/projects/pricer/badge/?version=latest)](https://pricer.readthedocs.io/en/latest/?badge=latest)

## About the project

This project helps users automate the decision making required to make profit on the World of Warcraft classic auction house.
It does so by reading addon data, using historical pricing information from [booty bay](bootybaygazette.com), and user specified preference, to calculate policies.
These policies take all available information to produce optimal actions for items, i.e. buying volume, selling low, selling high and crafting.
The policies overwrite the addon data, so that upon re-entering the game, the interface is ready to enact the optimal actions.

This is primarily a hobby project.
My aim is to learn about good software development practices, and apply my data science skills to an interesting problem.
And the extra gold makes raiding even more fun!

I did some [twitch streaming](https://www.twitch.tv/bluemania2) for a while, where I discussed the project goals and showed how things worked in more detail.

## Built with

Major frameworks include:

* Python 3+
* Chromedriver and selenium - for booty bay data
* Nox - for CI/CD process
* Poetry - For dependencies, virtual environment, versioning and packaging

Requirements for World of Warcraft classic include:

* An active World of Warcraft account
* An active Booty Bay Gazette subscription
* ArkInventory addon - inventory tracking
* Auctioneer addon - scanning auctions and enacting buy and sell policies
* Beancounter addon - tracking player auction activity (comes with auctioneer)
* Trade Skill Master - enacting crafting policies, also recommended to speed up moving items to bank and buying materials from vendors

## Getting started

The following are instructions on how to set up the project locally.

The latest stable code can be found on [pypi](https://pypi.org/project/pricer/).

Download locally using:

```bash
pip install pricer
```

For development, clone the repository.

This project uses [poetry](https://python-poetry.org/) to manage dependencies, virtual environments, versioning and packaging.

```bash
poetry install
```

# Usage

## User configuration

It is recommended to edit the `config/user_items.yaml` file, as this is the primary mechanism that users can select items of interest for buying, selling and crafting.
Refer to the file to interpret the structure, ensure that items are named correctly.

The `config/user_settings.yaml` file should also be edited with information about paths to installations (WoW and Chromedriver), and active accounts.
Account names can be referenced from your WoW directory.
Booty Bay information should be specified to be specific to your server.
Specify at least one of your characters as an `ahm` (auction house main), with others as `mule`.

You can optionally create a `SECRETS.yaml` file in the root directory with the following format.
This is useful to help automate the booty bay data feed, but not required.
We highly recommend using the Blizzard authenticator (stay safe!).

```yaml
account: youraccount
password: yourpassword
```

## Running the program

Run the program using the following command from command line:

```bash
pricer
```

Additional flags can, and should be entered:

* `-v` or `-vv` is useful for debugging purposes
* `-b` is used to seek a refresh of Booty Bay data; it is recommended to seek an update at least once every day or so.
* `-h` for help on additional flags and functionality that may be available

## Tests and tooling

This project seeks to use modern code quality and CI/CD tooling including

* Dependency management, virtual env and packaging (poetry)
* Linting (black, flake8, darglint)
* Type checking (mypy)
* Testing (pytest, pandera, codecov)
* Docs (sphinx, autodoc)
* Task automation CI/CD (nox, pre-commit, github actions, pr-labeler, release-drafter)
* Publishing (pypi, readthedocs)

# Contributing

This project is pre-release and under development. 

Users are welcome to try the program, fork, or [contribute](CONTRIBUTING.md), however [support](SUPPORT.md) is not guarenteed.

Follow this link for instructions on managing [releases](RELEASE.md).

# License

All assets and code are under the MIT LICENSE and in the public domain unless specified otherwise.
See the [license](LICENSE.md) for more info.

# Contact

Feel free to reach out in-game; you'll see me on Grobbulus on Amazona. 

You can leave an open issue seeking to connect and I'll get back to you.

I also occassionally stream project development on [twitch](https://www.twitch.tv/bluemania2).
