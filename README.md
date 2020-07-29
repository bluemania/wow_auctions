# WoW Auction engine

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This project helps automate some aspects of trading on the World of Warcraft (WoW) auction house.

Related article here: https://www.nickjenkins.com.au/articles/personal/2020/07/07/programming-and-analytics-in-games

The program is currently under development and is not currently designed for third party use.

### Requirements

You will need Python 3.7 and World of Warcraft: Classic installed locally on your machine.

This project uses [poetry](https://python-poetry.org/) to manage dependencies and versioning.

```bash
poetry install
```

You will also need the following WoW Classic Addons installed to interface with the program:

* ArkInventory
* Auctioneer
* Beancounter (comes with Auctioneer)


### Running the script

After the above setup, to run the script enter the following on command line.

```bash
poetry shell
poetry run python run.py -a
```

There are many command line options; -a will run primary analysis (except for sell policies). Please refer to the scripts for further information.

### License
All assets and code are under the MIT LICENSE and in the public domain unless specified otherwise.

#### TODO

* Create additional selling profile for min-bid max-buy high-volume. May require splitting the function more carefully
* More visibility on current inventory
