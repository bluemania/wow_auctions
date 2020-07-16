# WoW Auction engine

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This project helps automate some aspects of trading on the World of Warcraft (WoW) auction house.

Related article here: https://www.nickjenkins.com.au/articles/personal/2020/07/07/programming-and-analytics-in-games

The program is currently under development and is not currently designed for third party use.

### Environment setup

Project uses Python 3.7, which is required to run the code. The following is used to add library dependencies.

```bash
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
```

### Running the script

To run the script, run the following from command line.

```bash
python run.py -a
```

There are many command line options; -a will run primary analysis (except for sell policies). Please refer to the scripts for further information.

#### TODO

* Create additional selling profile for min-bid max-buy high-volume. May require splitting the function more carefully
* More visibility on current inventory
