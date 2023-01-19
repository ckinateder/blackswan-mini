# blackswan-mini

Minified version of the blackswan-mvp.

## Core focuses

Vital items to product success.

### Product

- advanced, transparent logging for trades and operations
- functional paradigm for decision making - same decision function used for backtesting and trading
- use alpaca for bars and trade requests
- use finnnhub to backfill bars
- built to be lightweight

### Code

- python 3.11
- built on pandas
- only essential built-in functions are implemented

## Setup

```bash
docker build . -t blackswan-mini  # build image
docker run -it --rm --network host --env-file .env -v $(pwd):/bsm -w /bsm/ blackswan-mini bash -c "python src/stream.py"  # run stream
```


## Progress Tracking

### Todo
- add tracking for trades 
- switch resample for finta resample 
- add feature engineering
- proper shutdown stuff
