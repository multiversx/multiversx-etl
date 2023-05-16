
## Quickstart

```
python3 -m multiversxetl get_block_range_for_date --date=2023-05-01
```

```
python3 -m multiversxetl get_block_range_for_timestamps --start-timestamp=1682899200 --end-timestamp=1683676800
```

```
python3 -m multiversxetl export_blocks_and_transactions --start-block=14443500 --end-block=14443700 \
 --batch-size=10 --max-workers=4 \
 --blocks-output=blocks.json --transactions-output=transactions.json
```

```
python3 -m multiversxetl export_blocks_and_transactions --start-block=14443500 --end-block=14443700 \
 --batch-size=10 --max-workers=4 \
 --blocks-output=blocks.csv --transactions-output=transactions.csv
```

## Development

### Setup virtual environment

Create a virtual environment and install the dependencies:

```
python3 -m venv ./venv
source ./venv/bin/activate

pip install -r ./requirements.txt --upgrade
pip install -r ./requirements-dev.txt --upgrade
```

### Run tests

```
pytest
```
