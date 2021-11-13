"""
Extract top domains from BigQuery result.
"""
import json
import os
import sys
from pathlib import Path

import pandas as pd

DATA_DIR = Path(os.environ['HOME']) / 'data' / 'tinysearch'
ALL_DOMAINS_PATH = DATA_DIR / 'hn-top-domains.csv'
TOP_DOMAINS_PATH = DATA_DIR / 'hn-top-domains-filtered.json'

MIN_COUNT = 10


def get_top_domains():
    data = pd.read_csv(ALL_DOMAINS_PATH, index_col='domain')

    frequent = data[data['total'] >= MIN_COUNT]
    scores = frequent['mean_score'].to_dict()

    with open(TOP_DOMAINS_PATH, 'w') as output_file:
        json.dump(scores, output_file)


if __name__ == '__main__':
    get_top_domains()
