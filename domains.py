"""
Extract top domains from BigQuery result.
"""
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(os.environ['HOME']) / 'data' / 'tinysearch'
ALL_DOMAINS_PATH = DATA_DIR / 'hn-top-domains.csv'
TOP_DOMAINS_PATH = DATA_DIR / 'hn-top-domains-filtered.py'

MIN_COUNT = 10


def get_top_domains():
    data = pd.read_csv(ALL_DOMAINS_PATH, index_col='domain')

    frequent = data[data['total'] >= MIN_COUNT]
    scores = frequent['mean_score'] * np.log(frequent['total']) ** 2
    median_score = np.median(scores)
    probabilities = scores / (scores + median_score)

    probabilities.sort_values(ascending=False, inplace=True)
    with open(TOP_DOMAINS_PATH, 'w') as output_file:
        output_file.write("DOMAINS = " + str(probabilities.to_dict()) + '\n\n')
        # json.dump(probabilities.to_dict(), output_file, indent=2)

        # for row in probabilities.iterrows():
        #     output_file.write(json.dumps(row.to_dict()) + '\n')


if __name__ == '__main__':
    get_top_domains()
