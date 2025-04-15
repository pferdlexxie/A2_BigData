#!/usr/bin/env python3

import sys
import traceback
from collections import defaultdict
from cassandra.cluster import Cluster

current_key = None
freq_sum = 0
doc_lengths = defaultdict(int)
inverted = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        term, doc_id, freq = line.split('\t')
        freq = int(freq)
    except Exception:
        continue

    key = (term, doc_id)
    if current_key and key != current_key:
        inverted.append((*current_key, freq_sum))
        doc_lengths[current_key[1]] += freq_sum
        freq_sum = freq
    else:
        freq_sum += freq
    current_key = key

if current_key:
    inverted.append((*current_key, freq_sum))
    doc_lengths[current_key[1]] += freq_sum

try:
    cluster = Cluster(['172.18.0.4'], connect_timeout=10)
    session = cluster.connect('wiki_index')
except Exception:
    print("Fail cassandra", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

for term, doc_id, freq in inverted:
    try:
        session.execute(
            "INSERT INTO inverted_index (term, doc_id, freq) VALUES (%s, %s, %s)",
            (term, doc_id, freq)
        )
    except Exception:
        continue

for doc_id, length in doc_lengths.items():
    try:
        session.execute(
            "INSERT INTO doc_stats (doc_id, length) VALUES (%s, %s)",
            (doc_id, length)
        )
    except Exception:
        continue


