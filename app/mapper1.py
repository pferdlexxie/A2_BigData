#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 3:
        continue

    doc_id, title, text = parts

    content = f"{title} {text}"

    # Извлекаем слова
    words = re.findall(r'\w+', content.lower())

    for word in words:
        print(f"{word}\t{doc_id}\t1")
