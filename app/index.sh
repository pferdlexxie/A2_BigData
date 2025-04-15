#!/bin/bash

INPUT_PATH="/data_sample"
OUTPUT_PATH="/tmp/index-output"

echo "Sample ./data/"

hdfs dfs -rm -r -f "$INPUT_PATH" > /dev/null 2>&1
hdfs dfs -mkdir -p "$INPUT_PATH"

ls data | head -n 1 | while read file; do
  hdfs dfs -put -f "data/$file" "$INPUT_PATH/" > /dev/null 2>&1
done

echo "Copy: $INPUT_PATH"

echo "Mapper"

hdfs dfs -rm -r -f "$OUTPUT_PATH" > /dev/null 2>&1
hdfs dfs -rm -r -skipTrash "$OUTPUT_PATH" > /dev/null 2>&1

while hdfs dfs -test -d "$OUTPUT_PATH"; do
  echo "Hold $OUTPUT_PATH"
  sleep 1
done

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
  -D mapreduce.job.reduces=0 \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH" \
  -mapper mapper1.py \
  -file mapper1.py

echo "Mapper end"

echo "Reducer"

MAPPER_OUTPUT_PATH="$OUTPUT_PATH/part-00000"
if hdfs dfs -test -e "$MAPPER_OUTPUT_PATH"; then
  hdfs dfs -cat "$MAPPER_OUTPUT_PATH" | sort | python3 reducer1.py
  echo "Reducer end"
else
  echo "No output"
  exit 1
fi
