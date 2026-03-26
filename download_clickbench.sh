#!/bin/bash
set -e

echo "downloading..."
curl -L -o hits.csv.gz "https://datasets.clickhouse.com/hits_compatible/hits.csv.gz"

echo "decompressing..."
gunzip hits.csv.gz

echo "done: hits.csv"
