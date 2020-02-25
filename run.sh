#!/usr/bin/env bash

set -e

hdfs dfs -mkdir -p project/raw_corpus
hdfs dfs -mkdir -p project/corpus

hdfs dfs -copyFromLocal word/word.txt project/word.txt
hdfs dfs -copyFromLocal corpus/geo.txt project/raw_corpus/geo.txt
hdfs dfs -copyFromLocal corpus/geo_processed.txt project/corpus/geo_processed.txt

java -cp target/mp_project-1.0-SNAPSHOT.jar mpproject.ACAM config.json
hadoop jar target/mp_project-1.0-SNAPSHOT.jar mpproject.Main config.json