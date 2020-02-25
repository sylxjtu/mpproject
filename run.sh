#!/usr/bin/env bash

set -e

hdfs dfs -mkdir -p project/raw_corpus
hdfs dfs -mkdir -p project/corpus

hdfs dfs -copyFromLocal -f word/word.txt project/word.txt
hdfs dfs -copyFromLocal -f corpus/geo.txt project/raw_corpus/geo.txt
hdfs dfs -copyFromLocal -f corpus/geo_processed.txt project/corpus/geo_processed.txt

hadoop jar target/mp_project-1.0-SNAPSHOT-jar-with-dependencies.jar mpproject.ACAM config.json
hadoop jar target/mp_project-1.0-SNAPSHOT-jar-with-dependencies.jar mpproject.Main config.json