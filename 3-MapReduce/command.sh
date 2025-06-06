#!/bin/bash

OUTPUT_DIR="/user/hadoop/outpu/job3_result"
INPUT_DIR="/user/hadoop/input/us_used_cars_cleaned_job3"

# Elimina output precedente se esiste
hdfs dfs -rm -r $OUTPUT_DIR

hadoop jar /Users/bishoyzakhary/Downloads/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -files /Users/bishoyzakhary/Desktop/bigData/JOB2/map.py,/Users/bishoyzakhary/Desktop/bigData/JOB3/reduce.py \
    -input $INPUT_DIR \
    -output $OUTPUT_DIR \
    -mapper map.py \
    -reducer reduce.py