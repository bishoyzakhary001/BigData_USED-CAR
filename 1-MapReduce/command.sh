#!/bin/zsh

OUTPUT_DIR="/user/hadoop/output/job1_result"
hdfs dfs -rm -r $OUTPUT_DIR

hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
  -files /home/hadoop/map.py,/home/hadoop/reduce.py \
  -input /user/hadoop/input/us_used_cars_cleaned_job1.csv \
  -output /user/hadoop/output/job1_result \
  -mapper map.py \
  -reducer reduce.py