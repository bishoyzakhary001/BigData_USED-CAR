hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /user/tuo_utente/input/us_used_cars_cleaned_job1.csv \
    -output /user/tuo_utente/output/job1_result \
    -mapper /path/assoluto/map.py \
    -reducer /path/assoluto/reduce.py \
    -file /path/assoluto/map.py \
    -file /path/assoluto/reduce.py





   