hdfs dfs -rm -r assignment/hw_9/ETL

hadoop \
    jar /usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.6.0-mr1-cdh5.12.0.jar \
    -mapper "python $PWD/ETL_mapper.py" \
    -reducer "python $PWD/ETL_reducer.py" \
    -input "assignment/hw_9/tmdb_5000_movies.csv" \
    -output "assignment/hw_9/ETL"

hdfs dfs -cat assignment/hw_9/ETL/part-00000
