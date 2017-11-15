hdfs dfs -rm -r assignment/hw_9/profiling

hadoop \
    jar /usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.6.0-mr1-cdh5.12.0.jar \
    -mapper "python $PWD/Profiling_mapper.py" \
    -reducer "python $PWD/Profiling_reducer.py" \
    -input "assignment/hw_9/tmdb_5000_movies.csv" \
    -output "assignment/hw_9/profiling"

hdfs dfs -cat assignment/hw_9/profiling/part-00000
