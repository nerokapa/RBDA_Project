echo "processing cast data using map reduce...."
# cat ../Data/tmdb_5000_credits.csv | python ./cast_process_mapper.py | sort -k1 -n | python ./cast_process_reducer.py > ../Data/processed_cast_data.dat
echo "starting hbase service..."
start-hbase.sh
echo "starting hbase thrift service..."
hbase-daemon.sh start thrift
echo "resetting database..."
python ./reset_database.py
