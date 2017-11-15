 cat ../Data/tmdb_5000_credits.csv | python ./cast_process_mapper.py | sort -k1 -n | python ./cast_process_reducer.py
