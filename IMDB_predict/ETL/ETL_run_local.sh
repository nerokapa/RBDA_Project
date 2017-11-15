cat ../Data/tmdb_5000_movies.csv | python ETL_mapper.py | sort -k1 | python ETL_reducer.py
