cat ../Data/tmdb_5000_movies.csv | python Profiling_mapper.py | sort -k1 | python Profiling_reducer.py
