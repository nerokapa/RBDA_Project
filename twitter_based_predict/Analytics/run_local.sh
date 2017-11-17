cat test_in.txt | python analytic_mapper.py | sort -k1 -n | python analytic_reducer.py
# python streaming.py | python analytic_mapper.py | sort -k1 -n | python analytic_reducer.py
