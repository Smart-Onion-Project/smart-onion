#!/usr/bin/python3.5

import sys
import postgresql

with postgresql.open('pq://smart_onion_metric_collector:vindeta11@localhost:5432/smart_onion_metric_collector') as tokenizer_db_conn:
    query = tokenizer_db_conn.prepare("select metric_param_key from tokenizer where metric_param_token = '" + sys.argv[1] + "'")
    print(query())