import re
from .prepare import get_ssh_client, vsql
import run_query

VERTICA_QUERY_MAP = {
  '1a': run_query.QUERY_1a_SQL,
  '1b': run_query.QUERY_1b_SQL,
  '1c': run_query.QUERY_1c_SQL,
  '2a': run_query.QUERY_2a_SQL,
  '2b': run_query.QUERY_2b_SQL,
  '2c': run_query.QUERY_2c_SQL,
  '3a': run_query.QUERY_3a_SQL,
  '3b': run_query.QUERY_3b_SQL,
  '3c': run_query.QUERY_3c_SQL,
}

def _get_time_from_result_str(result_str):
  match = re.search('All rows formatted: (\d+\.\d+) ms', result_str)
  if match:
    return float(match.group(1))

def run_vertica_benchmark(opts):
  ssh_client = get_ssh_client(opts) 
  _vsql = lambda sql: vsql(ssh_client, opts, sql)

  sql = VERTICA_QUERY_MAP[opts.query_num]

  results = []
  for i in xrange(opts.num_trials):
    print "Query %s : Trial %i" % (opts.query_num, i+1)
    tmp_table_name = 'TMP_TABLE_{0}'.format(opts.query_num)

    _vsql('DROP TABLE IF EXISTS {0}'.format(tmp_table_name))
    # Load the results of the query into a table, to simulate
    # persisting the results to disk, as in the other benchmarks
    persist_results_sql = '''
      SELECT * INTO TABLE {tmp_table_name} FROM (
          {original_sql}
      ) as original_results_tmp
    '''.format(tmp_table_name=tmp_table_name,
               original_sql=sql)
    stdout, stderr = _vsql(r"\timing\\\\{sql}".format(sql=persist_results_sql))

    timing_str = stdout[-1].strip()
    t_ms = _get_time_from_result_str(timing_str)
    results.append(t_ms/1000.0)

  return results, []

def main():
  run_query.main()

if __name__ == "__main__":
  main()
