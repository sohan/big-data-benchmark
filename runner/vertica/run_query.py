import prepare_benchmark_vertica
import run_query
import sys
from optparse import OptionParser
import os
import time
import datetime
import re
import multiprocessing
from StringIO import StringIO
import vertica_python


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

def parse_args():
  parser = OptionParser(usage="run_query.py [options]")

  parser.add_option("--vertica-host",
      help="Hostname of Vertica ODBC endpoint")

  parser.add_option("--vertica-identity-file",
      help="SSH private key file to use for logging into Vertica node")

  parser.add_option("--vertica-username",
      help="Username for Vertica ODBC connection")
  parser.add_option("--vertica-port",
      help="Port for Vertica ODBC connection")
  parser.add_option("--vertica-password",
      help="Password for Vertica ODBC connection")
  parser.add_option("--vertica-database",
      help="Database to use in Vertica")

  parser.add_option("-d", "--aws-key-id",
      help="Access key ID for AWS")
  parser.add_option("-k", "--aws-key",
      help="Access key for AWS")

  parser.add_option("--num-trials", type="int", default=10,
      help="Number of trials to run for this query")
  parser.add_option("--prefix", type="string", default="",
      help="Prefix result files with this string")


  parser.add_option("-q", "--query-num", default="1a",
                    help="Which query to run in benchmark: " \
                    "%s" % ", ".join(VERTICA_QUERY_MAP.keys()))

  (opts, args) = parser.parse_args()

  if (opts.vertica_username is None or
      opts.vertica_password is None or
      opts.vertica_host is None or
      opts.vertica_database is None or
      opts.aws_key_id is None or
      opts.aws_key is None):
    print >> sys.stderr, \
        "Vertica requires host, username, password, db, and AWS credentials"
    sys.exit(1)

  return opts

def _get_time_from_result_str(result_str):
  match = re.search('All rows formatted: (\d+\.\d+) ms', result_str)
  if match:
    return float(match.group(1))

def run_vertica_benchmark(opts):
  ssh_client = prepare_benchmark_vertica.get_ssh_client(opts) 

  _vsql = lambda sql: prepare_benchmark_vertica.vsql(ssh_client, opts, sql)

  sql = VERTICA_QUERY_MAP[opts.query_num]

  results = []
  for i in xrange(opts.num_trials):
    print "Query %s : Trial %i" % (opts.query_num, i+1)
    stdout, stderr = _vsql(r"\timing\\\\" + sql)
    #TODO: write results to file as part of test
    # use the \o or -o flag:
    # https://my.vertica.com/docs/4.1/HTML/Master/15255.htm
    timing_str = stdout[-1].strip()
    t_ms = _get_time_from_result_str(timing_str)
    results.append(t_ms/1000.0)

  return results, []

def main():
  global opts
  opts = parse_args()
  print "Query %s:" % opts.query_num
  fname = opts.prefix + 'vertica'

  def prettylist(lst):
    return ",".join([str(k) for k in lst]) 

  results, contents = run_vertica_benchmark(opts)

  output = StringIO()
  out_filename = 'results/%s_%s/%s' % (fname, opts.query_num, datetime.datetime.now())
  dir_name = os.path.dirname(out_filename)
  if not os.path.exists(dir_name):
    os.makedirs(dir_name)
  with open(out_filename, 'w') as outfile:
    try:
      print >> output, "=================================="
      print >> output, "Results: %s" % prettylist(results)
      print >> output, "Percentiles: %s" % run_query.get_percentiles(results)
      print >> output, "Best: %s"  % min(results)
      print output.getvalue()
      print >> outfile, output.getvalue()
    except:
      print output.getvalue()
      print >> outfile, output.getvalue()
    output.close()

if __name__ == "__main__":
  main()
