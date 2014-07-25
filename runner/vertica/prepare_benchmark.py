from optparse import OptionParser
import sys

import paramiko
from boto.s3.connection import S3Connection
import workerpool
import ipdb

SCALE_FACTOR_MAP = {
  0: "tiny",
  1: "1node",
  5: "5nodes",
  10: "10nodes"
}

def parse_args():
  parser = OptionParser(usage="prepare_benchmark.py [options]")

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

  parser.add_option("-n", "--scale-factor", type="int", default=5,
      help="Number of database nodes (dataset is scaled accordingly)")

  parser.add_option("-d", "--aws-key-id",
      help="Access key ID for AWS")
  parser.add_option("-k", "--aws-key",
      help="Access key for AWS")

  (opts, args) = parser.parse_args()

  if opts.scale_factor not in SCALE_FACTOR_MAP.keys():
    print >> sys.stderr, "Unsupported cluster size: %s" % opts.scale_factor
    sys.exit(1)

  opts.data_prefix = SCALE_FACTOR_MAP[opts.scale_factor]
  opts.file_format = 'text'

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

def get_ssh_client(opts):
  ssh = paramiko.SSHClient()
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  ssh.connect(opts.vertica_host, username='root', key_filename=opts.vertica_identity_file)
  return ssh

def ssh_vertica(ssh_client, command):
  stdin, stdout, stderr = ssh_client.exec_command('source /root/.bash_profile; %s' % command)

  stdout = list(stdout)
  stderr = list(stderr)
  for i, line in enumerate(stdout):
      line = line.rstrip()
      print "%d: %s" % (i, line)
      if i >= 9:
        break
  return stdout, stderr

def vsql(ssh_client, opts, sql):
  '''
  Execute some SQL using vsql on the server.
  Be sure to escape any " (double quotes) you use.
  '''
  cleaned_sql = sql.replace('\n', ' ')
  # return ssh_vertica(ssh_client, '''
  #   /opt/vertica/bin/vsql -U {vertica_username} -w {vertica_password} -h {vertica_host} -d {vertica_database} -p {vertica_port} -c "{sql}"
  # '''.format(sql=cleaned_sql, **opts.__dict__))

  return ssh_vertica(ssh_client, '''
    echo "{sql}" | /opt/vertica/bin/vsql -U {vertica_username} -w {vertica_password} -h {vertica_host} -d {vertica_database} -p {vertica_port}
  '''.format(sql=cleaned_sql, **opts.__dict__))

def prepare_vertica_dataset(opts):
  ssh_client = get_ssh_client(opts) 

  _ssh_vertica = lambda c: ssh_vertica(ssh_client, c)
  _vsql = lambda sql: vsql(ssh_client, opts, sql)

  def s3_to_table(s3_url, table):
    # use s3cmd get to import into vertica

    # _ssh_vertica('''
    #   s3cmd get {s3_key} - | /opt/vertica/bin/vsql -U {vertica_username} -w {vertica_password} -h {vertica_host} -d {vertica_database} -p {vertica_port} -c "copy {table} from STDIN DELIMITER ','"
    # '''.format(s3_key=s3_key, table=table, **opts.__dict__))

    _ssh_vertica("COPY {0} WITH SOURCE curl(url='{1}') DELIMITER ','".format(s3_url, table))

  def parallel_s3_to_table(bucket, s3_key, table, parallelism=10):
    def copy_to_vertica(url):
      ssh_client = get_ssh_client(opts)
      print 'started copying {0}'.format(url)
      vsql(ssh_client, opts, "COPY {0} WITH SOURCE curl(url='{1}') DELIMITER ','".format(table, url))
      print 'finished copying {0}'.format(url)

    keys = bucket.list(s3_key)
    pool = workerpool.WorkerPool(size=parallelism)
    urls = [key.generate_url(expires_in=0, query_auth=False) for key in keys if 'part-' in key.key]

    pool.map(copy_to_vertica, urls)

    pool.shutdown()
    pool.wait()

  s3_conn = S3Connection(opts.aws_key_id, opts.aws_key)
  bucket = s3_conn.get_bucket('big-data-benchmark')
  s3_key = 'pavlo/{file_format}/{data_prefix}/{table_name}'.format(
      file_format=opts.file_format, 
      data_prefix=opts.data_prefix,
      table_name='{table_name}')

  print "=== IMPORTING BENCHMARK DATA FROM S3 ==="

  _vsql('DROP TABLE IF EXISTS rankings')
  _vsql('''
    CREATE TABLE rankings (
      pageURL VARCHAR(300), 
      pageRank INT, 
      avgDuration INT
    )
  ''')
  parallel_s3_to_table(bucket, s3_key.format(table_name='rankings'), 'rankings')

  _vsql('DROP TABLE IF EXISTS uservisits')
  _vsql('''
    CREATE TABLE uservisits (
      sourceIP VARCHAR(116),
      destURL VARCHAR(100),
      visitDate DATE,
      adRevenue FLOAT,
      userAgent VARCHAR(256),
      countryCode VARCHAR(3),
      languageCode VARCHAR(6),
      searchWord VARCHAR(32),
      duration INT 
    )
  ''')
  parallel_s3_to_table(bucket, s3_key.format(table_name='uservisits'), 'uservisits')

  _vsql('DROP TABLE IF EXISTS documents')
  _vsql('''
    CREATE TABLE documents (
      line VARCHAR(300)
    )
  ''')
  parallel_s3_to_table(bucket, s3_key.format(table_name='crawl'), 'documents')

  print "=== FINISHED CREATING BENCHMARK DATA ==="

def main():
  opts = parse_args()
  prepare_vertica_dataset(opts)

if __name__ == "__main__":
  main()
