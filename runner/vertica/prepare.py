import paramiko
from boto.s3.connection import S3Connection
import workerpool
import prepare_benchmark

SCALE_FACTOR_MAP = prepare_benchmark.SCALE_FACTOR_MAP

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

def vsql(ssh_client, opts, sql, out_file=None):
  '''
  Execute some SQL using vsql on the server.
  Be sure to escape any " (double quotes) you use.
  '''
  cleaned_sql = sql.replace('\n', ' ')

  return ssh_vertica(ssh_client, '''
    echo "{sql}" | /opt/vertica/bin/vsql -U {vertica_username} -w {vertica_password} -h {vertica_host} -d {vertica_database} -p {vertica_port}
  '''.format(sql=cleaned_sql, **opts.__dict__))

def prepare_vertica_dataset(opts):
  ssh_client = get_ssh_client(opts) 

  _ssh_vertica = lambda c: ssh_vertica(ssh_client, c)
  _vsql = lambda sql: vsql(ssh_client, opts, sql)

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
      destinationURL VARCHAR(100),
      visitDate DATE,
      adRevenue FLOAT,
      userAgent VARCHAR(256),
      countryCode VARCHAR(3),
      languageCode VARCHAR(6),
      searchWord VARCHAR(32),
      duration INT 
    )
  ''')
  # if opts.vertica_use_projections:
  #   _vsql('''
  #     CREATE PROJECTION uservists_p1 (
  #       sourceIP, destinationURL, visitDate, adRevenue
  #     )
  #   ''')
  #   pass
  parallel_s3_to_table(bucket, s3_key.format(table_name='uservisits'), 'uservisits')

  # _vsql('DROP TABLE IF EXISTS documents')
  # _vsql('''
  #   CREATE TABLE documents (
  #     line VARCHAR(300)
  #   )
  # ''')
  # parallel_s3_to_table(bucket, s3_key.format(table_name='crawl'), 'documents')

  print "=== FINISHED CREATING BENCHMARK DATA ==="

def main():
  prepare_benchmark.main()

if __name__ == "__main__":
  main()
