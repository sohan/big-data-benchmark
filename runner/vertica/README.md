## Vertica Benchmark

### Prepare benchmark
#### Vertica dependencies
This benchmark has been tested on Vertica 7.0 Community Edition, but will likely work on 6.x.x and 7.x.x as well.

If you do not have a vertica cluster already, you can deploy one on AWS using these scripts: https://github.com/sohan/aws-vertica

When importing data, we use a User Defined Load function for `curl`, which is included in `/opt/vertica/sdk/examples` on your vertica machines.
There's a `fabric` command `install_curl_udl` in the `aws-vertica` repo above that automates this on an EC2 instance.

On each vertica node, install curl and gcc as follows:

`yum install -y gcc-c++`

`yum install -y curl`

`yum install -y libcurl-devel`

Then, create a library and UDL for curl:

`cd /opt/vertica/sdk/examples && make`

`vsql> CREATE LIBRARY curllib as '/opt/vertica/sdk/examples/build/cURLLib.so'

`CREATE SOURCE curl AS LANGUAGE 'C++' NAME 'CurlSourceFactory' LIBRARY curllib`

#### Python dependencies
Install python dependencies using pip and optionally virtualenv (recommended):

`pip install vertica/requirements.txt`

#### Load data

Example usage for loading data from the top level directory:

```
./prepare-benchmark.sh \
  --aws-key-id="`echo $AWS_ACCESS_KEY_ID`" \
  --aws-key="`echo $AWS_SECRET_ACCESS_KEY`" \
  --vertica-host="$VERTICA_HOST" \
  --vertica-identity-file="$IDENTITY_FILE" \
  --vertica-username="dbadmin" \
  --vertica-password="`echo $VERTICA_PW`" \
  --vertica-port="5433" \
  --vertica-database="dw" \
  --scale-factor=0 \
  --vertica
```

### Run benchmark

In the top level directory, Copy `run_scripts/run_vertica.sample.sh` to `run_scripts/run_vertica.sh`. Then run `./run_scripts/run_vertica.sh`
