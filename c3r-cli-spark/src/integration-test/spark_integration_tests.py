import argparse
import unittest
import os as os
from pathlib import Path
import subprocess
import sys
import shutil
import requests
import urllib.request
import urllib.error
from typing import Literal

# Directory at the top of the git repository, from which we can
# easily grab any desired files directly from the `samples` dir
# without needing reoccuring `..` in the path.
repo_dir=Path(os.path.dirname(__file__)).parent.parent.parent.absolute()

# Currently disabled as the newer version of Spark is in preview. Locking to version 3.5.1 for the time being
def latest_spark_version() -> str:
    """
    Use the Maven Central Repository REST API to get the latest version of the spark-core_2.13 package.
    
    Return the version as a string 'MAJOR.MINOR.PATCH'.
    """
    response = requests.get('https://search.maven.org/solrsearch/select'
                            + '?q=g:org.apache.spark+AND+a:spark-core_2.13&core=gav&rows=50&wt=json')
    response = response.json()
    num_found = response['response']['numFound']

    if num_found < 1:
        raise Exception('Unable to determine latest spark version from Maven: ' + response)

    versions =  [entry['v'].split('.') for entry in response['response']['docs']]
    versions.sort()
    return '.'.join(versions[-1])

spark_version="3.5.1"
spark_dir:str = f'spark-{spark_version}-bin-hadoop3-scala2.13'
"""Apache Spark directory."""
spark_tgz:str = f'{spark_dir}.tgz'
"""Apache Spark tgz file to download."""
spark_tgz_url:str = f'https://dlcdn.apache.org/spark/spark-{spark_version}/{spark_tgz}'
"""URL for Spark tgz file."""


def wget_if_missing(file, file_url):
    """If `file` is not present, download `file_url`."""
    if not os.path.isfile(file):
        if not shutil.which('wget'):
            print('wget could not be found in the PATH.')
            print('Could not fetch c3r client.')
            print('Aborting!')
            return
        cmd = f'wget {file_url}'
        print(f'Downloading {file!r} from {file_url} via wget.')
        subprocess.run(cmd, cwd=repo_dir, shell=True)
    else:
        print(f"Using {file!r} found on disk.")


def local_spark_setup() -> None:
    """Download Spark if necessary and start the server."""
    wget_if_missing(Path(repo_dir, spark_tgz), spark_tgz_url)

    if not os.path.isdir(Path(repo_dir, spark_dir)):
        cmd = f'tar -xzvf  {spark_tgz}'
        print(f'Uncompressiong {spark_tgz}.')
        subprocess.run(cmd, cwd=repo_dir, shell=True)
    
    try:
        response = urllib.request.urlopen('http://localhost:8080')
        if not 200 == response.status:
            raise urllib.error.URLError()
        else:
            print(f'Using local Spark server running at http://localhost:8080')
    except urllib.error.URLError:
        print(f'Starting local Spark server at http://localhost:8080 ...')
        cmd = f'./{spark_dir}/sbin/start-all.sh'
        subprocess.run(cmd, cwd=repo_dir, shell=True)


# c3r-cli-spark JAR to test
c3r_spark_jar=None
# collaboration id with all crypto parameters 'Yes'
all_y_id=None
# collaboration id with all crypto parameters 'No'
all_n_id=None

class C3rCli():
    """
    Class that takes C3R CLI arguments and can be used to run a subprocess C3R CLI application
    using the current value of the module variable `c3r_spark_jar`.
    """

    def __init__(self,
        mode : Literal['encrypt', 'decrypt', 'schema'],
        *,
        input : str = None,
        key : str = 'vLO8H9rCFs4FO9b+K9xGERL1IhtgJBhe9OV5EXAMPLE=',
        region : str = 'us-east-1',
        parameters : list[str]):
        """
        Constructs a C3R CLI configuration that can be run using the current `c3r_spark_jar` 
        module variable value.

        Parameters
        ----------
        mode : Which mode to run the C3R CLI in (encrypt, decrypt, or schema)
        input : the input file
        key : value to populate the `C3R_SHARED_SECRET` env var with (use `None` to omit)
        region : value to populate the `AWS_REGION` env var with (use `None` to omit)
        parameters : All other CLI parameters for the application passed as-is in the order given
        """
        self.mode = mode
        self.input = input
        self.key = key
        self.region = region
        self.parameters = parameters

    def run(self) -> bool:
        """
        Run the `C3rCli` program with the arguments provided at object initialization.
        
        Returns whether the exit code was `0`.
        """
        params = " ".join(arg for arg in self.parameters)
        command = (
            f'  {repo_dir}/{spark_dir}/bin/spark-submit \\\n'
            f'  --class com.amazonaws.c3r.spark.cli.Main \\\n'
            f'  --master local[*] \\\n'
            f'  --executor-memory 4G \\\n'
            f'  {repo_dir}/{c3r_spark_jar} \\\n'
            f'  {self.mode} {self.input} \\\n'
            f'  {params}'
    )

        if self.key is not None:
            command = f'C3R_SHARED_SECRET="{self.key}" {command}'
        if self.region is not None:
            command = f'AWS_REGION="us-east-1" {command}'

        print('')
        print(f'Executing `{command}`.')
        process = subprocess.Popen(
            command,
            shell=True,
            cwd=repo_dir, 
            stdout=sys.stdout)
        process.wait()
        return process.returncode == 0


class C3rIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        local_spark_setup()
        
    def test_decrypt_with_wrong_key(self):
        """Test that decrypting with a different key than encrypting fails."""
        enc_cli = C3rCli('encrypt',
            input='samples/csv/data_sample_without_quotes.csv',
            parameters= [
                f'--id={all_y_id}',
                '--schema=samples/schema/config_sample_no_cleartext.json',
                '--output=encrypted_csv_out',
                '--overwrite'
            ]
        )
        self.assertTrue(enc_cli.run())
        dec_cli = C3rCli('decrypt',
            input='encrypted_csv_out',
            parameters= [
                f'--id={all_y_id}',
                '--output=decrypted_csv_out',
                f'--fileFormat=csv',
                '--overwrite'
            ]
        )
        # ensure decryption works when we use the same key
        self.assertTrue(dec_cli.run())
        # check that decryption fails when we swap out the key for a different one
        dec_cli.key = 'abcdefghijklmnopqrstuvwxyztgJBhe9OV5EXAMPLE='
        self.assertFalse(dec_cli.run())

    def test_encrypt_with_short_key(self):
        """Test that encrypting when the key is too short fails."""
        # provide a short key and ensure encryption fails
        enc_cli = C3rCli('encrypt',
            input='samples/csv/data_sample_without_quotes.csv',
            parameters=[
                f'--id={all_y_id}',
                '--schema=samples/schema/config_sample_no_cleartext.json',
                '--output=encrypted_csv_out',
                '--overwrite'
            ]
        )
        self.assertTrue(enc_cli.run())
        enc_cli.key = 'Sg=='
        self.assertFalse(enc_cli.run())

    def test_round_trip_csv(self):
        """Test round tripping through encryption and decryption with a csv file."""
        enc_cli = C3rCli('encrypt',
            input='samples/csv/data_sample_without_quotes.csv',
            parameters= [
                f'--id={all_y_id}',
                '--schema=samples/schema/config_sample_no_cleartext.json',
                '--output=encrypted_csv_out',
                '--overwrite'
            ]
        )
        self.assertTrue(enc_cli.run())
        dec_cli = C3rCli('decrypt',
            input='encrypted_csv_out',
            parameters= [
                f'--id={all_y_id}',
                '--output=decrypted_csv_out',
                f'--fileFormat=csv',
                '--overwrite'
            ]
        )
        # ensure decryption works when we use the same key
        self.assertTrue(dec_cli.run())

    def test_round_trip_parquet(self):
        """Test round tripping through encryption and decryption with a parquet file."""
        enc_cli = C3rCli('encrypt',
            input='samples/parquet/data_sample.parquet',
            parameters=[
                f'--id={all_y_id}',
                '--schema=samples/schema/config_sample_no_cleartext.json',
                '--output=encrypted_parquet_out',
                '--overwrite'
            ]
        )
        self.assertTrue(enc_cli.run())
        dec_cli = C3rCli('decrypt',
            input='encrypted_parquet_out',
            parameters=[
                f'--id={all_y_id}',
                '--output=decrypted_parquet_out',
                f'--fileFormat=parquet',
                '--overwrite'
            ]
        )
        # ensure decryption works when we use the same key
        self.assertTrue(dec_cli.run())

def parse_int_test_cli_args():
    """Parse C3R CLI specific integration test flags from the command line.
    
    The following module-level variables will be populated based on CLI args:

        1. `c3r_spark_jar` - the c3r-cli-spark JAR file to test
        2. `all_y_id` - collaboration ID with all crypto params set to 'Yes'
        3. `all_n_id` - collaboration ID with all crypto params set to 'No'

    """
    global c3r_spark_jar
    global all_y_id
    global all_n_id

    parser = argparse.ArgumentParser(description='Integration tests for c3r-cli-spark.')
    parser.add_argument('c3r_spark_jar', metavar='c3r_spark_jar', type=str, 
        help="C3R spark command line JAR file to test.")
    req_grp = parser.add_argument_group(title='required parameters')
    req_grp.add_argument("--id-y", required=True, metavar="ID",
        help="AWS Clean Rooms collaboration ID with all crypto parameters set to 'Yes'.")
    req_grp.add_argument("--id-n", required=True, metavar="ID",
        help="AWS Clean Rooms collaboration ID with all crypto parameters set to 'No'.")

    args = vars(parser.parse_args())

    if not os.path.exists(args['c3r_spark_jar']):
        raise Exception('JAR did not exist: ' + c3r_spark_jar)

    c3r_spark_jar=args['c3r_spark_jar']
    all_y_id=args['id_y']
    all_n_id=args['id_n']

if __name__ == "__main__":
    # Parse the CLI args for our integration testing
    parse_int_test_cli_args()
    # Clear the CLI args so they don't appear for unittest.main()
    sys.argv = sys.argv[:1] + ['-v']
    unittest.main()