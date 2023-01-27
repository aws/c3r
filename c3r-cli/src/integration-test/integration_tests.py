import argparse
import unittest
import os as os
from pathlib import Path
import subprocess
import sys
from typing import Literal

# Directory at the top of the git repository, from which we can
# easily grab any desired files directly from the `samples` dir
# without needing reoccuring `..` in the path.
repo_dir=Path(os.path.dirname(__file__)).parent.parent.parent.absolute()


# c3r-cli JAR to test
c3r_cli_jar=None
# collaboration id with all crypto parameters 'Yes'
all_y_id=None
# collaboration id with all crypto parameters 'No'
all_n_id=None

class C3rCli():
    """
    Class that takes C3R CLI arguments and can be be used to run a subprocess C3R CLI application
    using the current value of the module variable `c3r_cli_jar`.
    """

    def __init__(self,
        mode : Literal['encrypt', 'decrypt', 'schema'],
        *,
        input : str = None,
        key : str = 'vLO8H9rCFs4FO9b+K9xGERL1IhtgJBhe9OV5EXAMPLE=',
        region : str = 'us-east-1',
        parameters : list[str]):
        """
        Constructs a C3R CLI configuration that can be run using the current `c3r_cli_jar` 
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
        command = f'java -jar {c3r_cli_jar} {self.mode} {self.input} {params}'
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

    def test_decrypt_with_wrong_key(self):
        """Test that decrypting with a different key than encrypting fails."""
        enc_cli = C3rCli('encrypt',
            input='samples/csv/data_sample_without_quotes.csv',
            parameters= [
                f'--id={all_y_id}',
                '--schema=samples/schema/config_sample_no_cleartext.json',
                '--output=encrypted.csv.out',
                '--overwrite'
            ]
        )
        self.assertTrue(enc_cli.run())
        dec_cli = C3rCli('decrypt',
            input='encrypted.csv.out',
            parameters= [
                f'--id={all_y_id}',
                '--output=decrypted.csv.out',
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
                '--output=encrypted.csv.out',
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
            parameters=[
                f'--id={all_y_id}',
                '--schema=samples/schema/config_sample_no_cleartext.json',
                '--output=encrypted.csv.out',
                '--overwrite'
            ]
        )
        self.assertTrue(enc_cli.run())
        dec_cli = C3rCli('decrypt',
            input='encrypted.csv.out',
            parameters=[
                f'--id={all_y_id}',
                '--output=decrypted.csv.out',
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
                '--output=encrypted.parquet.out',
                '--overwrite'
            ]
        )
        self.assertTrue(enc_cli.run())
        dec_cli = C3rCli('decrypt',
            input='encrypted.parquet.out',
            parameters=[
                f'--id={all_y_id}',
                '--output=decrypted.parquet.out',
                f'--fileFormat=parquet',
                '--overwrite'
            ]
        )
        # ensure decryption works when we use the same key
        self.assertTrue(dec_cli.run())

def parse_int_test_cli_args():
    """Parse C3R CLI specific integration test flags from the command line.
    
    The following module-level variables will be populated based on CLI args:

        1. `c3r_cli_jar` - the c3r-cli JAR file to test
        2. `all_y_id` - collaboration ID with all crypto params set to 'Yes'
        3. `all_n_id` - collaboration ID with all crypto params set to 'No'

    """
    global c3r_cli_jar
    global all_y_id
    global all_n_id

    parser = argparse.ArgumentParser(description='Integration tests for c3r-cli.')
    parser.add_argument('c3r_cli_jar', metavar='C3R_CLI_JAR', type=str, 
        help="C3R command line JAR file to test.")
    req_grp = parser.add_argument_group(title='required parameters')
    req_grp.add_argument("--id-y", required=True, metavar="ID",
        help="AWS Clean Rooms collaboration ID with all crypto parameters set to 'Yes'.")
    req_grp.add_argument("--id-n", required=True, metavar="ID",
        help="AWS Clean Rooms collaboration ID with all crypto parameters set to 'No'.")

    args = vars(parser.parse_args())

    if not os.path.exists(args['c3r_cli_jar']):
        raise Exception('JAR did not exist: ' + c3r_cli_jar)

    c3r_cli_jar=args['c3r_cli_jar']
    all_y_id=args['id_y']
    all_n_id=args['id_n']

if __name__ == "__main__":
    # Parse the CLI args for our integration testing
    parse_int_test_cli_args()
    # Clear the CLI args so they don't appear for unittest.main()
    sys.argv = sys.argv[:1] + ['-v']
    unittest.main()