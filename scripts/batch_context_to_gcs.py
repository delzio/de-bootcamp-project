import sys
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--gcs_bucket', required=True)
parser.add_argument('--gcs_input_path', required=True)
parser.add_argument('--gcs_output_path', required=True)

args = parser.parse_args()

sys.path.append(f"gs://{args.gcs_bucket}/pyspark_code")
from dag_functions import batch_context_to_gcs

batch_context_to_gcs(gcs_bucket=args.gcs_bucket, gcs_input_path=args.gcs_input_path, gcs_output_path=args.gcs_output_path)