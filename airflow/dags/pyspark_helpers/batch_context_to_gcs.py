import argparse
from dag_functions import DagFunctions

parser = argparse.ArgumentParser()

parser.add_argument('--gcs_bucket', required=True)
parser.add_argument('--gcs_input_path', required=True)
parser.add_argument('--gcs_output_path', required=True)

args = parser.parse_args()

DagFunctions.batch_context_to_gcs(gcs_bucket=args.gcs_bucket, gcs_input_path=args.gcs_input_path, gcs_output_path=args.gcs_output_path)