import argparse
from datetime import datetime
from dag_functions import DagFunctions


parser = argparse.ArgumentParser()

parser.add_argument('--gcs_bucket', required=True)
parser.add_argument('--dataset', required=True)
parser.add_argument('--gcs_raw_path', required=True)
parser.add_argument('--project_id', required=True)
parser.add_argument('--pls_model_path', required=True)
parser.add_argument('--current_time', required=True)

args = parser.parse_args()

current_time = datetime.fromisoformat(args.current_time)
DagFunctions.pls_prediction_to_bq(gcs_bucket=args.gcs_bucket, dataset=args.dataset, gcs_raw_path=args.gcs_raw_path, project_id=args.project_id, pls_model_path=args.pls_model_path, current_time=current_time)