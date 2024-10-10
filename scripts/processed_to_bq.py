import sys
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()

parser.add_argument('--gcs_bucket', required=True)
parser.add_argument('--dataset', required=True)
parser.add_argument('--gcs_raw_path', required=True)
parser.add_argument('--gcs_sample_path', required=True)
parser.add_argument('--project_id', required=True)
parser.add_argument('--current_time', required=True)
parser.add_argument('--full_backfill', required=True)

args = parser.parse_args()

sys.path.append(f"gs://{args.gcs_bucket}/pyspark_code")
from dag_functions import processed_to_bq

current_time = datetime.fromisoformat(args.current_time)
full_backfill = bool(args.full_backfill)
processed_to_bq(gcs_bucket=args.gcs_bucket, dataset=args.dataset, gcs_raw_path=args.gcs_raw_path, gcs_sample_path=args.gcs_sample_path, project_id=args.project_id, current_time=current_time, full_backfill=full_backfill)