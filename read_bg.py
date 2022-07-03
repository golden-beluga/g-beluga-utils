import os
import pandas as pd
from google.cloud import bigquery


# set_up
project_id = os.environ['PROJECT_ID']
dataset_id = 'raw_data'
table_id = 'horse_race_jockey'
client = bigquery.Client(project=project_id)

# main
# BigQueryからDataFrame形式でロード

def hrj_read() -> pd.DataFrame:
  project = os.environ['PROJECT_ID']
  dataset = 'raw_data'
  table = 'horse_race_jockey'

  query = f"""
  SELECT * EXCEPT(made_by, created_at)
  FROM {project}.{dataset}.{table}
  """

  return pd.read_gbq(query, project, dialect='standard')

