# ライブラリ

# bigqueryと接続する
from google.cloud import bigquery
import pandas as pd
import time
import os

# 設定
json_file = os.environ['BIGQUERY_CREDENTIAL']

# 自身のGCPProjecIDを指定
project_id = os.environ['PROJECT_ID']
dataset_name = 'raw_data'
client = bigquery.Client(project=project_id)


scraping_dir = os.listdir(path=os.environ['RAW_DATA_LOCATION'])
for file in scraping_dir:
  print(file)
  if(file != '.DS_Store'):
    sub_files = os.listdir(path=os.environ['RAW_DATA_LOCATION'] + '/' + file)
    for sub in sub_files:
      print('======= ' + sub + ' START!!')

      csv_file = sub
      csv_file_name = os.environ['RAW_DATA_LOCATION'] + '/' + file + '/' + sub


      df = pd.read_csv(csv_file_name)

      # bigqueryでシャード化するために変更
      idx = csv_file.find('-')
      r = csv_file[:idx]
      csv_file = r + '_' + df['race_id'].astype(str)[len(df)  - 1][:8]
      print('bigquery_table_name: ' + csv_file)



      # bigqueryではnanを許容していないためnullに置き換える
      df=df.fillna("null")

      cnt_row = len(df) - 1

      df_columns = list(df.columns)
      schema_columns = []
      for column in df_columns:
          schema_columns.append(column.replace('-','_'))
      print(schema_columns)

      dtypes = df.dtypes
      retypes = []
      for dtype in dtypes:
          if(str(dtype) == 'object'):
              retypes.append('STRING')
          elif('datetime' in str(dtype)):
              retypes.append('DATETIME')
          elif('float' in str(dtype)):
              retypes.append('FLOAT')
          else:
              retypes.append(str(dtype).upper())
      print(retypes)

      schema = []
      for column, redtype in zip(schema_columns, retypes):
          if (column != 'made_by' and redtype == 'STRING' ):
              schema.append(bigquery.SchemaField(column, redtype, mode="NULLABLE"))
          else:
              schema.append(bigquery.SchemaField(column, redtype, mode="REQUIRED"))

      table_name = csv_file

      table_id = project_id + '.' + dataset_name + '.' + table_name
      table = bigquery.Table(table_id, schema=schema)
      error = 'null'
      try:
          table = client.create_table(table) # Make an API request.
      except Exception as e:
          error = e
          print(e)
      if(error == 'null'):
          print("Succeed Create Table !! ")


      print(
          "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
      )

      df_list = df.values.tolist()


      # データ insert
      table = client.get_table(table_id)  # Make an API request.

      # リストの二次元配列
      # データ数が10000行より大きい場合
      rows = len(df_list)
      count = 0
      number = 990 # 一度にinsertする行数
      while rows > 0:
          errors = client.insert_rows(table, df_list[(count * number): ((count+1) * number)])
          if errors == []:
              print("COUNT: " + str(count) + "  New rows have been added.")
          rows = rows - number
          count = count + 1
          time.sleep(1)





