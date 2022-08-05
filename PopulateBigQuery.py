# PROJECT_ID = 'gcp-core-services'
# pip install Flask gunicorn requests uuid google-cloud-bigquery

# Kaggle Credentitals-> login to Kaggle -> create API Token -> copy the Json to C:\users\tparamanandam\.kaggle\ Folder
#
# For GCP credentials -> Create Key from GCP for the service account -> copy to local path->
#     -> add path to GOOGLE_APPLICATION_CREDENTIALS env variable
#     -> GOOGLE_APPLICATION_CREDENTIALS ='C:\Users\tparamanandam\.google\gcp-core-services.json'

# from google.cloud import bigquery
# client = bigquery.Client(project=PROJECT_ID, location="US")
# dataset = client.create_dataset('bqml_tutorial', exists_ok=True)
#
# from google.cloud.bigquery import magics
# from kaggle_gcp  import  KaggleKernelCredentials
# magics.context.credentials = KaggleKernelCredentials()
# magics.context.project = PROJECT_ID

# query_job = client.query("""
#    SELECT *
#    FROM us_states_dataset.projectteam
#    LIMIT 1000 """)

# results = query_job.result()
# print(results.total_rows)
# for i in results:
#     print(i)


from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest
import pandas as pd

credentials = service_account.Credentials.from_service_account_file('C:/Users/tparamanandam/.google/smartappbeta-mta.json')

project_id ='smartappbeta-mta'
client = bigquery.Client(credentials= credentials,project=project_id)

dataset=client.create_dataset(dataset='Gilbane_Extract', exists_ok=True)
table=dataset.table('Companies')

file ='Temp.jsonl' ;

#
# import newlinejson as nlj
# with nlj.open(file) as src :
#     for line in src:
#         print(line)


job_config= bigquery.LoadJobConfig(
    autodetect=False, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
)
try :
    with open(file, mode='rb') as f:
        load_job=client.load_table_from_file(f,table,job_config=job_config)
        load_job.result()
        # destination_table = client.get_table(table)
        # print("Loaded {} rows.".format(destination_table.num_rows))
except BadRequest as e:
    for e in load_job.errors:
        print('Error :{}'.format(e['message']))



"""
df = pd.read_json('D:/IQEXTRACT/2022_05_31/pceed45bfd809d4df2a95acd4225089ac7/Accident Notification Contact List.jsonl', lines=True)

for index, row in df.iterrows():
  print(row[0])
print(df.head())
"""
