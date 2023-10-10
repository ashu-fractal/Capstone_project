#import modules
import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import google.auth
import google.auth.transport.requests




#give credentials for current project
credentials, project_id = google.auth.default(scopes=['https://storage.cloud.google.com/us-central1-composer1-041882ce-bucket/dags/scripts/one_time_load.py'])
authed_session = google.auth.transport.requests.AuthorizedSession(credentials)
location = 'asia-south2' 

#setup default arguments for dag
default_args = {
        'retries': 1,
        'owner' : 'airflow',
        'execution_timeout' : timedelta(seconds=300),
        'start_date' : airflow.utils.dates.days_ago(1)
}

# gcsfuse gs://ashu_bucket/ /path/to/mount/

# service = googleapiclient.discovery.build('storage', 'v1')


# #### List Bucket
# fields_to_return = \
#         'nextPageToken,items(name,size,contentType,metadata(my-key))'
# req = service.objects().list(bucket='ashu-bucket', fields=fields_to_return)
# resp = req.execute()

# ### Get Object 
# req = service.objects().get_media(bucket='ashu-bucket', object='incremental.py')

#call the dag
dag = DAG(
        dag_id='ashu-dag1',
        default_args=default_args,
        #dagrun_timeout=timedelta(minutes=20),
        schedule_interval='@once', 
        catchup=False,
    )

# def load_data(filepath):
#     with open(filepath, 'r') as file:
#         code = file.read()
#         exec(code)


# Step 4: Creating task
# Creating first task
start = DummyOperator(task_id = 'start',
                      dag = dag
                        )

# download = BashOperator(
#                 task_id = 'download',
#                 bash_command = 'gsutil -m cp -r  gs://ashu_bucket ~/',
#                 dag = dag)

# download1 = BashOperator(
#                 task_id = 'download1',
#                 bash_command = 'cd ashu_bucket',
#                 dag = dag)

# gsutil -m cp -r incremental.py gs://ashu_bucket ~/
# gcsfuse ashu-bucket /path/to/mount/

#creating the task
sql_to_bq = BashOperator(
    task_id = 'sql_to_bq',
    bash_command = 'python us-central1-composer1-041882ce-bucket/dags/scripts/one_time_load.py',
    dag =dag
)


# sql_to_bq = BashOperator(
#                 task_id = 'sql_to_bq',
                                
#                 bash_command =  "gsutil cat gs://ashu_bucket/incremental.py",
#                 dag = dag) 
    
#end task
end = DummyOperator(task_id = 'end',
                     dag = dag)

#defining dependencies
start >> sql_to_bq >>end 
