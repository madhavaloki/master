####imports
from file_merge_functions import run_file_merge
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta, FR
import json
import airflow
from airflow import models
from google.cloud import bigquery
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.hooks.datafusion import PipelineStates
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.bash import BashOperator                             
from google.cloud import storage
from get_dates import get_dates, get_pfecha
import ast
import io
import os
import google.auth
import pytz
#####

####key
key = {0}.format(json_key) #'ah_noval'
------------
Add logic to load json input
------------
table_name =  key.lower()#key
#####


####variables
environment = ""

project = os.environ.get("GCP_PROJECT")

if project == "datalake2-desarrollo":
    environment = "dev"
elif project == "datalake2-laboratorio":
    environment = "lab"
elif project == "datalake2-prototipado":
    environment = "proto"
else:
    environment= "prod"

credentials,project = google.auth.default()
storage_client = storage.Client(credentials=credentials)
composer_bucket_name=os.environ.get("GCS_BUCKET")
bucket=storage_client.get_bucket(composer_bucket_name)
blob1 = bucket.blob("variables-"+environment+"/vars-"+environment+".json")
data = json.loads(blob1.download_as_string(client=None))
configuration_bucket_name = data["configuration_bucket_name"]
------------
namespace = value #need to check how this can be used
#Check with Ankita and Nagin once what else need to be parameterized
------------

current_time = date.today().strftime('%Y%m%d')
config_buket = storage_client.get_bucket(configuration_bucket_name)  
blob = config_buket.get_blob('configurations.json') #change #fetch us-central1 from configuration.json
config_string = blob.download_as_string()  
config = json.loads(config_string)
#####


####parameter
filename = 'AH_NOVAL'  #change
fuentes_cloudera_table_name="ah_noval" #change
report_id = '161' #change
pfecha = get_pfecha(report_id)
table_id = '{}.{}.{}'.format(config['project_id'], config['dest_dataset_id'], table_name)

blob = config_buket.get_blob('all_directory_list_1.json') #change
table_directory_list_string = blob.download_as_string()  
table_directory_list = json.loads(table_directory_list_string)
#####


####timezone
cot = pytz.timezone('America/Bogota')
df_file_path_to_delete_from_dict = table_directory_list[table_name.upper()]
df_file_path_to_delete = df_file_path_to_delete_from_dict[0].replace('$','\$')
df_rename_file_name = filename + '_' + (datetime.now(cot)- timedelta(days = 1)).strftime("%Y%m%d") + ".csv"
df_yesterday = (datetime.now(cot)- timedelta(days=1)).strftime("%Y-%m-%d")
file_path = df_file_path_to_delete.split("/")
destination_path = "gs://"+file_path[2]+"/"+"processed/"
source_bucket="gs://"+file_path[2]
dag_schedule = df_file_path_to_delete_from_dict[1]

if dag_schedule == 'None':
    dag_schedule = None

#gcs_delete_cmd = 'gcloud storage rm -r '+ df_file_path_to_delete + df_yesterday + ' ; exit 0'
gcs_rename_cmd = 'gcloud storage mv ' + df_file_path_to_delete + df_yesterday + '/part-r-00000 ' +  "gs://"+ df_file_path_to_delete.split("/")[2]+"/landing/" + df_rename_file_name +  ' ; exit 0'
gcs_to_gcs_cmd ='gsutil mv ' + source_bucket + '/landing/' + df_rename_file_name +' '+ destination_path
gcs_delete_cmd='gsutil rm  '+ df_file_path_to_delete  +'**'
#####


####odiquery
odi_query = """
Select 
Upper(REGEXP_REPLACE( TRANSLATE(COD_PRODUCTO, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as COD_PRODUCTO,
Upper(REGEXP_REPLACE( TRANSLATE(COD_CUENTA, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as NRO_CUENTA,
Upper(REGEXP_REPLACE( TRANSLATE(COD_CUENTA, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as NRO_CUENTA_CIFRADO,
Upper(REGEXP_REPLACE( TRANSLATE(OFICINA_RECAUDO, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as OFICINA_RECAUDO ,          
FECHA_SISTEMA ,    
HORA_SISTEMA,      
Upper(REGEXP_REPLACE( TRANSLATE(TIPO_MOVIMIENTO, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as TIPO_MOVIMIENTO,
NRO_TRANSACCION ,          
NRO_IDENTIFICACION ,          
NRO_IDENTIFICACION AS NRO_IDENTIFICACION_CIFRADO ,
Upper(REGEXP_REPLACE( TRANSLATE(TIPO_IDENTIFICACION, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as TIPO_IDENTIFICACION,
FECHA_JORNADA ,          
Upper(REGEXP_REPLACE( TRANSLATE(CANAL, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as CANAL,
Upper(REGEXP_REPLACE( TRANSLATE(MEDIO_TRANSAC, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as MEDIO_TRANSAC,
COD_MASCARA ,          
IDENT_TERMINAL ,          
Upper(REGEXP_REPLACE( TRANSLATE(CODIGO_USUARIO, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as CODIGO_USUARIO,
CODIGO_SPV ,          
REFERENCIA_1 ,     
REFERENCIA_1 AS REFERENCIA_1_CIFRADO ,          
REFERENCIA_2 ,          
REFERENCIA_2 AS REFERENCIA_2_CIFRADO ,     
VALOR_TOTAL ,          
MOTIVO_CANCEL ,          
CLASE_TALONARIO ,          
MANEJO_CUENTA ,          
OBJETIVO_CUENTA ,          
DESTINO_EXTRACTO ,          
MEDIO_ENTREGA_EXT ,          
IND_BLOQUEO ,          
TARJETA_DEBITO ,          
VRND_INFORMATIVA ,          
CTA_BANCO AS CUENTA_BANCO,          
CTA_BANCO AS CUENTA_BANCO_CIFRADO,
MOTIVO_DEVOLUCION ,          
Upper(REGEXP_REPLACE( TRANSLATE(CHEQ_PERSONALIZADA, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as CHEQ_PERSONALIZADA,
CANT_CHEQUES ,          
SOLICITUD_ACTIVAC ,          
NROID_AUTORIZADO ,
NROID_AUTORIZADO AS  NROID_AUTORIZADO_CIFRADO,          
TIPOID_AUTORIZADO ,          
TIPO_AUTORIZACION ,          
NOVEDAD_AUTORIZADO ,          
IND_BOQUEOCC ,          
TIPO_BLOQUEO ,          
MOTIVO_BLOQUEO ,          
MOTBLQ_CHEQUE ,          
CANTBLQ_CHEQUE ,          
NRO_INICHEQUE ,          
NRO_FINCHEQUE ,          
Upper(REGEXP_REPLACE( TRANSLATE(TIPO_PERSONA, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as TIPO_PERSONA,
Upper(REGEXP_REPLACE( TRANSLATE(TIPO_SEG, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as TIPO_SEG,
Upper(REGEXP_REPLACE( TRANSLATE(TIPO_COLOR, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as TIPO_COLOR,
Upper(REGEXP_REPLACE( TRANSLATE(PORTAFOLIO, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as PORTAFOLIO,
Upper(REGEXP_REPLACE( TRANSLATE(COD_VENDEDOR, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as COD_VENDEDOR,
Upper(REGEXP_REPLACE( TRANSLATE(ESTRATEGIA, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as ESTRATEGIA,
Upper(REGEXP_REPLACE( TRANSLATE(OFI_RADICACION, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as OFI_RADICACION,
Upper(REGEXP_REPLACE( TRANSLATE(SUBPRODUCTO, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as SUBPRODUCTO,
Upper(REGEXP_REPLACE( TRANSLATE(JORNADA, 'áéíóúÁÉÍÓÚñÑ ', 'aeiouAEIOUnN'),'[^0-9A-Za-z]','')) as JORNADA
from {}.{}.{}
where DATE(PERIODO) = DATE_TRUNC(PARSE_DATE('%Y%m%d',{}), MONTH)
"""
#####


####functions
def start_pipeline(ini, **kwargs):
    ''' 
        Function is used to start the Datafusion pipeline.
        It will load the data from the respective RDBMS to BigQuery RAW-ZONE.
    '''
    for dates in ini.strip('][').split(','):
        dates = dates.strip('\'')
        print(dates)
        start_pipeline_one = CloudDataFusionStartPipelineOperator(
            location='us-central1', #change
            pipeline_name='AH_NOVAL_ORACLE_TO_BQ',  #change
            instance_name=config['instance_name'],
            task_id="start_pipeline"+str(dates),
            success_states=[PipelineStates.COMPLETED],
            runtime_args={"output.instance": "someOutputInstance",
                        "input.dataset": "someInputDataset",
                        "input.project": "someInputProject",
                        "input.fecha_ini": str(dates),
                        "Path_Suffix": str(df_yesterday),
                        "system.profile.name":config['high_profile'],
                        "gcp.cmek.key.name":config['gcp_kms_key']
                        },
            pipeline_timeout=21600,
            retries=0
        )
        start_pipeline_one.execute(kwargs)

    return "completed"

def get_gcs_path():
    ''' 
        Querying Metadata table to retrieve information of destination CSV file path and delimiter.
    '''
    var=[]
    client = bigquery.Client()
    sql = """ select database_destino, trim(delimitador_campo) from `{}.{}.{}` where nombre_archivo=upper({}) or nombre_archivo={} limit 1 """.format(config['project_id'],config['taxonomy_dataset'],config['taxonomy_table'], "\'" + fuentes_cloudera_table_name.upper() + "\'", "\'" + fuentes_cloudera_table_name.upper() + "\'")
    try:
        query_job = client.query(sql).to_dataframe()
        var.append(query_job.iloc[0][0].split("/")[-1])
        var.append(query_job.iloc[0][1])
    except Exception as e:
        return 'temporary'
    return var

def run_bq_temp_to_gcs(ini, variable, **kwargs):
    '''
        Function to trigger the ODI query.
        An ODI query will be run against RAW-ZONE and the transformed data will be loaded into temporary tables of SEMI_RAW-ZONE and
        then exports the data from the temporary tables to GCS.     
    '''

    for dates in ini.strip('][').split(','):
        dates = dates.strip('\'')
        bq_semi_raw_gcs_intermediate = BigQueryInsertJobOperator(
            task_id=table_name.lower()+str(dates),
            configuration={
                "query": {
                    "query": odi_query.format(config['project_id'], 
                    config['dataset_id'], 
                    table_name,
                    "'"+str(dates)+"'"),
                    "destinationTable": {
                        "projectId": config['project_id'],
                        "datasetId": config['dest_dataset_id'],
                        "tableId": table_name,
                    },
                    "useLegacySql": False,
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
            dag=dag,
        )
        bq_semi_raw_gcs_intermediate.execute(kwargs)

        path_var = variable.strip('][').split(',')[0]
        del_var = variable.strip('][').split(',')[1].strip(' ')
        dataset = table_id.split('.')
        source_project_dataset_table=dataset[1]+'.'+dataset[2]
        location='us-central1'
        destination_cloud_storage_uris='{}/{}/{}*.csv'.format(config['bucket_name'], path_var.strip('\''),filename.lower() + "_" + str(dates))
        cmd = 'bq extract --location='+location+' --destination_format CSV --field_delimiter '+del_var+' --print_header=false '+source_project_dataset_table+' '+destination_cloud_storage_uris+''
        bq_to_gcs = BashOperator (task_id='bq_to_gcs', bash_command=cmd)

        bq_to_gcs.execute(kwargs)


def run_merge(variable):
    path_var = variable.strip('][').split(',')[0]
    bucket_name = config['bucket_name']
    temp_file_location_with_gs='{}/{}/{}'.format(config['bucket_name'], path_var.strip('\''),filename.lower() + "_" + (date.today()- timedelta(days = 1)).strftime('%Y%m%d'))
    temp_file_location=temp_file_location_with_gs.replace("gs://"+temp_file_location_with_gs.replace("gs://","").split("/")[0],"")[1:]
    run_file_merge(temp_file_location)
#####


####defargs
DEFAULT_DAG_PARAMS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': config['dag_email'],             
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup': False,
    'start_date': datetime(2023, 2, 16),
}

with models.DAG(
        dag_id=table_name+'_oracle_to_bq_test_2',
        default_args=DEFAULT_DAG_PARAMS,
        schedule_interval=dag_schedule                  
) as dag:
#####


####tasks

    t1 = PythonOperator(
        task_id = 'dates_list',
        python_callable = get_dates,
        op_kwargs={'FECHA_FIN': pfecha[1], 'FECHA_INI': pfecha[0]}
    )

    t2 = PythonOperator(
        task_id = 'run_start_pipeline',
        python_callable = start_pipeline,
        op_kwargs = {'ini': "{{ ti.xcom_pull(task_ids='dates_list') }}"}
    )
    
    t3 = PythonOperator(
        task_id='get_gcs_path',
        python_callable=get_gcs_path
    )

    t4 = PythonOperator(
        task_id='run_bq_temp_to_gcs',
        python_callable=run_bq_temp_to_gcs,
        op_kwargs={'ini': "{{ ti.xcom_pull(task_ids='dates_list') }}", 'variable': "{{ ti.xcom_pull(task_ids='get_gcs_path') }}"}
    )

    merge_files = PythonOperator(
        task_id='run_davapp_merge',
        python_callable=run_merge,
        op_kwargs={'variable': "{{ ti.xcom_pull(task_ids='get_gcs_path') }}"}
    )


    delete_df_gcs_file = BashOperator(
        task_id="delete_df_gcs_file",
        bash_command=gcs_delete_cmd,
    )

    rename_df_gcs_file = BashOperator(
        task_id="rename_df_gcs_file",
        bash_command=gcs_rename_cmd,
    )

    move_df_gcs_files = BashOperator(
        task_id="move_df_gcs_files",
        bash_command=gcs_to_gcs_cmd,
    )


    t1 >> t2 >> rename_df_gcs_file >> delete_df_gcs_file >> t3 >> t4 >> merge_files >> move_df_gcs_files

#####