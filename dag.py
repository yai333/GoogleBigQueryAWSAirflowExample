# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import S3KeySensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from botocore.exceptions import ClientError
from datetime import timedelta, datetime
import logging
import sys
import boto3
import time
import zipfile
import json
import os
import pandas as pd
import numpy as np
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
suffix = str(np.random.uniform())[4:9]

BUCKET_NAME = 'airflow-demo-personalise'
LIB_KEY = 'libs/site-packages.zip'
GLUE_DATABASE_NAME = "google_analytics"
TABLE_NAME = "session_events"
DATASET_GROUP_NAME = f"DEMO-personalise"
INTERCATION_SCHEMA_NAME = f"DEMO-interaction-schema-{suffix}"
SOLUTION_NAME = "DEMO-popularity-count"
CAMPAIGN_NAME = "DEMO-popularity-campaign"
pre_utc_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
OUTPUT_FILE_KEY = f"session-events/dt={pre_utc_date}/events.csv"
OUTPUT_PATH = f"s3://{BUCKET_NAME}/{OUTPUT_FILE_KEY}"
PERSONALIZE_ROLE_NAME = "PersonalizeS3Role"
iam = boto3.client("iam")
personalize = boto3.client('personalize')

BQ_SQL = """
SELECT  REGEXP_EXTRACT(USER_ID, r'(\d+)\.') AS USER_ID, UNIX_SECONDS(EVENT_DATE) AS TIMESTAMP, REGEXP_EXTRACT(page_location,r'product/([^?&#]*)') as ITEM_ID, LOCATION, DEVICE, EVENT_NAME AS EVENT_TYPE
FROM
(
    SELECT user_pseudo_id AS USER_ID, (SELECT value.string_value FROM UNNEST(event_params) 
    WHERE key = "page_location") as page_location, TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), 
    MINUTE) AS EVENT_DATE, device.category AS DEVICE, geo.country AS LOCATION, event_name AS EVENT_NAME 
    FROM `lively-metrics-295911.analytics_254171871.events_intraday_*`
    WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
WHERE  page_location LIKE '%/product/%'
GROUP BY USER_ID, EVENT_DATE, page_location, LOCATION, DEVICE,EVENT_NAME
ORDER BY EVENT_DATE DESC
"""


def bq_to_s3():
    s3 = boto3.resource('s3')
    logger.info(
        "Download google bigquery and google client dependencies from S3")
    s3.Bucket(BUCKET_NAME).download_file(LIB_KEY, '/tmp/site-packages.zip')

    with zipfile.ZipFile('/tmp/site-packages.zip', 'r') as zip_ref:
        zip_ref.extractall('/tmp/python3.7/site-packages')

    sys.path.insert(1, "/tmp/python3.7/site-packages/site-packages/")

    # Import google bigquery and google client dependencies
    import pyarrow
    from google.cloud import bigquery
    from airflow.contrib.hooks.bigquery_hook import BigQueryHook

    bq_hook = BigQueryHook(
        bigquery_conn_id="bigquery_default", use_legacy_sql=False)

    bq_client = bigquery.Client(project=bq_hook._get_field(
        "project"), credentials=bq_hook._get_credentials())

    events_df = bq_client.query(BQ_SQL).result().to_dataframe(
        create_bqstorage_client=False)

    logger.info(
        f'google analytics events dataframe head - {events_df.head()}')

    wr.s3.to_csv(events_df, OUTPUT_PATH, index=False)


def create_dataset_group(**kwargs):
    create_dg_response = personalize.create_dataset_group(
        name=DATASET_GROUP_NAME
    )
    dataset_group_arn = create_dg_response["datasetGroupArn"]

    status = None
    max_time = time.time() + 2*60*60  # 2 hours
    while time.time() < max_time:
        describe_dataset_group_response = personalize.describe_dataset_group(
            datasetGroupArn=dataset_group_arn
        )
        status = describe_dataset_group_response["datasetGroup"]["status"]
        logger.info(f"DatasetGroup: {status}")

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(20)
    if status == "ACTIVE":
        kwargs['ti'].xcom_push(key="dataset_group_arn",
                               value=dataset_group_arn)
    if status == "CREATE FAILED":
        raise AirflowFailException(
            f"DatasetGroup {DATASET_GROUP_NAME} create failed")


def check_dataset_group(**kwargs):
    dg_response = personalize.list_dataset_groups(
        maxResults=100
    )

    demo_dg = next((datasetGroup for datasetGroup in dg_response["datasetGroups"]
                    if datasetGroup["name"] == DATASET_GROUP_NAME), False)

    if not demo_dg:
        return "init_personalize"
    else:
        kwargs['ti'].xcom_push(key="dataset_group_arn",
                               value=demo_dg["datasetGroupArn"])
        return "skip_init_personalize"


def create_schema():
    schema_response = personalize.list_schemas(
        maxResults=100
    )

    interaction_schema = next((schema for schema in schema_response["schemas"]
                               if schema["name"] == INTERCATION_SCHEMA_NAME), False)
    if not interaction_schema:
        create_schema_response = personalize.create_schema(
            name=INTERCATION_SCHEMA_NAME,
            schema=json.dumps({
                "type": "record",
                "name": "Interactions",
                "namespace": "com.amazonaws.personalize.schema",
                "fields": [
                    {
                        "name": "USER_ID",
                        "type": "string"
                    },
                    {
                        "name": "ITEM_ID",
                        "type": "string"
                    },
                    {
                        "name": "TIMESTAMP",
                        "type": "long"
                    },
                    {
                        "name": "LOCATION",
                        "type": "string",
                        "categorical": True
                    },
                    {
                        "name": "DEVICE",
                        "type": "string",
                        "categorical": True
                    },
                    {
                        "name": "EVENT_TYPE",
                        "type": "string"
                    }
                ]
            }))
        logger.info(json.dumps(create_schema_response, indent=2))
        schema_arn = create_schema_response["schemaArn"]
        return schema_arn

    return interaction_schema["schemaArn"]


def put_bucket_policies():
    s3 = boto3.client("s3")
    policy = {
        "Version": "2012-10-17",
        "Id": "PersonalizeS3BucketAccessPolicy",
        "Statement": [
            {
                "Sid": "PersonalizeS3BucketAccessPolicy",
                "Effect": "Allow",
                "Principal": {
                    "Service": "personalize.amazonaws.com"
                },
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::{}".format(BUCKET_NAME),
                    "arn:aws:s3:::{}/*".format(BUCKET_NAME)
                ]
            }
        ]
    }

    s3.put_bucket_policy(Bucket=BUCKET_NAME, Policy=json.dumps(policy))


def create_iam_role(**kwargs):
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "personalize.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    try:
        create_role_response = iam.create_role(
            RoleName=PERSONALIZE_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
        )

        iam.attach_role_policy(
            RoleName=PERSONALIZE_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )

        role_arn = create_role_response["Role"]["Arn"]

        # sometimes need to wait a bit for the role to be created
        time.sleep(30)
        return role_arn
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            role_arn = iam.get_role(RoleName=PERSONALIZE_ROLE_NAME)[
                'Role']['Arn']
            time.sleep(30)
            return role_arn
        else:
            raise AirflowFailException(f"PersonalizeS3Role create failed")


def create_dataset_type(**kwargs):
    ti = kwargs['ti']
    schema_arn = ti.xcom_pull(key="return_value", task_ids='create_schema')
    dataset_group_arn = ti.xcom_pull(key="dataset_group_arn",
                                     task_ids='create_dataset_group')
    dataset_type = "INTERACTIONS"
    create_dataset_response = personalize.create_dataset(
        datasetType=dataset_type,
        datasetGroupArn=dataset_group_arn,
        schemaArn=schema_arn,
        name=f"DEMO-metadata-dataset-interactions-{suffix}"
    )

    interactions_dataset_arn = create_dataset_response['datasetArn']
    logger.info(json.dumps(create_dataset_response, indent=2))
    return interactions_dataset_arn


def import_dataset(**kwargs):
    ti = kwargs['ti']
    interactions_dataset_arn = ti.xcom_pull(key="return_value",
                                            task_ids='create_dataset_type')
    role_arn = ti.xcom_pull(key="return_value",
                            task_ids='create_iam_role')

    if not role_arn:
        role_arn = iam.get_role(RoleName=PERSONALIZE_ROLE_NAME)[
            'Role']['Arn']
        time.sleep(30)

    create_dataset_import_job_response = personalize.create_dataset_import_job(
        jobName="DEMO-dataset-import-job-"+suffix,
        datasetArn=interactions_dataset_arn,
        dataSource={
            "dataLocation": OUTPUT_PATH
        },
        roleArn=role_arn
    )

    dataset_import_job_arn = create_dataset_import_job_response['datasetImportJobArn']
    logger.info(json.dumps(create_dataset_import_job_response, indent=2))

    status = None
    max_time = time.time() + 2*60*60  # 2 hours

    while time.time() < max_time:
        describe_dataset_import_job_response = personalize.describe_dataset_import_job(
            datasetImportJobArn=dataset_import_job_arn
        )

        dataset_import_job = describe_dataset_import_job_response["datasetImportJob"]
        if "latestDatasetImportJobRun" not in dataset_import_job:
            status = dataset_import_job["status"]
            logger.info("DatasetImportJob: {}".format(status))
        else:
            status = dataset_import_job["latestDatasetImportJobRun"]["status"]
            logger.info("LatestDatasetImportJobRun: {}".format(status))

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(60)

    if status == "ACTIVE":
        return dataset_import_job_arn
    if status == "CREATE FAILED":
        raise AirflowFailException(
            f"Dataset import job create failed")


def update_solution(**kwargs):
    recipe_arn = "arn:aws:personalize:::recipe/aws-popularity-count"
    ti = kwargs['ti']
    dataset_group_arn = ti.xcom_pull(key="dataset_group_arn",
                                     task_ids='create_dataset_group')
    list_solutions_response = personalize.list_solutions(
        datasetGroupArn=dataset_group_arn,
        maxResults=100
    )

    demo_solution = next((solution for solution in list_solutions_response["solutions"]
                          if solution["name"] == SOLUTION_NAME), False)

    if not demo_solution:
        create_solution_response = personalize.create_solution(
            name=SOLUTION_NAME,
            datasetGroupArn=dataset_group_arn,
            recipeArn=recipe_arn
        )

        solution_arn = create_solution_response['solutionArn']
        logger.info(json.dumps(create_solution_response, indent=2))
    else:
        solution_arn = demo_solution["solutionArn"]

    kwargs['ti'].xcom_push(key="solution_arn",
                               value=solution_arn)
    create_solution_version_response = personalize.create_solution_version(
        solutionArn=solution_arn,
        trainingMode='FULL'
    )

    solution_version_arn = create_solution_version_response['solutionVersionArn']

    status = None
    max_time = time.time() + 2*60*60  # 2 hours
    while time.time() < max_time:
        describe_solution_version_response = personalize.describe_solution_version(
            solutionVersionArn=solution_version_arn
        )
        status = describe_solution_version_response["solutionVersion"]["status"]
        logger.info(f"SolutionVersion: {status}")

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(60)

    if status == "ACTIVE":
        return solution_version_arn
    if status == "CREATE FAILED":
        raise AirflowFailException(
            f"Solution version create failed")


def update_campaign(**kwargs):
    ti = kwargs['ti']
    solution_version_arn = ti.xcom_pull(key="return_value",
                                        task_ids='update_solution')
    solution_arn = ti.xcom_pull(key="solution_arn",
                                task_ids='update_solution')

    list_campaigns_response = personalize.list_campaigns(
        solutionArn=solution_arn,
        maxResults=100
    )

    demo_campaign = next((campaign for campaign in list_campaigns_response["campaigns"]
                          if campaign["name"] == CAMPAIGN_NAME), False)
    if not demo_campaign:
        create_campaign_response = personalize.create_campaign(
            name=CAMPAIGN_NAME,
            solutionVersionArn=solution_version_arn,
            minProvisionedTPS=2,
        )

        campaign_arn = create_campaign_response['campaignArn']
        logger.info(json.dumps(create_campaign_response, indent=2))
    else:
        campaign_arn = demo_campaign["campaignArn"]
        personalize.update_campaign(
            campaignArn=campaign_arn,
            solutionVersionArn=solution_version_arn,
            minProvisionedTPS=2
        )

    status = None
    max_time = time.time() + 2*60*60  # 2 hours
    while time.time() < max_time:
        describe_campaign_response = personalize.describe_campaign(
            campaignArn=campaign_arn
        )
        status = describe_campaign_response["campaign"]["status"]
        print("Campaign: {}".format(status))

        if status == "ACTIVE" or status == "CREATE FAILED":
            break

        time.sleep(60)

    if status == "ACTIVE":
        return campaign_arn
    if status == "CREATE FAILED":
        raise AirflowFailException(
            f"Campaign create/update failed")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['yi.ai@afox.mobi'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ml-pipeline',
    default_args=default_args,
    concurrency=1,
    description='A simple ML data pipeline DAG',
    schedule_interval='@daily',
)

t_export_bq_to_s3 = PythonOperator(task_id='export_bq_to_s3',
                                   python_callable=bq_to_s3,
                                   dag=dag,
                                   retries=1
                                   )

check_s3_for_key = S3KeySensor(
    task_id='check_s3_for_key',
    bucket_key=OUTPUT_FILE_KEY,
    wildcard_match=True,
    bucket_name=BUCKET_NAME,
    s3_conn_id='aws_default',
    timeout=20,
    poke_interval=5,
    dag=dag
)

t_check_dataset_group = BranchPythonOperator(
    task_id='check_dataset_group',
    provide_context=True,
    python_callable=check_dataset_group,
    retries=1,
    dag=dag,
)

t_init_personalize = DummyOperator(
    task_id="init_personalize",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

t_create_dataset_group = PythonOperator(
    task_id='create_dataset_group',
    provide_context=True,
    python_callable=create_dataset_group,
    retries=1,
    dag=dag,
)

t_create_schema = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    retries=1,
    dag=dag,
)

t_put_bucket_policies = PythonOperator(
    task_id='put_bucket_policies',
    python_callable=put_bucket_policies,
    retries=1,
    dag=dag,
)

t_create_iam_role = PythonOperator(
    task_id='create_iam_role',
    provide_context=True,
    python_callable=create_iam_role,
    retries=1,
    dag=dag,
)

t_create_dataset_type = PythonOperator(
    task_id='create_dataset_type',
    provide_context=True,
    python_callable=create_dataset_type,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    retries=1,
    dag=dag,
)

t_create_import_dataset_job = PythonOperator(
    task_id='import_dataset',
    provide_context=True,
    python_callable=import_dataset,
    retries=1,
    dag=dag,
)

t_skip_init_personalize = DummyOperator(
    task_id="skip_init_personalize",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

t_init_personalize_done = DummyOperator(
    task_id="init_personalize_done",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)


t_update_solution = PythonOperator(
    task_id='update_solution',
    provide_context=True,
    python_callable=update_solution,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    retries=1,
    dag=dag,
)

t_update_campaign = PythonOperator(
    task_id='update_campaign',
    provide_context=True,
    python_callable=update_campaign,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    retries=1,
    dag=dag,
)

t_export_bq_to_s3 >> check_s3_for_key >> t_check_dataset_group
t_check_dataset_group >> t_init_personalize
t_check_dataset_group >> t_skip_init_personalize >> t_init_personalize_done
t_init_personalize >> [
    t_create_dataset_group,
    t_create_schema,
    t_put_bucket_policies,
    t_create_iam_role
] >> t_create_dataset_type
t_create_dataset_type >> t_init_personalize_done
t_init_personalize_done >> t_create_import_dataset_job >> t_update_solution
t_update_solution >> t_update_campaign
