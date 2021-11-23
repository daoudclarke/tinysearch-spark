import os

import boto3

S3_BUCKET = os.environ['S3_BUCKET']
S3_KEY = os.environ['S3_KEY']
S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)


def run():
    client = boto3.client('emr', region_name='us-east-1')

    # upload file to an S3 bucket
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file("extract.py", S3_BUCKET, S3_KEY)

    response = client.run_job_flow(
        Name="My Spark Cluster",
        ReleaseLabel='emr-4.6.0',
        Instances={
            'MasterInstanceType': 'm4.xlarge',
            'SlaveInstanceType': 'm4.xlarge',
            'InstanceCount': 4,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Applications=[
            {
                'Name': 'Spark'
            }
        ],
        BootstrapActions=[
            {
                'Name': 'Maximize Spark Default Config',
                'ScriptBootstrapAction': {
                    'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config',
                }
            },
        ],
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'setup - copy files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', S3_URI, '/home/hadoop/']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/main.py']
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )
    print("Response", response)
