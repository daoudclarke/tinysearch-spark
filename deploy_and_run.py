import os

import boto3

# S3_BUCKET = os.environ['S3_BUCKET']
AWS_KEY = os.environ['AWS_KEY']
AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']

# S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)


def run():
    connection = boto3.client(
        'emr',
        region_name='us-east-1',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )

    cluster_id = connection.run_job_flow(
        Name='test_emr_job_boto3',
        LogUri='s3://tinysearch/pyspark-logs',
        ReleaseLabel='emr-5.18.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm1.xlarge',
                'InstanceCount': 1,
            },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm1.xlarge',
                'InstanceCount': 2,
                }
            ],
            'Ec2KeyName': 'Dkan-key-supun',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-04a2978b7fc0b4606',
        },
        Steps=[
            {
                'Name': 'file-copy-step',   
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                'Jar': 's3://kula-emr-test/jars/CopyFilesS3-1.0-SNAPSHOT-jar-with-dependencies.jar',
                    'Args': ['test.xml', 'kula-emr-test', 'kula-emr-test-2']
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Tags=[
            {
                'Key': 'tag_name_1',
                'Value': 'tab_value_1',
            },
            {
                'Key': 'tag_name_2',
            'Value': 'tag_value_2',
            },
        ],
    )
    

if __name__ == "__main__":
    run()
