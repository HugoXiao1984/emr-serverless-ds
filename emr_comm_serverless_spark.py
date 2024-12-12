import gzip
import os
from string import Template
import time
import boto3
from datetime import datetime
from pydolphinscheduler.tasks.python import Python
from pydolphinscheduler.core.engine import TaskResult

# this py is for submit job to emr on ec2 and emr serverless 

# EMRResult 类用于存储 EMR 作业的运行 ID 和状态
class EMRResult:
    def __init__(self,job_run_id,status):
        self.job_run_id=job_run_id
        self.status=status

# Session 类用于管理 EMR Serverless 作业的提交和执行
class Session:
    def __init__(self,
                 application_id='', # 设置 EMR Serverless 应用 ID
                 job_role='arn:aws:iam::******:role/AmazonEMR-ExecutionRole-1694412227712',
                 dolphin_s3_path='s3://*****/dolphinscheduler/ec2-user/resources/',
                 logs_s3_path='s3://aws-logs-****-ap-southeast-1/elasticmapreduce/',
                 tempfile_s3_path='s3://****/tmp/',
                 python_venv_s3_path='s3://****/python/pyspark_venv.tar.gz',
                 spark_conf='--conf spark.executor.cores=4 --conf spark.executor.memory=16g --conf spark.driver.cores=4 --conf spark.driver.memory=16g'
                 ):

        self.application_id = application_id

        self.region='us-east-1'
        self.job_role = job_role
        self.dolphin_s3_path = dolphin_s3_path
        self.logs_s3_path=logs_s3_path
        self.tempfile_s3_path=tempfile_s3_path
        self.spark_conf=spark_conf
        self.python_venv_s3_path=python_venv_s3_path

        self.client_serverless = boto3.client('emr-serverless', region_name=self.region)

        # 如果未设置 application_id,则查询当前第一个 active 的 EMR Serverless 应用的 ID
        if self.application_id == '':
            self.application_id=self.getDefaultApplicaitonId()

        self.session=EmrServerlessSession(
            region=self.region,
            application_id=self.application_id,
            job_role=self.job_role,
            dolphin_s3_path=self.dolphin_s3_path,
            logs_s3_path=self.logs_s3_path,
            tempfile_s3_path=self.tempfile_s3_path,
            python_venv_s3_path=self.python_venv_s3_path,
            spark_conf=self.spark_conf
        )

        self.initTemplateSQLFile()

    # 提交 SQL 作业
    def submit_sql(self,jobname, sql):
        result= self.session.submit_sql(jobname,sql)
        if result.status == "FAILED" :
            raise Exception("ERROR：任务失败")

    # 提交文件作业
    def submit_file(self,jobname, filename):
        result=  self.session.submit_file(jobname,filename)
        if result.status == "FAILED":
            raise Exception("ERROR：任务失败")

    # 获取默认的 EMR Serverless 应用 ID，找了第一个Application，是支持Spark的
    def getDefaultApplicaitonId(self):
        emr_applications = self.client_serverless.list_applications()
        spark_applications = [app for app in emr_applications['applications'] if app['type'] == 'Spark']
        if spark_applications:
            app_id = spark_applications[0]['id']
            print(f"选择默认的应用ID:{app_id}")
            return app_id
        else:
            raise Exception("没有找到活跃的 EMR Serverless 应用")

    # 初始化 SQL 模板文件
    def initTemplateSQLFile(self):
        with open('sql_template.py', 'w') as f:
            f.write('''
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.enableHiveSupport()
    .appName("Python Spark SQL basic example")
    .getOrCreate()
)

df = spark.sql("$query")
df.show()
        ''')

# EMR Serverless 作业提交类
class EmrServerlessSession:
    def __init__(self,
                 region,
                 application_id, #若是 serverless, 则设置 应用的 ID；若不设置，则自动其第一个active的 app 
                 job_role,
                 dolphin_s3_path,
                 logs_s3_path,
                 tempfile_s3_path,
                 python_venv_s3_path,
                 spark_conf
                 ):
        self.s3_client = boto3.client("s3")
        self.region=region
        self.client = boto3.client('emr-serverless', region_name=self.region)
        self.application_id = application_id

        self.job_role = job_role
        self.dolphin_s3_path = dolphin_s3_path
        self.logs_s3_path=logs_s3_path
        self.tempfile_s3_path=tempfile_s3_path
        self.python_venv_s3_path=python_venv_s3_path
        self.spark_conf=spark_conf

    # 提交 SQL 作业到 EMR Serverless
    def submit_sql(self,jobname, sql):
        # 将 SQL 写入临时文件
        print(f"RUN SQL:{sql}")
        self.python_venv_conf=''
        with open(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql_template.py")
        ) as f:
            query_file = Template(f.read()).substitute(query=sql.replace('"', '\\"'))

            script_bucket = self.tempfile_s3_path.split('/')[2]
            script_key = '/'.join(self.tempfile_s3_path.split('/')[3:])

            current_time = datetime.now().strftime("%Y%m%d%H%M%S")
            script_key = script_key+"sql_template_"+current_time+".py"
            self.s3_client.put_object(
                Body=query_file, Bucket=script_bucket, Key=script_key
            )

            script_file=f"s3://{script_bucket}/{script_key}"
            result= self._submit_job_emr(jobname, script_file)

            #delete the temp file
            self.s3_client.delete_object(
                Bucket=script_bucket, Key=script_key
            )
            return result
    def submit_file(self,jobname, filename):  #serverless
        # temporary file for the sql parameter
        print(f"RUN Script :{filename}")

        self.python_venv_conf=''
        if self.python_venv_s3_path and self.python_venv_s3_path != '':
            self.python_venv_conf = f"--conf spark.archives={self.python_venv_s3_path}#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"


        script_file=f"{self.dolphin_s3_path}{filename}"
        result= self._submit_job_emr(jobname, script_file)

        return result


    def _submit_job_emr(self, name, script_file):#serverless
        job_driver = {
            "sparkSubmit": {
                "entryPoint": f"{script_file}",
                "sparkSubmitParameters": f"{self.spark_conf} --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory {self.python_venv_conf}",
            }
        }
        print(f"job_driver:{job_driver}")
        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.job_role,
            name=name,
            jobDriver=job_driver,
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": self.logs_s3_path,
                    }
                }
            },
        )

        job_run_id = response.get("jobRunId")
        print(f"Emr Serverless Job submitted, job id: {job_run_id}")

        job_done = False
        status="PENDING"
        while not job_done:
            status = self.get_job_run(job_run_id).get("state")
            print(f"current status:{status}")
            job_done = status in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]

            time.sleep(10)

        if status == "FAILED":
            self.print_driver_log(job_run_id,log_type="stderr")
            self.print_driver_log(job_run_id,log_type="stdout")
            raise Exception(f"EMR Serverless job failed:{job_run_id}")
        return EMRResult(job_run_id,status)


    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")

    def print_driver_log(self, job_run_id: str, log_type: str = "stderr") -> str:


        s3_client = boto3.client("s3")
        logs_location = f"{self.logs_s3_path}applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz"
        logs_bucket = logs_location.split('/')[2]
        logs_key = '/'.join(logs_location.split('/')[3:])
        print(f"Fetching {log_type} from {logs_location}")
        try:
            response = s3_client.get_object(Bucket=logs_bucket, Key=logs_key)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except Exception:
            file_content = ""
        print(file_content)
