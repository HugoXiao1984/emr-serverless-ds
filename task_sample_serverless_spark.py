from emr_comm_serverless_spark import Session
from pydolphinscheduler.tasks.python import Python
from pydolphinscheduler.core.engine import TaskResult

def emr_serverless_task():
    try:
        # 创建 EMR Serverless Session
        session_emrserverless = Session(
            application_id='00fokrodkuci2g09',
            logs_s3_path='s3://emr-spark-hugo/logs/',
            spark_conf='--conf spark.executor.cores=8 --conf spark.executor.memory=32g --conf spark.driver.cores=4 --conf spark.driver.memory=16g --jars s3://emr-spark-hugo/jars/config-1.4.1.jar',
            job_role='arn:aws:iam::535002884571:role/service-role/AmazonEMR-ExecutionRole-1733971980623',
            dolphin_s3_path='s3://emr-spark-hugo/dolphine/',
            tempfile_s3_path='s3://emr-spark-hugo/tempfile/',
            python_venv_s3_path='s3://emr-spark-hugo/python_venv/custom_venv.tar.gz'
        )

        # 提交 SQL 语句
        sql_result = session_emrserverless.submit_sql("sql-task", "SELECT * FROM xxtable LIMIT 10")

        # 提交脚本文件
        script_result = session_emrserverless.submit_file("script-task", "spark-test.py")

        # 检查任务执行结果
        if sql_result.success and script_result.success:
            return TaskResult.success("EMR Serverless tasks completed successfully.")
        else:
            failed_tasks = []
            if not sql_result.success:
                failed_tasks.append("SQL task")
            if not script_result.success:
                failed_tasks.append("Script task")
            return TaskResult.failure(f"EMR Serverless tasks failed: {', '.join(failed_tasks)}")

    except Exception as e:
        return TaskResult.failure(f"EMR Serverless task failed with error: {str(e)}")

# 创建 Python 任务
emr_task = Python(
    name="EMR_Serverless_Task",
    definition=emr_serverless_task
)

# 如果需要设置任务的其他属性，可以在这里添加
# 例如：emr_task.set_task_priority(2)

# 将任务添加到工作流中
# 注意：这部分通常在工作流定义文件中完成，而不是在任务脚本中
# from pydolphinscheduler.core.workflow import Workflow
# with Workflow(name="EMR_Workflow", schedule="0 0 * * *") as workflow:
#     emr_task
# workflow.submit()
