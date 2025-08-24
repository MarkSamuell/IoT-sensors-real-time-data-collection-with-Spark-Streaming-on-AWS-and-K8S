# Job Configurations for EMR Step Submission
JOB_CONFIGURATIONS = {
    'ui_sevs': {
        'name': 'UI SEVs Processing',
        'action_on_failure': 'CONTINUE',
        'hadoop_jar_step': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', 
                '--master', 'yarn', 
                '--deploy-mode', 'cluster', 
                '--conf', 'spark.yarn.appMasterEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH', 
                '--conf', 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH',
                '--py-files', 's3://elasticmapreduce-uraeusdev/spark_build/streaming_pipeline-0.0.1-py3-none-any.whl',
                's3://elasticmapreduce-uraeusdev/spark_build/spark_runner.py',
                'ui_sevs'
            ]
        }
    },
    'ui_event_logs': {
        'name': 'UI Event Logs Processing',
        'action_on_failure': 'CONTINUE',
        'hadoop_jar_step': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', 
                '--master', 'yarn', 
                '--deploy-mode', 'cluster', 
                '--conf', 'spark.yarn.appMasterEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH', 
                '--conf', 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH',
                '--py-files', 's3://elasticmapreduce-uraeusdev/spark_build/streaming_pipeline-0.0.1-py3-none-any.whl',
                's3://elasticmapreduce-uraeusdev/spark_build/spark_runner.py', 
                'ui_event_logs'
            ]
        }
    },
    'ui_sevs_aggs': {
        'name': 'UI SEVs Aggregations',
        'action_on_failure': 'CONTINUE',
        'hadoop_jar_step': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', 
                '--master', 'yarn', 
                '--deploy-mode', 'cluster',
                '--conf', 'spark.yarn.appMasterEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH', 
                '--conf', 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH',
                '--py-files', 's3://elasticmapreduce-uraeusdev/spark_build/streaming_pipeline-0.0.1-py3-none-any.whl',
                's3://elasticmapreduce-uraeusdev/spark_build/spark_runner.py', 
                'ui_sevs_aggs'
            ]
        }
    },
    'ui_event_logs_aggs': {
        'name': 'UI Event Logs Aggregations',
        'action_on_failure': 'CONTINUE',
        'hadoop_jar_step': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', 
                '--master', 'yarn', 
                '--deploy-mode', 'cluster', 
                '--conf', 'spark.yarn.appMasterEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH', 
                '--conf', 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH',
                '--py-files', 's3://elasticmapreduce-uraeusdev/spark_build/streaming_pipeline-0.0.1-py3-none-any.whl',
                's3://elasticmapreduce-uraeusdev/spark_build/spark_runner.py', 
                'ui_event_logs_aggs'
            ]
        }
    },
    'ice_sevs': {
        'name': 'Iceberg SEVs Processing',
        'action_on_failure': 'CONTINUE',
        'hadoop_jar_step': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', 
                '--master', 'yarn', 
                '--deploy-mode', 'cluster',
                '--conf', 'spark.yarn.appMasterEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH', 
                '--conf', 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH',
                '--py-files', 's3://elasticmapreduce-uraeusdev/spark_build/streaming_pipeline-0.0.1-py3-none-any.whl', 
                's3://elasticmapreduce-uraeusdev/spark_build/spark_runner.py', 
                'ice_sevs'
            ]
        }
    },
    'can_ml_batch': {
        'name': 'CAN ML Batch Processing',
        'action_on_failure': 'CONTINUE',
        'hadoop_jar_step': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', 
                '--master', 'yarn', 
                '--deploy-mode', 'cluster',
                '--conf', 'spark.yarn.appMasterEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH', 
                '--conf', 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python:/mnt/tmp:$PYTHONPATH',
                '--py-files', 's3://elasticmapreduce-uraeusdev/spark_build/streaming_pipeline-0.0.1-py3-none-any.whl', 
                's3://elasticmapreduce-uraeusdev/spark_build/spark_runner.py', 
                'can_ml_batch'
            ]
        }
    }
}