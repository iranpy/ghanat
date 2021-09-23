import subprocess
import re
from typing import Iterator, Any, Optional

from yarn_api_client.resource_manager import ResourceManager

from ghanat.tasks.executors.base import Executor, SparkRunnerConf


class YarnExecutor(Executor):
    def __init__(
        self,
        hadoop_conf_path=None,
        spark_conf: SparkRunnerConf=None
        ) -> None:
        self.request = ResourceManager()
        self.spark_conf = spark_conf
        self._yarn_application_id: Optional[str] = None
        # self._deploy_mode = deploy_mode or 'cluster'
        self._spark_driver_status: Optional[str] = None
        self.hadoop_conf_path = hadoop_conf_path

    def _process_spark_submit_log(self, itr: Iterator[Any]):
        for line in itr:
            line = line.strip()
            print(line)
            if self.spark_conf._deploy_mode == 'cluster':
                match = re.search('(application[0-9_]+)', line)
                if match:
                    self._yarn_application_id = match.groups()[0]
                    print("Identified spark driver id: %s", self._yarn_application_id)

    
    def spark_submit(self, func):
        spark_submit_cmd = self.spark_conf.submit_command(func)

        submit_result = subprocess.Popen(
            spark_submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
            )

        self._process_spark_submit_log(iter(submit_result.stdout))  # type: ignore
        returncode = submit_result.wait()
        if returncode:
            raise
        self._spark_driver_status = "SUBMITTED"

    
    def application_kill(self, application_id):
        return self.request.cluster_application_kill(application_id).data

    def application_state_tracking(self, application_id):
        return self.request.cluster_application_state(application_id).data


yarn_exec = YarnExecutor(
        spark_conf= SparkRunnerConf(
        executor_cores='4',
        executor_memory='4',
        driver_memory='4',
        total_executor_cores='8',
        queue='queue2',
        deploy_mode='cluster',
        extra_conf={
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON":"./environment/bin/python" ,
            "spark.yarn.dist.archives":"/home/ali/Projects/ghanat/ghanat-env.tar.gz#environment"
            }
))
# print('out', yarn_exec.application_state_tracking('application_1615172329732_0604'))
