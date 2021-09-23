import subprocess
import re
from typing import Iterator, Any, Optional

from ghanat.tasks.executors.base import Executor, SparkRunnerConf
from ghanat.tasks.exceptions import InvalidValueException



class LocalExecutor(Executor):
    def __init__(
        self,
        hadoop_conf_path=None,
        spark_conf: SparkRunnerConf=None
        ) -> None:
        self.spark_conf = spark_conf
        
        if self.spark_conf._deploy_mode == 'cluster':
            raise InvalidValueException('Cluster deploy mode is currently not supported for standalone clusters.')

        self._yarn_application_id: Optional[str] = None
        self._spark_driver_status: Optional[str] = None
        self.hadoop_conf_path = hadoop_conf_path
        
    def _process_spark_submit_log(self, itr: Iterator[Any]):
        for line in itr:
            line = line.strip()
            match_driver_id = re.search(r'(app-[0-9\-]+)', line)
            if match_driver_id:
                self._driver_id = match_driver_id.groups()[0]
                print("Identified spark driver id: {}".format(self._driver_id))
                
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
        #TODO
        return
    
    def application_state_tracking(self, application_id):
        #TODO
        return

local_exec = LocalExecutor(
        spark_conf= SparkRunnerConf(
        executor_cores='4',
        executor_memory='4',
        driver_memory='2',
        master='spark://ali:7077',
))

print('out', local_exec.spark_submit('/home/ali/Projects/ghanat/ghanat/tasks/executors/test.py'))
