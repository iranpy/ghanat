import os
from typing import Dict, Any, Optional


class Executor(object):
    pass


class SparkRunnerConf:
    def __init__(
        self,
        spark_home: Optional[str] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        driver_memory: Optional[str] = None,
        num_executors: Optional[int] = None,
        total_executor_cores: Optional[int] = None,
        master: Optional[str] = None,
        deploy_mode: Optional[str] = None,
        queue: Optional[str] = None,
        archive: Optional[str] = None,
        proxy_user: Optional[str] = None,
        extra_conf: Optional[Dict[str, Any]] = None,
        env_params: Optional[Dict[str, Any]] = None,
        verbose: bool = False,
        ) -> None:
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._num_executors = num_executors
        self._total_executor_cores = total_executor_cores
        self._master = master
        self._queue = queue
        self._proxy_user = proxy_user
        self._extra_conf = extra_conf or {}
        self._env_params = env_params
        self._deploy_mode = deploy_mode
        self._archives = archive
        self._verbose = verbose

        if not spark_home:
            spark_home = os.environ['SPARK_HOME']

        self._spark_bin =  [os.path.join(spark_home, 'bin', 'spark-submit')]

    
    def submit_command(self, func):
        connection_cmd = []
        # for key in self._extra_conf:
        #     connection_cmd += ["--conf", f"{key}={str(self._extra_conf[key])}"]
        for key in self._extra_conf:
            connection_cmd += ["--conf", f"{key}={str(self._extra_conf[key])}"]
        if self._archives:
            connection_cmd += ["--archives", self._archives]
        if self._num_executors:
            connection_cmd += ["--num-executors", str(self._num_executors)]
        if self._total_executor_cores:
            connection_cmd += ["--total-executor-cores", str(self._total_executor_cores)]
        if self._executor_cores:
            connection_cmd += ["--executor-cores", str(self._executor_cores)]
        if self._executor_memory:
            connection_cmd += ["--executor-memory", self._executor_memory]
        if self._driver_memory:
            connection_cmd += ["--driver-memory", self._driver_memory]
        if self._master:
            connection_cmd += ["--master", self._master]
        if self._deploy_mode:
            connection_cmd += ["--deploy-mode", self._deploy_mode]
        if self._proxy_user:
            connection_cmd += ["--proxy-user", self._proxy_user]
        if self._verbose:
            connection_cmd += ["--verbose"]
        if self._queue:
            connection_cmd += ["--queue", self._queue]
        # The actual script to execute
        connection_cmd += [func]
        connection_cmd = self._spark_bin + connection_cmd
        return connection_cmd
    
