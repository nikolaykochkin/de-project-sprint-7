import os


def init_environ() -> None:
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['JAVA_HOME'] = '/usr'
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
    os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'
