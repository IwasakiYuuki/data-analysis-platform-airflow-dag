from airflow.models.dag import DAG
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.operators.python import PythonOperator


def read_hdfs_file(filename: str, hdfs_conn_id: str):
    """
    Read a file from HDFS using the WebHDFSHook.
    :param file_path: The path to the file in HDFS.
    :param hdfs_conn_id: The connection ID for the WebHDFS connection.
    """
    hook = WebHDFSHook(webhdfs_conn_id=hdfs_conn_id)
    try:
        content = hook.read_file(filename=filename)
        print(f"File contents:")
        print(content.decode('utf-8'))
    except Exception as e:
        print(f"A error occurs: {e}")


with DAG(
    "webhdfs_example",
    schedule=None,
    catchup=False,
    tags=["hdfs", "example"],
) as dag:

    filename = "/tmp/test.txt"
    hdfs_connection_id = "webhdfs_default"

    read_file_task = PythonOperator(
        task_id="read_hdfs_file",
        python_callable=read_hdfs_file,
        op_kwargs={
            "filename": filename,
            "hdfs_conn_id": hdfs_connection_id,
        },
    )
