from __future__ import annotations

import pendulum
import time

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


MACHINES = {
    "machine1": {"mac": "{{ var.value.hadoop_datanode1_mac }}", "ssh_conn_id": "ssh_hadoop-datanode1"},
    "machine2": {"mac": "{{ var.value.hadoop_datanode2_mac }}", "ssh_conn_id": "ssh_hadoop-datanode2"},
    "machine3": {"mac": "{{ var.value.hadoop_datanode3_mac }}", "ssh_conn_id": "ssh_hadoop-datanode3"},
}

HADOOP_RUN_DURATION_SEC = 7200  # 2 hours

with DAG(
    dag_id="hadoop_cluster_on_demand_simple",
    schedule="40 12 * * Mon",
    start_date=pendulum.datetime(2025, 4, 10),
    catchup=False,
    tags=["hadoop", "lan", "ssh", "simple"],
) as dag:
    # Task 1: Wake up machines
    wake_up_tasks = []
    for machine_id, machine_info in MACHINES.items():
        wake_up_tasks.append(
            BashOperator(
                task_id=f"wake_up_{machine_id}",
                bash_command=f"wakeonlan {machine_info['mac']}",
                do_xcom_push=False,
            )
        )

    # Task 2: Run Hadoop Duration (Assuming the daemon is automatically started by systemd)
    def sleep_run_duration(duration_sec):
        print(f"Running Hadoop cluster for {duration_sec} seconds...")
        time.sleep(duration_sec)
        print("Hadoop run duration finished.")

    run_duration = PythonOperator(
        task_id="run_hadoop_duration",
        python_callable=sleep_run_duration,
        op_kwargs={"duration_sec": HADOOP_RUN_DURATION_SEC},
    )

    # Task 3: Shutdown machines (using SSHOperator)
    shutdown_tasks = []
    for machine_id, machine_info in MACHINES.items():
        shutdown_tasks.append(
            SSHOperator(
                task_id=f"shutdown_{machine_id}",
                ssh_conn_id=machine_info['ssh_conn_id'],
                command="sudo shutdown -h now",
                conn_timeout=10,
                cmd_timeout=60,
            )
        )

    # Define dependencies
    wake_up_tasks >> run_duration
    run_duration >> shutdown_tasks
