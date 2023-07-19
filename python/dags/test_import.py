from airflow.models.dag import DAG

def test_airflow_import():
    dag = DAG(dag_id="dag_id")
    print(dag.dag_id)
