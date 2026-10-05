# import os
# from datetime import datetime

# from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
# from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# profile_config = ProfileConfig(
#     profile_name="default",
#     target_name="dev",
#     profile_mapping=SnowflakeUserPasswordProfileMapping(
#         conn_id="snowflake_conn",
#         profile_args={"database": "dbt_db", "schema": "dbt_schema"},
#     )
# )

# dbt_snowflake_dag = DbtDag(
#     project_config=ProjectConfig("dbt-dag/dags/dbt/data_pipeline",),
#     operator_args={"install_deps": True},
#     profile_config=profile_config,
#     execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
#     schedule="@daily",
#     start_date=datetime(2023, 5, 17),
#     catchup=False,
#     dag_id="dbt_dag",
# )

from pathlib import Path
import os
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DAG_DIR = Path(__file__).parent  # = /usr/local/airflow/dags/

profile_config = ProfileConfig(
    profile_name="data_pipeline",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DAG_DIR / "dbt" / "data_pipeline",  # ← exact path
    ),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    schedule="@daily",
    start_date=datetime(2023, 5, 17),
    catchup=False,
    dag_id="dbt_dag",
)