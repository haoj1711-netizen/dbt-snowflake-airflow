FROM astrocrpublic.azurecr.io/runtime:3.2-4

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate