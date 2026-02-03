import pandas_gbq

def load_to_bigquery(df, project_id, dataset, table):
    pandas_gbq.to_gbq(
        df,
        f"{dataset}.{table}",
        project_id=project_id,
        if_exists="append",
    )

