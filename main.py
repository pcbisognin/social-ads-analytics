import pandas as pd
from src.meta_client import get_page_token
from src.collector import (
    collect_demographics,
    collect_day_media_product,
    collect_follows_unfollows_yesterday,
)
from src.loaders.bigquery_loader import load_to_bigquery

# --- DEMOGRAPHICS ---
DEMOGRAPHICS_METRICS = [
    "follower_demographics",
    "engaged_audience_demographics",
    "reached_audience_demographics",
]
TIMEFRAMES = ["this_week", "this_month"]
DIMENSIONS = ["city", "age", "gender"]

# --- MEDIA PRODUCT (DAY) ---
MEDIA_PRODUCT_METRICS = [
    "reach",
    "likes",
    "shares",
    "comments",
    "saves",
    "views",
    "total_interactions",
]


def main():
    page_token = get_page_token()
    extracted_at = pd.Timestamp.now(tz="UTC")

    # 1) Demographics
    df_demo = collect_demographics(
        metrics=DEMOGRAPHICS_METRICS,
        timeframes=TIMEFRAMES,
        dimensions=DIMENSIONS,
        page_token=page_token,
    )
    df_demo["extracted_at"] = extracted_at

    if df_demo.empty:
        raise RuntimeError("Demographics veio vazio. Abortando para evitar carga ruim.")

    print("Preview demographics:")
    print(df_demo.head(10))

    load_to_bigquery(
        df=df_demo,
        project_id="cannele-marketing",
        dataset="marketing",
        table="fact_instagram_demographics",
    )
    print("Demographics enviados com sucesso para o BigQuery!")

    # 2) Media Product - last 24h
    df_media = collect_day_media_product(
        since=None,
        until=None,
        page_token=page_token,
        metrics=MEDIA_PRODUCT_METRICS,
        extracted_at=extracted_at,
    )

    if df_media.empty:
        raise RuntimeError("Media product veio vazio. Abortando para evitar carga ruim.")

    print("Preview media_product (last 24h):")
    print(df_media.head(10))

    load_to_bigquery(
        df=df_media,
        project_id="cannele-marketing",
        dataset="marketing",
        table="fact_instagram_media_product_24h",
    )
    print("Media product enviados com sucesso para o BigQuery!")

    # 3) Follows & Unfollows - yesterday
    df_follows = collect_follows_unfollows_yesterday(
        page_token=page_token,
        extracted_at=extracted_at,
    )

    if df_follows.empty:
        raise RuntimeError("Follows/unfollows veio vazio. Abortando para evitar carga ruim.")

    print("Preview follows/unfollows:")
    print(df_follows.head())

    load_to_bigquery(
        df=df_follows,
        project_id="cannele-marketing",
        dataset="marketing",
        table="fact_instagram_follows_unfollows_day",
    )
    print("Follows & unfollows enviados com sucesso para o BigQuery!")

    print("✅ Pipeline finalizado com sucesso!")


if __name__ == "__main__":
    main()
