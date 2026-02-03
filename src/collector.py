from itertools import product
import pandas as pd
from typing import List, Dict, Any, Optional
from src.meta_client import get_demo, get_day_totals_by_media_product, get_time_series, get_follows_and_unfollows_by_day
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

def collect_demographics(metrics, timeframes, dimensions, page_token):
    rows = []

    for metric, timeframe, dimension in product(metrics, timeframes, dimensions):
        data = get_demo(metric, timeframe, dimension, page_token)
        for r in data:
            rows.append({
                "metric" : metric,
                "timeframe": timeframe,
                "dimension": dimension,
                "dimension_value": r["dimension_value"],
                "value": r["value"],
            })

    return pd.DataFrame(rows)

# metrics could be: reach, follower_count, website_clicks, profile_views, 
# online_followers, accounts_engaged, total_interactions, likes, comments, 
# shares, saves, replies, engaged_audience_demographics, reached_audience_demographics, 
# follower_demographics, follows_and_unfollows, profile_links_taps, views, threads_likes, 
# threads_replies, reposts, quotes, threads_followers, threads_follower_demographics, 
# content_views, threads_views, threads_clicks, threads_reposts

# breakdown could be: city, country, age and gender
# timeframe could be: last_14_days*, last_30_days*, last_90_days*, prev_month, this_month, this_week

DEFAULT_MEDIA_PRODUCT_METRICS = [
    "reach",
    "likes",
    "shares",
    "comments",
    "saves",
    "views",
    "total_interactions",
]

def collect_day_media_product(
    since: Optional[int],
    until: Optional[int],
    page_token: str,
    metrics: Optional[List[str]] = None,
    extracted_at: Optional[str] = None,  # ISO string
) -> pd.DataFrame:
    """
    Coleta métricas diárias (period=day) agregadas por media_product_type
    para um intervalo definido por since/until (UNIX timestamps).

    Se metrics=None, usa o pacote padrão:
    reach, likes, shares, comments, saves, views, total_interactions
    """

    tz = ZoneInfo("America/Sao_Paulo")
    if extracted_at is None:
        extracted_at = datetime.now(tz).isoformat()

    metrics_to_collect = metrics or DEFAULT_MEDIA_PRODUCT_METRICS

    rows = []

    for metric in metrics_to_collect:
        data = get_day_totals_by_media_product(
            metric_name=metric,
            since=since,
            until=until,
            page_token=page_token,
        )

        for r in data:
            rows.append({
                "metric": metric,
                "period": "day",
                "metric_type": "total_value",
                "breakdown": "media_product_type",
                "extracted_at": extracted_at,
                "since": since,
                "until": until,
                "dimension_value": r.get("media_product_type"),  # ex: POST, REEL, STORY, AD...
                "value": r.get("value"),
            })

    return pd.DataFrame(rows)

DEFAULT_TIME_SERIES_METRICS = [
    "reach",
    # currently this is the only time serie metric availabe

]

def collect_time_series_last_n_days(
    n_days: int,
    page_token: str,
    metrics: Optional[List[str]] = None,
    extracted_at: Optional[str] = None,
    tz_name: str = "America/Sao_Paulo",
) -> pd.DataFrame:
    """
    Coleta time series diária para os últimos n dias (intervalo fechado),
    retornando DF com uma linha por (metric, end_time).
    """
    tz = ZoneInfo(tz_name)
    if extracted_at is None:
        extracted_at = datetime.now(tz).isoformat()

    metrics_to_collect = metrics or DEFAULT_TIME_SERIES_METRICS

    # janela: últimos n dias até agora (rolling), mas com since/until explícitos
    now = datetime.now(tz)
    since_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)  # hoje 00:00
    since_dt = since_dt - pd.Timedelta(days=n_days)
    until_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)  # hoje 00:00

    since_ts = int(since_dt.timestamp())
    until_ts = int(until_dt.timestamp())

    rows = get_time_series(
        metrics=metrics_to_collect,
        since=since_ts,
        until=until_ts,
        page_token=page_token,
        period="day",
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["metric", "metric_date", "end_time", "value", "since", "until", "extracted_at"])

    # end_time vem ISO; vamos criar metric_date (YYYY-MM-DD) para BI
    df["metric_date"] = pd.to_datetime(df["end_time"], errors="coerce").dt.date.astype("string")

    df["since"] = since_ts
    df["until"] = until_ts
    df["extracted_at"] = extracted_at

    # ordena e padroniza colunas
    df = df[["metric", "metric_date", "end_time", "value", "since", "until", "extracted_at"]].sort_values(["metric", "metric_date"])
    return df


def _day_window_ts(d: date, tz_name: str = "America/Sao_Paulo") -> tuple[int, int]:
    tz = ZoneInfo(tz_name)
    since_dt = datetime.combine(d, time(0, 0, 0), tzinfo=tz)
    until_dt = since_dt + timedelta(days=1)
    return int(since_dt.timestamp()), int(until_dt.timestamp())


def collect_follows_unfollows_yesterday(
    page_token: str,
    extracted_at=None,  # pd.Timestamp utc recomendado
    tz_name: str = "America/Sao_Paulo",
) -> pd.DataFrame:
    """
    Coleta follows_and_unfollows (breakdown=follow_type) apenas para o DIA ANTERIOR (ontem),
    retornando um DataFrame pronto pro BigQuery.
    """
    tz = ZoneInfo(tz_name)
    if extracted_at is None:
        extracted_at = pd.Timestamp.utcnow()

    yesterday = datetime.now(tz).date() - timedelta(days=1)
    since_ts, until_ts = _day_window_ts(yesterday, tz_name=tz_name)

    data = get_follows_and_unfollows_by_day(
        since=since_ts,
        until=until_ts,
        page_token=page_token,
    )

    rows: List[Dict[str, Any]] = []
    for r in data:
        rows.append({
            "metric_date": yesterday.isoformat(),
            "metric": "follows_and_unfollows",
            "follow_type": r.get("follow_type"),  # FOLLOWER / NON_FOLLOWER / UNKNOWN
            "value": r.get("value"),
            "since": since_ts,
            "until": until_ts,
            "extracted_at": extracted_at,
        })

    return pd.DataFrame(rows).reindex(columns=[
        "metric_date", "metric", "follow_type", "value", "since", "until", "extracted_at"
    ])