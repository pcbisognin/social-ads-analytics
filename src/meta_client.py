import os
from typing import List, Dict, Any, Optional
import requests
from dotenv import load_dotenv

load_dotenv()


GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v24.0")
BASE_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"

META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")

if not META_ACCESS_TOKEN or not IG_USER_ID:
    raise ValueError("Faltou META_ACCESS_TOKEN ou IG_USER_ID no .env")

def get_pages(meta_access_token: str = META_ACCESS_TOKEN) -> Dict[str, Any]:
    url = f"{BASE_URL}/me/accounts"
    params = {
        "fields": "id,name,access_token,instagram_business_account",
        "access_token": meta_access_token,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def get_page_access_token_for_ig(
    pages_json: Dict[str, Any],
    ig_user_id: str = IG_USER_ID
) -> str:
    for page in pages_json.get("data", []):
        ig = page.get("instagram_business_account")
        if ig and ig.get("id") == ig_user_id:
            return page["access_token"]
    raise RuntimeError("Não encontrei Page Access Token para esse IG_USER_ID.")

def get_page_token(meta_access_token: str = META_ACCESS_TOKEN) -> str:
    pages = get_pages(meta_access_token)
    return get_page_access_token_for_ig(pages, IG_USER_ID)

def get_demo(
    metric_name: str,
    timeframe: str,
    breakdown: str,
    page_token: str,
) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/{IG_USER_ID}/insights"
    params = {
        "metric": metric_name,
        "metric_type": "total_value",
        "period": "lifetime",
        "timeframe": timeframe,
        "breakdown": breakdown,
        "access_token": page_token,
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    raw = r.json()
    rows: List[Dict[str, Any]] = []

    for item in raw.get("data", []):
        for b in item.get("total_value", {}).get("breakdowns", []):
            for res in b.get("results", []):
                # alguns endpoints podem retornar lista vazia; defensivo:
                dim_vals = res.get("dimension_values") or [None]
                rows.append({
                    "dimension_value": dim_vals[0],
                    "value": res.get("value"),
                })

    return rows


def get_day_totals_by_media_product(
    metric_name: str,
    since: Optional[int],
    until: Optional[int],
    page_token: str,
) -> List[Dict[str, Any]]:
    """
    Coleta métricas diárias agregadas por media_product_type
    (ex: FEED, REELS, STORY).
    """

    url = f"{BASE_URL}/{IG_USER_ID}/insights"

    params = {
        "metric": metric_name,
        "metric_type": "total_value",
        "period": "day",
        "breakdown": "media_product_type",
        "access_token": page_token,
    }

    # since / until optinal but recommended. Will return values for the last 24h if absent
    if since:
        params["since"] = since
    if until:
        params["until"] = until

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    raw = r.json()
    rows: List[Dict[str, Any]] = []

    for item in raw.get("data", []):
        total_value = item.get("total_value", {})
        for b in total_value.get("breakdowns", []):
            dimension_keys = b.get("dimension_keys", [])
            for res in b.get("results", []):
                dim_vals = res.get("dimension_values") or []
                rows.append({
                    "media_product_type": dim_vals[0] if dim_vals else None,
                    "value": res.get("value"),
                })

    return rows

def get_time_series(
    metrics: List[str],
    since: Optional[int],
    until: Optional[int],
    page_token: str,
    period: str = "day",
) -> List[Dict[str, Any]]:
    """
    Busca métricas como time_series (por período), retornando lista de linhas:
    [
      {"metric": "reach", "end_time": "...", "value": 123},
      ...
    ]

    """
    url = f"{BASE_URL}/{IG_USER_ID}/insights"

    params = {
        "metric": ",".join(metrics),
        "metric_type": "time_series",
        "period": period,
        "access_token": page_token,
    }
    if since:
        params["since"] = since
    if until:
        params["until"] = until

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    raw = r.json()
    rows: List[Dict[str, Any]] = []

    for item in raw.get("data", []):
        metric_name = item.get("name")

        # time_series costuma vir em item["values"]
        for v in item.get("values", []) or []:
            rows.append({
                "metric": metric_name,
                "end_time": v.get("end_time"),
                "value": v.get("value"),
            })

    return rows

def get_follows_and_unfollows_by_day(
    since: int,
    until: int,
    page_token: str,
) -> List[Dict[str, Any]]:
    """
    Busca follows_and_unfollows no intervalo since/until (UNIX timestamps),
    com breakdown por follow_type.

    Retorna linhas no formato:
    [
      {"follow_type": "FOLLOWER", "value": 10},
      {"follow_type": "NON_FOLLOWER", "value": 2},
      {"follow_type": "UNKNOWN", "value": 1},
    ]
    """
    url = f"{BASE_URL}/{IG_USER_ID}/insights"
    params = {
        "metric": "follows_and_unfollows",
        "metric_type": "total_value",
        "period": "day",
        "breakdown": "follow_type",
        "since": since,
        "until": until,
        "access_token": page_token,
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    raw = r.json()

    rows: List[Dict[str, Any]] = []

    for item in raw.get("data", []):
        for b in item.get("total_value", {}).get("breakdowns", []):
            for res in b.get("results", []):
                dim_vals = res.get("dimension_values") or [None]
                rows.append({
                    "follow_type": dim_vals[0],   # FOLLOWER / NON_FOLLOWER / UNKNOWN
                    "value": res.get("value"),
                })

    return rows

def get_followers_count(meta_access_token: str = META_ACCESS_TOKEN) -> int:
    """
    Retorna o followers_count atual do Instagram Business Account (snapshot).
    Usa User Access Token (META_ACCESS_TOKEN), não Page token.
    """
    url = f"{BASE_URL}/{IG_USER_ID}"
    params = {
        "fields": "followers_count",
        "access_token": meta_access_token,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    if "followers_count" not in data:
        raise RuntimeError(f"followers_count não veio na resposta: {data}")

    return int(data["followers_count"])

def get_ad_account_info(ad_account_id: str = META_AD_ACCOUNT_ID, meta_access_token: str = META_ACCESS_TOKEN) -> Dict[str, Any]:
    if not ad_account_id:
        raise ValueError("Faltou META_AD_ACCOUNT_ID (ex: act_180987220455255).")

    url = f"{BASE_URL}/{ad_account_id}"
    params = {
        "fields": "id,name,account_status,currency,timezone_name",
        "access_token": meta_access_token,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def get_ads_insights_daily(
    since: str,  # "YYYY-MM-DD"
    until: str,  # "YYYY-MM-DD"
    level: str = "account",
    fields: Optional[List[str]] = None,
    ad_account_id: str = META_AD_ACCOUNT_ID,
    meta_access_token: str = META_ACCESS_TOKEN,
) -> List[Dict[str, Any]]:
    """
    Coleta insights diários (time_increment=1) no intervalo [since, until].
    Observação: na Marketing API, 'until' costuma ser tratado como fim do intervalo (dependendo do endpoint),
    mas na prática o mais seguro é passar o intervalo do "dia" que você quer (ex: yesterday -> since=yday, until=today).
    """
    if not ad_account_id:
        raise ValueError("Faltou META_AD_ACCOUNT_ID (ex: act_180987220455255).")

    url = f"{BASE_URL}/{ad_account_id}/insights"

    fields = fields or ["date_start", "date_stop", "spend", "impressions", "clicks"]

    params = {
        "fields": ",".join(fields),
        "level": level,
        "time_increment": 1,
        "since": since,
        "until": until,
        "access_token": meta_access_token,
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    raw = r.json()

    return raw.get("data", []) or []


