import os
import csv
import json
import time
import asyncio
import aiohttp
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from aiohttp import ClientTimeout, TCPConnector
from aiolimiter import AsyncLimiter
from asyncio import Semaphore
from tqdm.asyncio import tqdm_asyncio

# ==========================================================
# WHAT THIS IS
# ==========================================================
# The 1NCE daily SIM report — sibling to monogoto_sim_report.py, telenor_sim_report.py
# and flolive_sim_report.py, now converted to the same repo/CI-ready pattern:
# env-var credentials and a Zoho Analytics write-back instead of a local
# Excel file.
#   1) Auth: 1NCE Management API OAuth2 client_credentials grant.
#   2) SIM inventory: GET /v1/sims, paginated — carries 1NCE's own SIM
#      status for free.
#   3) Usage + quota: per-SIM concurrent calls (GET /v1/sims/{iccid}/usage
#      for yesterday's window, GET /v1/sims/{iccid}/quota/data for the
#      current plan/quota), rate-limited with the same
#      AsyncLimiter/Semaphore pattern the FloLive script uses.
#   4) AccountId / Account Name mapping: identical Zoho Analytics logic to
#      the other three scripts (no Zenduit website/API calls).
#
# ⚠️ CREDENTIALS: this script previously had a real 1NCE Management API
# client id + secret hard-coded at the top. That is a serious problem for a
# script that lives in a repo — anyone who can read the file gets live API
# access. Both values now come from environment variables (ONCE_USERNAME /
# ONCE_PASSWORD), set as GitHub Actions secrets for the scheduled run, the
# same way the Monogoto/Telenor/FloLive scripts do it. If that client
# secret was ever committed, pasted, or shared, treat it as compromised and
# rotate it in the 1NCE portal.
#
# OUTPUT: this used to write a local .xlsx to a hard-coded Windows path,
# which only works on one machine and can't run on a GitHub Actions runner.
# Like the other three scripts, Zoho Analytics is now the ONLY destination —
# a write-back block (previously missing from this script, which only READ
# from Analytics) pushes the combined report into a 'OneNCE_daily_report'
# table. Nothing is written to disk, so this runs cleanly on CI with no
# path configuration at all.
#
# THE ZANALYTICS-ORGID FIX (carried over from Monogoto/Telenor/FloLive): Zoho
# Analytics calls were failing with ORGID_NOT_PRESENT_IN_THE_HEADER
# (errorCode 8083) because `os.getenv("ZOHO_ORG_ID", default)` only falls
# back to the default when the var is UNSET — GitHub Actions sets it to an
# EMPTY STRING when the secret doesn't exist, so the default never kicked
# in and the header went out blank. Fixed by treating blank/whitespace as
# unset, AND by also sending the org id in the request payload (form field /
# query string) alongside the header, belt and braces.
#
# Everything else — the 1NCE fetching logic, usage/quota parsing, the Zoho
# Analytics device/account read lookups, the report shape (base_combined /
# no_customer_in_zenduone / account_usage_summary) — is unchanged from what
# was pasted; only the three items above (credentials, output destination,
# org-id handling) were touched.
# ==========================================================
# ==========================================================
# CONFIG
# ==========================================================
ONE_NCE = {
    "url": os.getenv("ONCE_BASE_URL", "https://api.1nce.com"),
    # Credentials come from environment variables so that NOTHING SECRET is
    # ever stored in this file - important because this repo is public.
    # Set ONCE_USERNAME / ONCE_PASSWORD locally (PyCharm run config or your
    # shell) and as GitHub Actions repository secrets for the scheduled run.
    "username": os.getenv("ONCE_USERNAME", ""),
    "password": os.getenv("ONCE_PASSWORD", ""),
}
PAGE_SIZE = 100
MAX_RPS = 15
CONCURRENT_REQUESTS = 15


# ==========================================================
# SMALL SHARED HELPER (used by both Zoho Analytics column lookups below)
# ==========================================================
def _find_col(df, candidates):
    # Guard against non-string column labels: csv.DictReader stuffs any
    # values beyond the header row under a None key.
    lower_map = {str(c).lower(): c for c in df.columns if c is not None}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def _numeric_fillna(series, fill=0):
    """
    fillna(0) on an all-NaN *object* column (which is what a failed merge
    leaves behind) triggers pandas' "Downcasting object dtype arrays on
    .fillna is deprecated" FutureWarning. Coercing to numeric first both
    silences that and guarantees the column is actually summable downstream.
    """
    return pd.to_numeric(series, errors="coerce").fillna(fill)


# ==========================================================
# AUTH
# ==========================================================
async def get_1nce_token():
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{ONE_NCE['url']}/management-api/oauth/token",
            data={"grant_type": "client_credentials"},
            auth=aiohttp.BasicAuth(ONE_NCE["username"], ONE_NCE["password"]),
            timeout=ClientTimeout(total=30)
        ) as r:
            r.raise_for_status()
            data = await r.json()
            return data["access_token"]


# ==========================================================
# 1NCE SIM INVENTORY (BASE) - carries SIM_Status.
# ==========================================================
def parse_status(payload):
    if isinstance(payload, dict):
        return payload.get("status") or payload.get("simState") or payload.get("state")
    if isinstance(payload, str):
        return payload
    return None


async def fetch_all_sims(token):
    headers = {"Authorization": f"Bearer {token}"}
    page = 1
    rows = []
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(
                f"{ONE_NCE['url']}/management-api/v1/sims",
                headers=headers,
                params={"page": page, "pageSize": PAGE_SIZE},
                timeout=ClientTimeout(total=30)
            ) as r:
                r.raise_for_status()
                batch = await r.json()
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < PAGE_SIZE:
                break
            page += 1

    if rows:
        print(f"--- Sample fields available directly from /v1/sims (record 1 of {len(rows)}) ---")
        print(rows[0])
        print("-" * 60)

    df = pd.DataFrame(rows)
    if df.empty or "iccid" not in df.columns:
        return pd.DataFrame(columns=["ICCID", "SIM_Status"])

    df["iccid"] = df["iccid"].where(df["iccid"].notna(), None)
    df["iccid"] = df["iccid"].astype(str).str.strip()
    df.loc[df["iccid"].isin(["None", "nan", ""]), "iccid"] = None

    if "status" in df.columns:
        df["status"] = df["status"].apply(parse_status)
    else:
        df["status"] = None

    return df[["iccid", "status"]].rename(columns={"iccid": "ICCID", "status": "SIM_Status"})


# ==========================================================
# 1NCE - usage (single day) + plan/quota, combined per SIM.
# ==========================================================
def parse_quota(payload):
    if not isinstance(payload, dict):
        return None, None, None
    return payload.get("totalVolume"), payload.get("volume"), payload.get("expiryDate")


def parse_usage(payload):
    if not isinstance(payload, dict):
        return None
    total = 0.0
    for s in payload.get("stats", []):
        if s.get("date") == "TOTAL":
            continue
        total += float(s.get("data", {}).get("volume", 0) or 0)
    return round(total, 2)


async def fetch_sim_row(session, limiter, sem, token, iccid, day):
    headers = {"Authorization": f"Bearer {token}"}

    async def _get(url, params=None):
        async with limiter, sem:
            try:
                async with session.get(url, headers=headers, params=params) as r:
                    if r.status != 200:
                        return None, r.status
                    return await r.json(), 200
            except Exception:
                return None, "exception"

    usage_payload, usage_code = await _get(
        f"{ONE_NCE['url']}/management-api/v1/sims/{iccid}/usage",
        params={
            "start_dt": day.strftime("%Y-%m-%dT00:00:00Z"),
            "end_dt": day.strftime("%Y-%m-%dT23:59:59Z"),
        },
    )
    quota_payload, quota_code = await _get(f"{ONE_NCE['url']}/management-api/v1/sims/{iccid}/quota/data")
    plan_mb, remaining_mb, quota_expiry = parse_quota(quota_payload)
    return {
        "ICCID": iccid,
        "1NCE_MB_Usage": parse_usage(usage_payload) if usage_code == 200 else None,
        "Usage_Fetch_Error": None if usage_code == 200 else str(usage_code),
        "OneNCE_Plan_MB": plan_mb,
        "OneNCE_Remaining_MB": remaining_mb,
        "OneNCE_Quota_Expiry": quota_expiry,
    }


async def fetch_all_sim_data(token, iccids, day):
    timeout = ClientTimeout(sock_connect=20, sock_read=60)
    connector = TCPConnector(limit=CONCURRENT_REQUESTS)
    limiter = AsyncLimiter(MAX_RPS, 1)
    sem = Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [fetch_sim_row(session, limiter, sem, token, iccid, day) for iccid in iccids]
        results = []
        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Fetching 1NCE usage+plan"):
            results.append(await f)
        return pd.DataFrame(results)


# ==========================================================
# ZOHO ANALYTICS — the ONLY source of Zenduit-side data (no calls to
# Zenduit's own website/API). Two views are used, identical to the
# Monogoto/Telenor/FloLive scripts:
#   1) "Zenduit Devices" view: ICCID, AccountId, Device_Serial,
#      Zenduit_Data_Plan, CompanyName, Zenduit_Usage_MB, Zenduit_BillingStatus
#   2) "Accounts" view:        AccountId -> Analytics_Customer_Name
# Kept synchronous, run via asyncio.to_thread from main().
# ==========================================================
ZOHO_ANALYTICS_DOMAIN = os.getenv("ZOHO_ANALYTICS_DOMAIN", "analyticsapi.zoho.com")
ZOHO_ACCOUNTS_DOMAIN = os.getenv("ZOHO_ACCOUNTS_DOMAIN", "accounts.zoho.com")
ZOHO_ANALYTICS_API = f"https://{ZOHO_ANALYTICS_DOMAIN}/restapi/v2"
# NOTE: os.getenv(name, default) only returns the default when the variable is
# UNSET. In GitHub Actions `ZOHO_ORG_ID: ${{ secrets.ZOHO_ORG_ID }}` sets the
# variable to an EMPTY STRING when that secret doesn't exist, so the default
# never kicks in and the ZANALYTICS-ORGID header goes out blank — which Zoho
# reports as ORGID_NOT_PRESENT_IN_THE_HEADER (errorCode 8083). Treating
# blank/whitespace as "unset" is what makes the fallback actually apply.
ZOHO_ORG_ID = (os.getenv("ZOHO_ORG_ID") or "").strip() or "67409019"
ZOHO_ANALYTICS_WORKSPACE_ID = "953790000013364003"
ZOHO_ANALYTICS_ZENDUIT_DEVICES_VIEW_ID = "953790000054827175"   # "Zenduit Devices" view
ZOHO_ANALYTICS_ACCOUNTS_VIEW_ID = "953790000013364024"          # "Accounts" view
ZOHO_ANALYTICS_EXPORT_TIMEOUT_SECONDS = int(os.getenv("ZOHO_ANALYTICS_EXPORT_TIMEOUT_SECONDS", "300"))


def _pick_env(*names):
    """
    Same first-match-wins fallback chain the Monogoto/Telenor/FloLive
    scripts use, but also reports WHICH env var actually supplied the value.

    This matters: the chain prefers ANALYTICS-specific vars and falls back
    to generic ZOHO_* ones. If the generic vars hold CRM credentials (a
    very common setup), the fallback silently produces a token that
    authenticates fine but lacks Analytics scopes - which surfaces later as
    the confusing "INVALID_OAUTHSCOPE / errorCode 8540" export failure
    rather than as an auth error. Printing the source name makes that
    visible instead of invisible.
    """
    for n in names:
        v = os.environ.get(n)
        if v:
            return v, n
    return None, None


ZOHO_ANALYTICS_CLIENT_ID, _ZOHO_CLIENT_ID_SRC = _pick_env(
    "ZOHO_CLIENT_ID_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_ID", "ZOHO_CLIENT_ID")
ZOHO_ANALYTICS_CLIENT_SECRET, _ZOHO_CLIENT_SECRET_SRC = _pick_env(
    "ZOHO_CLIENT_SECRET_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_SECRET", "ZOHO_CLIENT_SECRET")
ZOHO_ANALYTICS_REFRESH_TOKEN, _ZOHO_REFRESH_TOKEN_SRC = _pick_env(
    "ZOHO_CLIENT_REFRESH_TOKEN_ANALYTICS", "ZOHO_ANALYTICS_REFRESH_TOKEN", "ZOHO_REFRESH_TOKEN")

# Names that indicate we got Analytics-specific credentials rather than
# falling through to whatever generic ZOHO_* vars happen to be set.
_ZOHO_ANALYTICS_SPECIFIC = {
    "ZOHO_CLIENT_ID_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_ID",
    "ZOHO_CLIENT_SECRET_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_SECRET",
    "ZOHO_CLIENT_REFRESH_TOKEN_ANALYTICS", "ZOHO_ANALYTICS_REFRESH_TOKEN",
}


def _report_zoho_credential_sources():
    srcs = {
        "client_id": _ZOHO_CLIENT_ID_SRC,
        "client_secret": _ZOHO_CLIENT_SECRET_SRC,
        "refresh_token": _ZOHO_REFRESH_TOKEN_SRC,
    }
    print(f"Zoho credentials sourced from env vars: {srcs}")
    print(f"Zoho org id in use: {ZOHO_ORG_ID!r} "
          f"(env ZOHO_ORG_ID={os.environ.get('ZOHO_ORG_ID')!r})")

    unset = [k for k, v in srcs.items() if v is None]
    if unset:
        print(f"⚠️ No env var found at all for: {unset}. If you're running this from a "
              f"different PyCharm run configuration than the other SIM report scripts, "
              f"copy the Zoho env vars across - run configs don't share them.")
        return

    generic = {k: v for k, v in srcs.items() if v not in _ZOHO_ANALYTICS_SPECIFIC}
    if generic:
        print(f"⚠️ These fell back to GENERIC Zoho env vars rather than Analytics-specific "
              f"ones: {generic}. If those generic vars hold Zoho CRM credentials, the token "
              f"will authenticate but be rejected by Analytics with INVALID_OAUTHSCOPE "
              f"(errorCode 8540). Set ZOHO_CLIENT_ID_ANALYTICS / ZOHO_CLIENT_SECRET_ANALYTICS / "
              f"ZOHO_CLIENT_REFRESH_TOKEN_ANALYTICS to a token generated with the "
              f"ZohoAnalytics.data.read + ZohoAnalytics.metadata.read scopes.")


def get_zoho_analytics_token():
    _report_zoho_credential_sources()
    r = requests.post(
        f"https://{ZOHO_ACCOUNTS_DOMAIN}/oauth/v2/token",
        data={
            "refresh_token": ZOHO_ANALYTICS_REFRESH_TOKEN,
            "client_id": ZOHO_ANALYTICS_CLIENT_ID,
            "client_secret": ZOHO_ANALYTICS_CLIENT_SECRET,
            "grant_type": "refresh_token"
        },
        timeout=30
    )
    data = r.json()
    if r.status_code != 200 or "access_token" not in data:
        raise RuntimeError(f"Zoho Analytics OAuth failed | status={r.status_code} | response={data}")
    granted = data.get("scope")
    print(f"Zoho token acquired. Granted scope: {granted or '(not reported by Zoho)'}")
    if granted and "ZohoAnalytics" not in granted:
        print("⚠️ The granted scope contains no ZohoAnalytics.* entries — the Analytics "
              "exports below will fail with INVALID_OAUTHSCOPE. This is a credentials "
              "problem, not a script problem: the refresh token needs to be regenerated "
              "with Analytics scopes.")
    return data["access_token"]


def _zoho_headers(token):
    return {"Authorization": f"Zoho-oauthtoken {token}", "ZANALYTICS-ORGID": str(ZOHO_ORG_ID)}


def _zoho_create_job(token, view_id):
    url = f"{ZOHO_ANALYTICS_API}/bulk/workspaces/{ZOHO_ANALYTICS_WORKSPACE_ID}/views/{view_id}/data"
    # Org id is sent in the payload as well as the header — belt and braces
    # against the ORGID_NOT_PRESENT_IN_THE_HEADER (8083) rejection.
    params = {
        "CONFIG": json.dumps({"responseFormat": "csv"}, separators=(",", ":")),
        "ZANALYTICS-ORGID": str(ZOHO_ORG_ID),
    }
    resp = requests.get(url, params=params, headers=_zoho_headers(token), timeout=60)
    if resp.status_code >= 400:
        raise RuntimeError(f"Zoho Analytics create-export-job (view {view_id}) failed: "
                           f"{resp.status_code}: {resp.text[:500]}")
    job_id = (resp.json().get("data") or {}).get("jobId")
    if not job_id:
        raise RuntimeError(f"Zoho Analytics: no jobId returned for view {view_id}: {resp.text[:300]}")
    return job_id


def _zoho_wait(token, job_id, timeout_seconds=ZOHO_ANALYTICS_EXPORT_TIMEOUT_SECONDS):
    url = f"{ZOHO_ANALYTICS_API}/bulk/workspaces/{ZOHO_ANALYTICS_WORKSPACE_ID}/exportjobs/{job_id}"
    start = time.time()
    deadline = start + timeout_seconds
    last_log = start
    while time.time() < deadline:
        resp = requests.get(url, params={"responseFormat": "json"}, headers=_zoho_headers(token), timeout=60)
        resp.raise_for_status()
        info = resp.json().get("data") or {}
        if info.get("jobStatus") == "JOB COMPLETED" or str(info.get("jobCode")) == "1004":
            return info["downloadUrl"]
        if str(info.get("jobCode")) in ("1003", "1005"):
            raise RuntimeError(f"Zoho Analytics export job {job_id} failed: {info}")
        now = time.time()
        if now - last_log >= 30:
            print(f"  ...still waiting on Zoho Analytics export job {job_id} "
                  f"({now - start:.0f}s elapsed, timeout at {timeout_seconds}s)")
            last_log = now
        time.sleep(2)
    raise RuntimeError(f"Zoho Analytics export job {job_id} timed out after {timeout_seconds}s.")


def _zoho_download(token, url):
    resp = requests.get(url, headers={**_zoho_headers(token), "Accept-Encoding": "identity"}, timeout=180)
    resp.raise_for_status()
    text = resp.text
    return text[1:] if text and text[0] == "﻿" else text


def _flatten(obj, parent="", out=None):
    out = {} if out is None else out
    for k, v in obj.items():
        key = f"{parent}_{k}" if parent else k
        if isinstance(v, dict):
            _flatten(v, key, out)
        else:
            out[key] = v
    return out


def _parse_zoho_export(text):
    if text.lstrip()[:1] in ("{", "["):
        obj = json.loads(text)
        recs = obj.get("data") if isinstance(obj, dict) and "data" in obj else obj
        if isinstance(recs, dict):
            recs = [recs]
        return [_flatten(r) for r in recs if isinstance(r, dict)]
    return list(csv.DictReader(text.splitlines()))


def fetch_analytics_view(token, view_id, label, retries=2, timeout_seconds=ZOHO_ANALYTICS_EXPORT_TIMEOUT_SECONDS):
    """
    Runs a Zoho Analytics bulk export job and returns the parsed rows as a
    list of dicts (empty list on failure).

    Retries the whole job (create -> wait -> download) up to `retries`
    times instead of raising on the first stall/timeout, so a flaky
    Analytics export can't take down the whole run - the caller falls back
    to an empty lookup instead.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            job_id = _zoho_create_job(token, view_id)
            print(f"  Zoho Analytics export job created for '{label}': {job_id} (attempt {attempt}/{retries})")
            download_url = _zoho_wait(token, job_id, timeout_seconds=timeout_seconds)
            rows = _parse_zoho_export(_zoho_download(token, download_url))
            print(f"--- {label}: {len(rows)} rows. Columns found: "
                  f"{list(rows[0].keys()) if rows else '(no rows returned)'} ---")
            return rows
        except Exception as e:
            last_err = e
            print(f"⚠️ Zoho Analytics export for '{label}' failed on attempt {attempt}/{retries}: {e}")
    print(f"⚠️ Zoho Analytics export for '{label}' failed after {retries} attempt(s) — "
          f"giving up. Last error: {last_err}")
    return []


_ZENDUIT_DEVICE_COLUMNS = ["ICCID", "AccountId", "Device_Serial", "Zenduit_Data_Plan",
                           "CompanyName", "Zenduit_Usage_MB", "Zenduit_BillingStatus"]


def fetch_zenduit_devices_analytics(token):
    """
    Pulls ALL Zenduit-side data needed for this report - ICCID, AccountId,
    Device_Serial, Zenduit_Data_Plan, CompanyName, Zenduit_Usage_MB, and
    Zenduit_BillingStatus - from the Zoho Analytics "Zenduit Devices" view.
    This is the ONLY source of Zenduit-side data; the script never talks to
    Zenduit's own website/API. Identical to the other scripts' version.

    Column names are auto-detected via _find_col(). Any field that can't be
    matched is left blank/0 for every row (report still gets written), and
    a warning listing exactly which fields weren't found is printed instead
    of guessing or crashing - check that warning and tell me the real
    column name(s) if something's missing.
    """
    rows = fetch_analytics_view(token, ZOHO_ANALYTICS_ZENDUIT_DEVICES_VIEW_ID, "Zenduit Devices (analytics)")
    if not rows:
        print("⚠️ Zenduit Devices (analytics) export returned no data — "
              "device/account info will be empty for every SIM this run.")
        return pd.DataFrame(columns=_ZENDUIT_DEVICE_COLUMNS)

    df = pd.DataFrame(rows)

    col_map = {
        "ICCID": _find_col(df, ["ICCID", "SIM", "Sim", "SIM Number", "Iccid"]),
        "AccountId": _find_col(df, ["AccountId", "Account_Id", "AccountID", "Account Id",
                                     "CustomerId", "Customer_Id"]),
        "Device_Serial": _find_col(df, ["Device_Serial", "DeviceSerial", "Serial", "Device Serial"]),
        "Zenduit_Data_Plan": _find_col(df, ["Zenduit_Data_Plan", "DataPlan", "Data Plan",
                                             "Data_Plan", "Plan"]),
        "CompanyName": _find_col(df, ["CompanyName", "Company_Name", "Company Name", "Company"]),
        "Zenduit_Usage_MB": _find_col(df, ["Zenduit_Usage_MB", "Data_Usage", "Usage",
                                            "Zenduit_Usage", "Usage_MB", "Data Usage",
                                            "DataUsage"]),
        "Zenduit_BillingStatus": _find_col(df, ["Zenduit_BillingStatus", "BillingStatus",
                                                 "Billing_Status", "Billing Status", "Status"]),
    }
    print(f"Column match — Zenduit Devices (analytics): {col_map}")

    if not col_map["ICCID"]:
        print("⚠️ Couldn't find an ICCID/SIM column in the Zenduit Devices view — "
              "returning an empty lookup. See the column list printed just above "
              "(from fetch_analytics_view) and tell me the real column name.")
        return pd.DataFrame(columns=_ZENDUIT_DEVICE_COLUMNS)

    missing = [k for k, v in col_map.items() if v is None and k != "ICCID"]
    if missing:
        print(f"⚠️ These fields weren't found in the Zenduit Devices view and will be "
              f"blank/0 for every SIM: {missing}. Tell me the real column name(s) if "
              f"they exist under a different name.")

    out = pd.DataFrame()
    out["ICCID"] = df[col_map["ICCID"]].astype(str).str.strip()
    out["AccountId"] = df[col_map["AccountId"]].astype(str).str.strip() if col_map["AccountId"] else None
    out["Device_Serial"] = df[col_map["Device_Serial"]].astype(str).str.strip() if col_map["Device_Serial"] else None
    out["Zenduit_Data_Plan"] = (
        pd.to_numeric(df[col_map["Zenduit_Data_Plan"]], errors="coerce").fillna(0)
        if col_map["Zenduit_Data_Plan"] else 0
    )
    out["CompanyName"] = df[col_map["CompanyName"]] if col_map["CompanyName"] else None
    out["Zenduit_Usage_MB"] = (
        pd.to_numeric(df[col_map["Zenduit_Usage_MB"]], errors="coerce").fillna(0)
        if col_map["Zenduit_Usage_MB"] else 0
    )
    out["Zenduit_BillingStatus"] = (
        df[col_map["Zenduit_BillingStatus"]] if col_map["Zenduit_BillingStatus"] else None
    )

    total_rows, unique_iccids = len(out), out["ICCID"].nunique()
    if total_rows != unique_iccids:
        print(f"ℹ️ Zenduit Devices (analytics) has {total_rows} rows for only "
              f"{unique_iccids} unique ICCIDs — keeping the highest-Zenduit_Data_Plan "
              f"row per ICCID (same tie-break the other scripts use). If that's wrong "
              f"for this view (e.g. it's usage history, not one row per device), tell "
              f"me and I'll change the tie-break/aggregation.")
    out = (
        out.sort_values("Zenduit_Data_Plan", ascending=False)
           .drop_duplicates(subset=["ICCID"], keep="first")
    )
    return out


def fetch_account_name_lookup(token):
    """
    AccountId -> Analytics_Customer_Name, sourced from the Zoho Analytics
    "Accounts" view (ZOHO_ANALYTICS_ACCOUNTS_VIEW_ID). AccountId itself
    comes from fetch_zenduit_devices_analytics() above. Identical to the
    other scripts' version.

    Returns an EMPTY lookup (with a warning printed) instead of raising if
    the export fails after retries, so the rest of the daily report still
    gets written - just with Analytics_Customer_Name blank for that run.
    """
    rows = fetch_analytics_view(token, ZOHO_ANALYTICS_ACCOUNTS_VIEW_ID, "Accounts (analytics)")
    if not rows:
        print("⚠️ Accounts (analytics) export returned no data — "
              "Analytics_Customer_Name will be empty for every SIM this run.")
        return pd.DataFrame(columns=["AccountId", "Analytics_Customer_Name"])

    df_accounts = pd.DataFrame(rows)
    account_id_col = _find_col(df_accounts, ["Id", "AccountId", "Account_Id", "AccountID"])
    account_name_col = _find_col(df_accounts, ["Account Name", "AccountName", "Account_Name", "Name",
                                               "CustomerName", "Customer_Name"])
    print(f"Column match — accounts id: {account_id_col!r} | accounts name: {account_name_col!r}")
    if not (account_id_col and account_name_col):
        print("⚠️ Couldn't confidently identify the id/name columns in the Accounts view — "
              "returning an empty lookup. See columns printed above and tell me the real names.")
        return pd.DataFrame(columns=["AccountId", "Analytics_Customer_Name"])

    df_lookup = df_accounts[[account_id_col, account_name_col]].rename(
        columns={account_id_col: "AccountId", account_name_col: "Analytics_Customer_Name"}
    )
    df_lookup["AccountId"] = df_lookup["AccountId"].astype(str).str.strip()
    return df_lookup.drop_duplicates(subset=["AccountId"], keep="first")


# ==========================================================
# ZOHO ANALYTICS — WRITE BACK (push the report INTO Analytics)
# ==========================================================
# NEW in this version — the original 1NCE script only READ from Zoho
# Analytics (device/account lookups) and wrote its combined report to a
# local Excel file. That local file can't run on a GitHub Actions runner, so
# this mirrors the Monogoto/Telenor/FloLive scripts' write-back: chunked
# CSV upload with importType=truncateadd on the first chunk and append on
# the rest, staying under Zoho's synchronous 20MB-per-request cap. See
# those scripts' header comments for the three deliberate differences from
# the Geotab ETL reference (separate write credentials, table looked up by
# name and created on demand, CONFIG sent as form data first / query string
# second). The empty-DataFrame guard is important: with truncateadd,
# uploading an empty frame would WIPE the table.
# ==========================================================
ZOHO_TARGET_TABLE_NAME = "OneNCE_daily_report"
ZOHO_MAX_BYTES_PER_IMPORT = 14 * 1024 * 1024   # well under Zoho's 20MB sync cap

ZOHO_WRITE_CLIENT_ID, _ZOHO_W_ID_SRC = _pick_env(
    "ZOHO_CLIENT_ID_UNI", "ZOHO_CLIENT_ID_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_ID", "ZOHO_CLIENT_ID")
ZOHO_WRITE_CLIENT_SECRET, _ZOHO_W_SECRET_SRC = _pick_env(
    "ZOHO_CLIENT_SECRET_UNI", "ZOHO_CLIENT_SECRET_ANALYTICS", "ZOHO_ANALYTICS_CLIENT_SECRET",
    "ZOHO_CLIENT_SECRET")
ZOHO_WRITE_REFRESH_TOKEN, _ZOHO_W_REFRESH_SRC = _pick_env(
    "ZOHO_REFRESH_TOKEN_UNI", "ZOHO_CLIENT_REFRESH_TOKEN_ANALYTICS",
    "ZOHO_ANALYTICS_REFRESH_TOKEN", "ZOHO_REFRESH_TOKEN")


def get_zoho_write_token():
    """
    Access token for WRITING. Prints which env vars supplied it and what
    scope came back, so a read-only token is caught here with a clear
    message instead of as an opaque failure mid-upload.
    """
    srcs = {"client_id": _ZOHO_W_ID_SRC, "client_secret": _ZOHO_W_SECRET_SRC,
            "refresh_token": _ZOHO_W_REFRESH_SRC}
    print(f"Zoho WRITE credentials sourced from env vars: {srcs}")
    missing = [k for k, v in srcs.items() if v is None]
    if missing:
        raise RuntimeError(
            f"No Zoho write credentials found for: {missing}. Set ZOHO_CLIENT_ID_UNI / "
            f"ZOHO_CLIENT_SECRET_UNI / ZOHO_REFRESH_TOKEN_UNI (the same trio the Geotab "
            f"ETL / Monogoto / Telenor / FloLive scripts use to write) in this run "
            f"configuration."
        )
    r = requests.post(
        f"https://{ZOHO_ACCOUNTS_DOMAIN}/oauth/v2/token",
        data={
            "refresh_token": ZOHO_WRITE_REFRESH_TOKEN,
            "client_id": ZOHO_WRITE_CLIENT_ID,
            "client_secret": ZOHO_WRITE_CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    data = r.json()
    if r.status_code != 200 or "access_token" not in data:
        raise RuntimeError(f"Zoho write-token OAuth failed | status={r.status_code} | {data}")
    granted = data.get("scope") or ""
    print(f"Zoho write token acquired. Granted scope: {granted or '(not reported)'}")
    if granted and "ZohoAnalytics.data.create" not in granted \
            and "ZohoAnalytics.data.all" not in granted \
            and "ZohoAnalytics.fullaccess.all" not in granted:
        print(f"⚠️ This token's scope does not include ZohoAnalytics.data.create — the upload "
              f"will be rejected. Regenerate the refresh token with ZohoAnalytics.data.create "
              f"(plus ZohoAnalytics.metadata.read so the table can be looked up by name).")
    return data["access_token"]


def _zoho_write_headers(token):
    return {"Authorization": f"Zoho-oauthtoken {token}", "ZANALYTICS-ORGID": str(ZOHO_ORG_ID)}


def zoho_find_view_id(token, table_name):
    """Resolve a table NAME to its viewId. Returns None if it doesn't exist."""
    url = f"{ZOHO_ANALYTICS_API}/workspaces/{ZOHO_ANALYTICS_WORKSPACE_ID}/views"
    r = requests.get(url, params={"ZANALYTICS-ORGID": str(ZOHO_ORG_ID)},
                     headers=_zoho_write_headers(token), timeout=60)
    if r.status_code != 200:
        print(f"⚠️ Couldn't list Zoho views ({r.status_code}): {r.text[:300]} — "
              f"will attempt to create '{table_name}' as a new table.")
        return None
    views = (r.json().get("data") or {}).get("views") or []
    for v in views:
        if str(v.get("viewName", "")).strip().lower() == table_name.strip().lower():
            print(f"  Found existing Zoho table '{v.get('viewName')}' "
                  f"(viewId={v.get('viewId')}, type={v.get('viewType')})")
            return v.get("viewId")
    print(f"  No existing Zoho table named '{table_name}' — it will be created.")
    return None


def _zoho_post_import(url, config, csv_bytes, token, filename):
    """
    POSTs one CSV chunk. Tries CONFIG as a form field first (the way the
    Geotab ETL does it, proven working in this environment), then falls
    back to CONFIG in the query string (the way the v2 docs specify).
    """
    headers = _zoho_write_headers(token)
    files = {"FILE": (filename, csv_bytes, "text/csv")}
    config_json = json.dumps(config, separators=(",", ":"))

    # Org id rides along in the payload too (form field / query string), in
    # addition to the ZANALYTICS-ORGID header set by _zoho_write_headers.
    org_id = str(ZOHO_ORG_ID)
    attempts = [
        ("CONFIG as form field", {"url": url,
                                  "data": {"CONFIG": config_json,
                                           "ZANALYTICS-ORGID": org_id}}),
        ("CONFIG in query string", {"url": f"{url}?CONFIG={quote(config_json, safe='')}"
                                           f"&ZANALYTICS-ORGID={quote(org_id, safe='')}",
                                    "data": None}),
    ]
    last = None
    for label, kw in attempts:
        r = requests.post(kw["url"], headers=headers, data=kw["data"], files=files, timeout=300)
        if r.status_code == 200:
            return r.json()
        last = f"{label} -> {r.status_code}: {r.text[:400]}"
        print(f"  ⚠️ import attempt via {label} failed ({r.status_code}); "
              f"{'trying next form' if label != attempts[-1][0] else 'no more forms to try'}")
    raise RuntimeError(f"Zoho import failed. Last error: {last}")


def zoho_replace_table(df, table_name, token):
    """
    Replaces the whole table with `df`, chunked to stay under the import
    size cap. First chunk truncates (or creates the table if absent), the
    rest append.
    """
    if df is None or df.empty:
        print("⚠️ DataFrame is empty — SKIPPING the Zoho upload so the existing table "
              "isn't wiped by a truncateadd of nothing.")
        return
    view_id = zoho_find_view_id(token, table_name)
    header_bytes = len(df.iloc[0:0].to_csv(index=False).encode("utf-8"))
    full_bytes = len(df.to_csv(index=False).encode("utf-8"))
    avg_row = max(1, (full_bytes - header_bytes) // max(1, len(df)))
    rows_per_chunk = max(1, (ZOHO_MAX_BYTES_PER_IMPORT - header_bytes) // avg_row)
    total_rows = len(df)
    n_chunks = (total_rows + rows_per_chunk - 1) // rows_per_chunk
    print(f"🔹 Uploading {total_rows} rows ({full_bytes/1024/1024:.2f} MB) to Zoho table "
          f"'{table_name}' in {n_chunks} chunk(s) of ~{rows_per_chunk} rows...")
    sent = 0
    for idx, start in enumerate(range(0, total_rows, rows_per_chunk)):
        chunk = df.iloc[start:start + rows_per_chunk]
        csv_bytes = chunk.to_csv(index=False).encode("utf-8")   # header repeated per chunk
        first = (idx == 0)
        if first and view_id is None:
            url = f"{ZOHO_ANALYTICS_API}/workspaces/{ZOHO_ANALYTICS_WORKSPACE_ID}/data"
            config = {
                "tableName": table_name,
                "fileType": "csv",
                "autoIdentify": "true",
                "onError": "setcolumnempty",
            }
        else:
            target = view_id
            url = f"{ZOHO_ANALYTICS_API}/workspaces/{ZOHO_ANALYTICS_WORKSPACE_ID}/views/{target}/data"
            config = {
                "importType": "truncateadd" if first else "append",
                "fileType": "csv",
                "autoIdentify": "true",
                "onError": "setcolumnempty",
            }
        result = _zoho_post_import(url, config, csv_bytes, token,
                                   f"{table_name.lower()}.csv")
        data = result.get("data") or {}
        summary = data.get("importSummary") or {}
        if view_id is None:
            view_id = data.get("viewId")
            if view_id:
                print(f"  ✅ Created Zoho table '{table_name}' (viewId={view_id})")
            elif n_chunks > 1:
                raise RuntimeError(
                    "Zoho created the table but returned no viewId, so the remaining "
                    f"{n_chunks - 1} chunk(s) can't be appended. Response: {str(result)[:400]}")
        sent += len(chunk)
        print(f"  chunk {idx + 1}/{n_chunks}: sent {len(chunk)} rows "
              f"(Zoho reported {summary.get('successRowCount', summary.get('totalRowCount', '?'))}, "
              f"warnings={summary.get('warnings', 0)}) | cumulative {sent}/{total_rows}")
        if summary.get("importErrors"):
            print(f"    ⚠️ importErrors: {str(summary['importErrors'])[:300]}")
    print(f"✅ Zoho sync complete: {sent} rows loaded into '{table_name}'.")


def prepare_for_zoho(df, day):
    """
    Zoho-friendly copy of the report: stamp the report date, drop
    timezones (Zoho's CSV import chokes on tz-aware values) and flatten
    anything non-scalar.
    """
    out = df.copy()
    out.insert(0, "Report_Date", pd.Timestamp(day).strftime("%Y-%m-%d"))
    for col in out.columns:
        if isinstance(out[col].dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
        elif out[col].dtype == object:
            out[col] = out[col].map(
                lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)
    return out


# ==========================================================
# MAIN
# ==========================================================
def _print_run_summary(df_base, df_no_customer_in_zenduone):
    unmapped_iccid_count = df_no_customer_in_zenduone.shape[0]
    print(f"📊 ICCIDs without a matching row in the Zenduit Devices (analytics) view: {unmapped_iccid_count}")
    fail_count = int(df_base["Usage_Fetch_Error"].notna().sum())
    if fail_count:
        print(f"⚠️ {fail_count} SIM(s) failed to return 1NCE usage — see Usage_Fetch_Error column (not counted as 0)")
    print(df_base["SIM_Status"].value_counts(dropna=False))


async def main():
    if not ONE_NCE["username"] or not ONE_NCE["password"]:
        raise RuntimeError(
            "ONCE_USERNAME/ONCE_PASSWORD are not set. Fill these in as environment "
            "variables (your 1NCE Management API client id + client secret)."
        )

    print("🔹 Getting 1NCE token...")
    nce_token = await get_1nce_token()

    print("🔹 Fetching all SIMs...")
    df_base = await fetch_all_sims(nce_token)
    print(f"✅ Total SIMs fetched: {len(df_base)}")

    today = datetime.now(timezone.utc).date()
    day = today - timedelta(days=1)

    def zoho_analytics_chain():
        # Wraps both Zoho Analytics steps in try/except as a safety net —
        # even an unexpected error here (auth failure, network blip, etc.)
        # falls back to empty lookups instead of bubbling up and killing
        # the whole script.
        try:
            za_token = get_zoho_analytics_token()
            d_devices = fetch_zenduit_devices_analytics(za_token)
            d_account_name_lookup = fetch_account_name_lookup(za_token)
        except Exception as e:
            print(f"⚠️ Zoho Analytics lookups unavailable this run: {e}")
            d_devices = pd.DataFrame(columns=_ZENDUIT_DEVICE_COLUMNS)
            d_account_name_lookup = pd.DataFrame(columns=["AccountId", "Analytics_Customer_Name"])
        return d_devices, d_account_name_lookup

    print("🔹 Fetching 1NCE usage+plan, and Zoho Analytics device/account data, concurrently...")
    df_sim_data, (df_z_devices, df_account_name_lookup) = await asyncio.gather(
        fetch_all_sim_data(nce_token, df_base["ICCID"].tolist(), day),
        asyncio.to_thread(zoho_analytics_chain),
    )

    df_base = df_base.merge(df_sim_data, on="ICCID", how="left")
    print(f"✅ Zenduit devices (analytics) fetched: {len(df_z_devices)}")

    # Step 1: 1NCE (ICCID) <-> Zoho Analytics "Zenduit Devices" view, which
    # supplies AccountId, Device_Serial, Zenduit_Data_Plan, CompanyName, and
    # Zenduit_Usage_MB / Zenduit_BillingStatus in one merge.
    df_base = df_base.merge(df_z_devices, on="ICCID", how="left")
    df_base["Zenduit_Usage_MB"] = _numeric_fillna(df_base["Zenduit_Usage_MB"])

    # Step 2: now that AccountId is filled in, look up the Account Name
    # from the Zoho Analytics Accounts table by AccountId.
    df_base = df_base.merge(df_account_name_lookup, on="AccountId", how="left")
    print(f"✅ Account Name lookup merged: "
          f"{df_base['Analytics_Customer_Name'].notna().sum()} / {len(df_base)} SIMs matched")

    df_base["1NCE_MB_Usage"] = _numeric_fillna(df_base["1NCE_MB_Usage"])
    df_base["Zenduit_Data_Plan"] = _numeric_fillna(df_base["Zenduit_Data_Plan"])

    device_iccids = set(df_z_devices["ICCID"].dropna().astype(str).str.strip())
    df_no_customer_in_zenduone = df_base[
        df_base["ICCID"].notna() &
        ~df_base["ICCID"].astype(str).str.strip().isin(device_iccids)
        ]

    # ===== PUSH THE COMBINED TABLE INTO ZOHO ANALYTICS =====
    # Zoho is now the ONLY output (no Excel), which changes the failure
    # policy: an upload failure is no longer something to shrug off, because
    # nothing else is produced. It is therefore re-raised so the process
    # exits non-zero and the scheduled GitHub Actions run goes RED instead of
    # reporting success while having written nothing.
    print(f"🔹 Pushing base_combined to Zoho Analytics table '{ZOHO_TARGET_TABLE_NAME}'...")
    try:
        df_for_zoho = prepare_for_zoho(df_base, day)
        write_token = get_zoho_write_token()
        zoho_replace_table(df_for_zoho, ZOHO_TARGET_TABLE_NAME, write_token)
    except Exception as e:
        _print_run_summary(df_base, df_no_customer_in_zenduone)
        print(f"❌ Zoho Analytics upload FAILED: {e}")
        raise
    _print_run_summary(df_base, df_no_customer_in_zenduone)


if __name__ == "__main__":
    asyncio.run(main())
