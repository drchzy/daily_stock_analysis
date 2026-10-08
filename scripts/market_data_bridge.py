#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Market data bridge for ChatGPT/GitHub Actions.

Primary source: efinance (Eastmoney)
Fallback source: AKShare (Eastmoney)
"""

from __future__ import annotations

import argparse
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import efinance as ef
except Exception:
    ef = None

try:
    import akshare as ak
except Exception:
    ak = None


def _finite(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _num(v: Any, default: float = 0.0) -> float:
    x = _finite(v)
    return default if x is None else x


def _round(v: Any, n: int = 3) -> Optional[float]:
    x = _finite(v)
    return None if x is None else round(x, n)


def _records(df: pd.DataFrame, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    if max_rows:
        df = df.tail(max_rows)
    out: List[Dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        item: Dict[str, Any] = {}
        for k, v in row.items():
            if pd.isna(v):
                item[str(k)] = None
            elif isinstance(v, (pd.Timestamp, datetime)):
                item[str(k)] = v.strftime("%Y-%m-%d %H:%M:%S")
            elif hasattr(v, "item"):
                try:
                    item[str(k)] = v.item()
                except Exception:
                    item[str(k)] = v
            else:
                item[str(k)] = v
        out.append(item)
    return out


def _date_range(days: int) -> Tuple[str, str]:
    end = datetime.now()
    start = end - timedelta(days=max(days * 2, 90))
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rename = {
        "日期": "date", "时间": "date", "开盘": "open", "收盘": "close",
        "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount",
        "振幅": "amplitude", "涨跌幅": "pct_change", "涨跌额": "change",
        "换手率": "turnover", "股票代码": "code", "代码": "code",
        "股票名称": "name", "名称": "name",
    }
    df = df.rename(columns=rename).copy()
    for c in ["open", "close", "high", "low", "volume", "amount", "amplitude", "pct_change", "change", "turnover"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "date" in df:
        df["date"] = df["date"].astype(str)
    return df


def _normalize_minute(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rename = {
        "日期": "time", "时间": "time", "day": "time", "开盘": "open", "收盘": "close",
        "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount",
        "均价": "vwap", "涨跌幅": "pct_change", "涨跌额": "change",
        "振幅": "amplitude", "换手率": "turnover", "股票代码": "code",
        "代码": "code", "股票名称": "name", "名称": "name",
    }
    df = df.rename(columns=rename).copy()
    for c in ["open", "close", "high", "low", "volume", "amount", "vwap", "pct_change", "change", "amplitude", "turnover"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "time" in df:
        df["time"] = df["time"].astype(str)
    return df



TDX_HOSTS = [
    ("119.147.212.81", 7709),
    ("112.74.214.43", 7727),
    ("221.231.141.60", 7709),
    ("101.227.73.20", 7709),
    ("101.227.77.254", 7709),
    ("14.215.128.18", 7709),
    ("59.173.18.140", 7709),
    ("180.153.39.51", 7709),
]


def _tdx_connect():
    try:
        from pytdx.hq import TdxHq_API
    except Exception as e:
        raise RuntimeError(f"pytdx unavailable: {e}")
    last_error = None
    for host, port in TDX_HOSTS:
        api = TdxHq_API()
        try:
            if api.connect(host, port, time_out=3):
                return api, f"{host}:{port}"
        except Exception as e:
            last_error = e
        try:
            api.disconnect()
        except Exception:
            pass
    raise RuntimeError(f"unable to connect TDX servers: {last_error}")


def _tdx_market(code: str) -> int:
    return 1 if str(code).startswith(("60", "68")) else 0


def _is_tdx_a_share(market: int, code: str) -> bool:
    code = str(code).zfill(6)
    if market == 1:
        return code.startswith(("600", "601", "603", "605", "688", "689"))
    return code.startswith(("000", "001", "002", "003", "300", "301"))


def fetch_daily_tdx(code: str, days: int = 90) -> Tuple[pd.DataFrame, str]:
    api, host = _tdx_connect()
    try:
        market = _tdx_market(code)
        count = min(max(days + 30, 120), 800)
        rows = api.get_security_bars(9, market, str(code).zfill(6), 0, count) or []
        if not rows:
            raise RuntimeError("TDX returned no daily bars")
        df = pd.DataFrame(rows)
        rename = {"datetime": "date", "vol": "volume"}
        df = df.rename(columns=rename)
        for col in ["open", "close", "high", "low", "volume", "amount"]:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "date" in df:
            df["date"] = df["date"].astype(str)
            df = df.sort_values("date")
        return df.tail(days), f"pytdx:{host}"
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


def fetch_realtime_market_tdx() -> Tuple[pd.DataFrame, str]:
    api, host = _tdx_connect()
    try:
        securities = []
        name_map = {}
        for market in (0, 1):
            count = int(api.get_security_count(market) or 0)
            for start in range(0, count, 1000):
                page = api.get_security_list(market, start) or []
                if not page:
                    break
                for item in page:
                    code = str(item.get("code", "")).zfill(6)
                    name = str(item.get("name", "")).strip()
                    if code and name and _is_tdx_a_share(market, code):
                        securities.append((market, code))
                        name_map[(market, code)] = name
                if len(page) < 1000:
                    break

        rows = []
        for i in range(0, len(securities), 80):
            batch = securities[i:i + 80]
            quotes = api.get_security_quotes(batch) or []
            for q in quotes:
                code = str(q.get("code", "")).zfill(6)
                market = _tdx_market(code)
                price = _num(q.get("price"))
                pre_close = _num(q.get("last_close"))
                pct = ((price / pre_close - 1) * 100) if price > 0 and pre_close > 0 else None
                rows.append({
                    "code": code,
                    "name": name_map.get((market, code), ""),
                    "price": price,
                    "pct_change": pct,
                    "open": _num(q.get("open")),
                    "high": _num(q.get("high")),
                    "low": _num(q.get("low")),
                    "pre_close": pre_close,
                    "volume": _num(q.get("vol")),
                    "amount": _num(q.get("amount")),
                    "turnover": None,
                    "volume_ratio": None,
                })
        if not rows:
            raise RuntimeError("TDX returned no realtime quotes")
        return pd.DataFrame(rows), f"pytdx:{host}"
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


def fetch_daily(code: str, days: int = 90, adjust: str = "qfq") -> Tuple[pd.DataFrame, str]:
    beg, end = _date_range(days)
    errors: List[str] = []
    if ef is not None:
        try:
            fqt = {"": 0, "qfq": 1, "hfq": 2}.get(adjust, 1)
            df = ef.stock.get_quote_history(code, beg=beg, end=end, klt=101, fqt=fqt, suppress_error=True)
            df = _normalize_daily(df)
            if not df.empty:
                return df.tail(days), "efinance"
        except Exception as e:
            errors.append(f"efinance:{e}")
    if ak is not None:
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=beg, end_date=end, adjust=adjust, timeout=15)
            df = _normalize_daily(df)
            if not df.empty:
                return df.tail(days), "akshare"
        except Exception as e:
            errors.append(f"akshare_em:{e}")
        try:
            prefix = "sh" if str(code).startswith("6") else "sz"
            df = ak.stock_zh_a_daily(
                symbol=prefix + str(code).zfill(6),
                start_date=beg,
                end_date=end,
                adjust=adjust,
            )
            df = _normalize_daily(df)
            if not df.empty:
                return df.tail(days), "akshare_sina"
        except Exception as e:
            errors.append(f"akshare_sina:{e}")
    try:
        df, source = fetch_daily_tdx(code, days)
        if not df.empty:
            return df, source
    except Exception as e:
        errors.append(f"pytdx:{e}")
    raise RuntimeError("daily fetch failed: " + " | ".join(errors))


def fetch_minute(code: str, period: int = 1, start: str = "", end: str = "", max_rows: int = 500) -> Tuple[pd.DataFrame, str]:
    if period not in (1, 5, 15, 30, 60):
        raise ValueError("period must be one of 1,5,15,30,60")
    now = datetime.now()
    if not start:
        start = (now - timedelta(days=4)).strftime("%Y-%m-%d")
    if not end:
        end = now.strftime("%Y-%m-%d")
    errors: List[str] = []
    if ef is not None:
        try:
            beg = start.replace("-", "").replace(":", "").replace(" ", "")[:8]
            ed = end.replace("-", "").replace(":", "").replace(" ", "")[:8]
            df = ef.stock.get_quote_history(code, beg=beg, end=ed, klt=period, fqt=0, suppress_error=True)
            df = _normalize_minute(df)
            if not df.empty:
                return df.tail(max_rows), "efinance"
        except Exception as e:
            errors.append(f"efinance:{e}")
    if ak is not None:
        try:
            start_dt = start if " " in start else f"{start} 09:30:00"
            end_dt = end if " " in end else f"{end} 15:00:00"
            df = ak.stock_zh_a_hist_min_em(symbol=code, start_date=start_dt, end_date=end_dt, period=str(period), adjust="")
            df = _normalize_minute(df)
            if not df.empty:
                return df.tail(max_rows), "akshare"
        except Exception as e:
            errors.append(f"akshare_em:{e}")
        try:
            prefix = "sh" if str(code).startswith("6") else "sz"
            df = ak.stock_zh_a_minute(
                symbol=prefix + str(code).zfill(6),
                period=str(period),
                adjust="",
            )
            df = _normalize_minute(df)
            if not df.empty and "time" in df.columns:
                ts = pd.to_datetime(df["time"], errors="coerce")
                start_ts = pd.to_datetime(start)
                end_ts = pd.to_datetime(end) + (pd.Timedelta(hours=23, minutes=59) if " " not in end else pd.Timedelta(0))
                df = df[(ts >= start_ts) & (ts <= end_ts)]
            if not df.empty:
                return df.tail(max_rows), "akshare_sina"
        except Exception as e:
            errors.append(f"akshare_sina:{e}")
    raise RuntimeError("minute fetch failed: " + " | ".join(errors))


def fetch_realtime_market() -> Tuple[pd.DataFrame, str]:
    errors: List[str] = []
    if ef is not None:
        try:
            df = ef.stock.get_realtime_quotes("沪深A股")
            if df is not None and not df.empty:
                return df.copy(), "efinance"
        except Exception as e:
            errors.append(f"efinance:{e}")
    if ak is not None:
        try:
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                return df.copy(), "akshare"
        except Exception as e:
            errors.append(f"akshare_em:{e}")
        try:
            df = ak.stock_zh_a_spot()
            if df is not None and not df.empty:
                return df.copy(), "akshare_sina"
        except Exception as e:
            errors.append(f"akshare_sina:{e}")
        try:
            if hasattr(ak, "stock_zh_a_spot_tx"):
                df = ak.stock_zh_a_spot_tx()
                if df is not None and not df.empty:
                    return df.copy(), "akshare_tencent"
        except Exception as e:
            errors.append(f"akshare_tencent:{e}")
    try:
        df, source = fetch_realtime_market_tdx()
        if not df.empty:
            return df, source
    except Exception as e:
        errors.append(f"pytdx:{e}")
    raise RuntimeError("realtime market fetch failed: " + " | ".join(errors))


def _spot_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "股票代码": "code", "代码": "code", "股票名称": "name", "名称": "name",
        "最新价": "price", "涨跌幅": "pct_change", "最高": "high", "最低": "low",
        "今开": "open", "昨日收盘": "pre_close", "昨收": "pre_close",
        "换手率": "turnover", "量比": "volume_ratio", "成交量": "volume",
        "成交额": "amount", "总市值": "market_cap", "流通市值": "float_market_cap",
        "动态市盈率": "pe", "市盈率-动态": "pe",
        "trade": "price", "changepercent": "pct_change", "settlement": "pre_close",
        "turnoverratio": "turnover", "mktcap": "market_cap", "nmc": "float_market_cap", "per": "pe",
    }
    out = df.rename(columns=rename).copy()
    for c in ["price", "pct_change", "high", "low", "open", "pre_close", "turnover", "volume_ratio", "volume", "amount", "market_cap", "float_market_cap", "pe"]:
        if c in out:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    if "code" in out:
        out["code"] = out["code"].astype(str).str.lower().str.replace(r"^(sh|sz|bj)", "", regex=True).str.zfill(6)
    if "name" in out:
        out["name"] = out["name"].astype(str)
    return out


def _eligible(code: str, name: str, exclude_bse: bool, exclude_star: bool, exclude_st: bool) -> bool:
    code = str(code).zfill(6)
    name_u = str(name).upper()
    if exclude_bse and (code.startswith("4") or code.startswith("8") or code.startswith("92")):
        return False
    if exclude_star and (code.startswith("688") or code.startswith("689")):
        return False
    if exclude_st and ("ST" in name_u or "退" in str(name)):
        return False
    return True


def build_market_context(spot: pd.DataFrame) -> Dict[str, Any]:
    s = _spot_columns(spot)
    pct = pd.to_numeric(s.get("pct_change"), errors="coerce").dropna()
    context: Dict[str, Any] = {
        "total": int(len(pct)),
        "up": int((pct > 0).sum()),
        "down": int((pct < 0).sum()),
        "flat": int((pct == 0).sum()),
        "median_pct": _round(pct.median(), 3) if len(pct) else None,
        "avg_pct": _round(pct.mean(), 3) if len(pct) else None,
        "strong_up_3pct": int((pct >= 3).sum()),
        "strong_down_3pct": int((pct <= -3).sum()),
        "limit_like_up": int((pct >= 9.5).sum()),
        "limit_like_down": int((pct <= -9.5).sum()),
    }
    if "amount" in s:
        context["total_amount"] = _round(pd.to_numeric(s["amount"], errors="coerce").fillna(0).sum(), 0)
    return context


def fetch_board_rankings(kind: str = "industry", topn: int = 10) -> Dict[str, Any]:
    if ef is None:
        return {"source": None, "top": [], "bottom": []}
    fs = "行业板块" if kind == "industry" else "概念板块"
    try:
        df = ef.stock.get_realtime_quotes(fs)
        if df is None or df.empty:
            return {"source": "efinance", "top": [], "bottom": []}
        d = _spot_columns(df)
        if "pct_change" not in d:
            return {"source": "efinance", "top": [], "bottom": []}
        d = d.dropna(subset=["pct_change"]).sort_values("pct_change", ascending=False)
        cols = [c for c in ["code", "name", "pct_change", "turnover", "amount"] if c in d.columns]
        return {
            "source": "efinance",
            "top": _records(d[cols].head(topn)),
            "bottom": _records(d[cols].tail(topn).sort_values("pct_change")),
        }
    except Exception as e:
        return {"source": "efinance", "error": str(e), "top": [], "bottom": []}


def _calc_signal(code: str, name: str, spot_row: Dict[str, Any], hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if hist is None or len(hist) < 25:
        return None
    h = hist.copy().dropna(subset=["close", "volume"]).reset_index(drop=True)
    if len(h) < 25:
        return None

    close = float(h.iloc[-1]["close"])
    volume = float(h.iloc[-1]["volume"])
    ma5 = float(h["close"].tail(5).mean())
    ma10 = float(h["close"].tail(10).mean())
    ma20 = float(h["close"].tail(20).mean())
    ma5_prev = float(h["close"].iloc[-6:-1].mean())
    avg_vol5 = float(h["volume"].iloc[-6:-1].mean())
    prev20_high = float(h["high"].iloc[-21:-1].max()) if "high" in h else close
    ret5 = (close / float(h.iloc[-6]["close"]) - 1) * 100
    ret20 = (close / float(h.iloc[-21]["close"]) - 1) * 100
    daily_vol_ratio = volume / avg_vol5 if avg_vol5 > 0 else 0
    bias5 = (close / ma5 - 1) * 100 if ma5 > 0 else 0
    pct = _num(spot_row.get("pct_change"))
    turnover = _num(spot_row.get("turnover"))
    spot_vr = _num(spot_row.get("volume_ratio"))
    high = _num(spot_row.get("high"), close)
    low = _num(spot_row.get("low"), close)
    close_strength = (close - low) / (high - low) if high > low else 0.5

    score = 0.0
    reasons: List[str] = []
    setup = "trend"

    if close > ma5 > ma10 > ma20:
        score += 32
        reasons.append("MA5>MA10>MA20且收盘在MA5上")
    elif close > ma10 > ma20:
        score += 18
        reasons.append("中期趋势仍向上")
    else:
        score -= 18

    if ma5 > ma5_prev:
        score += 6
        reasons.append("MA5向上")

    if 0 <= bias5 <= 3.5:
        score += 12
        reasons.append(f"距MA5仅{bias5:.2f}%")
    elif bias5 > 5:
        score -= 15
        reasons.append("短线乖离过大")

    breakout = close >= prev20_high * 0.995 and daily_vol_ratio >= 1.15
    pullback = abs(bias5) <= 2.0 and close >= ma10 and 0 < daily_vol_ratio <= 0.90
    if breakout:
        score += 20
        setup = "volume_breakout"
        reasons.append("接近/突破20日高且量能放大")
    elif pullback:
        score += 18
        setup = "shrink_pullback"
        reasons.append("MA5附近缩量回踩")
    elif close >= prev20_high * 0.97:
        score += 8
        reasons.append("接近20日高位")

    if 1 <= ret5 <= 12:
        score += 8
        reasons.append(f"5日涨幅{ret5:.1f}%适中")
    elif ret5 > 18:
        score -= 10

    if 2 <= ret20 <= 25:
        score += 6
    elif ret20 > 35:
        score -= 8

    if 1.0 <= turnover <= 12:
        score += 5
    if 1.0 <= spot_vr <= 3.5:
        score += 4
    if close_strength >= 0.65:
        score += 5
        reasons.append("收盘靠近日内高位")
    elif close_strength <= 0.30:
        score -= 6
        reasons.append("收盘靠近日内低位")

    if pct >= 6.5:
        score -= 8
        reasons.append("当日涨幅偏大，次日避免追高")
    if pct <= -3:
        score -= 8

    stop = max(ma10 * 0.995, close * 0.97)
    stop = min(stop, close * 0.995)

    return {
        "code": code, "name": name, "score": round(score, 2), "setup": setup,
        "close": round(close, 3), "pct_change": _round(pct, 3),
        "turnover": _round(turnover, 3), "spot_volume_ratio": _round(spot_vr, 3),
        "daily_volume_ratio": round(daily_vol_ratio, 3),
        "ma5": round(ma5, 3), "ma10": round(ma10, 3), "ma20": round(ma20, 3),
        "bias5_pct": round(bias5, 3), "ret5_pct": round(ret5, 3), "ret20_pct": round(ret20, 3),
        "prev20_high": round(prev20_high, 3), "close_strength": round(close_strength, 3),
        "reference_plan": {
            "entry_low": round(max(ma5, close * 0.99), 3),
            "entry_high": round(close * 1.01, 3),
            "stop": round(stop, 3),
            "take_profit_1": round(close * 1.04, 3),
            "take_profit_2": round(close * 1.06, 3),
            "trailing_stop": "浮盈>=4%后，以阶段高点回撤2%作为减仓/退出参考",
        },
        "reasons": reasons[:6],
    }


def screen_ultra_short(
    spot_raw: pd.DataFrame,
    top_n: int = 10,
    max_prefilter: int = 50,
    exclude_bse: bool = True,
    exclude_star: bool = True,
    exclude_st: bool = True,
) -> List[Dict[str, Any]]:
    s = _spot_columns(spot_raw)
    for c in ["code", "name", "price", "pct_change", "amount"]:
        if c not in s.columns:
            raise RuntimeError(f"realtime data missing column: {c}")
    if "turnover" not in s.columns:
        s["turnover"] = float("nan")
    if "volume_ratio" not in s.columns:
        s["volume_ratio"] = float("nan")

    s = s[s.apply(lambda r: _eligible(r["code"], r["name"], exclude_bse, exclude_star, exclude_st), axis=1)]
    s = s.dropna(subset=["price", "pct_change", "amount"])
    s = s[(s["price"] >= 3) & (s["pct_change"] >= -2.5) & (s["pct_change"] <= 7.5) & (s["amount"] >= 8e7)]
    if s["turnover"].notna().any():
        s = s[(s["turnover"].fillna(0) >= 0.8) & (s["turnover"].fillna(0) <= 18)]

    vr = s["volume_ratio"].fillna(1.0) if "volume_ratio" in s else pd.Series(1.0, index=s.index)
    s = s.assign(
        _pre_score=
        s["pct_change"].fillna(0) * 1.2
        + s["turnover"].fillna(0) * 0.35
        + vr.clip(lower=0, upper=4) * 1.5
        + (s["amount"].clip(lower=1).map(lambda x: math.log10(x)) - 7) * 2
    ).sort_values("_pre_score", ascending=False).head(max_prefilter)

    tasks = {}
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for _, row in s.iterrows():
            code = str(row["code"]).zfill(6)
            tasks[pool.submit(fetch_daily, code, 70, "qfq")] = (code, str(row["name"]), row.to_dict())
        for fut in as_completed(tasks):
            code, name, row = tasks[fut]
            try:
                hist, source = fut.result()
                sig = _calc_signal(code, name, row, hist)
                if sig:
                    sig["history_source"] = source
                    results.append(sig)
            except Exception as e:
                results.append({"code": code, "name": name, "score": -999, "error": str(e)})
            time.sleep(0.03)

    results = [x for x in results if x.get("score", -999) > -900]
    results.sort(key=lambda x: x.get("score", -999), reverse=True)
    return results[:top_n]


def fetch_symbol_bundle(code: str, request: Dict[str, Any], spot_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"code": code}
    try:
        daily, src = fetch_daily(code, int(request.get("history_days", 60)), request.get("adjust", "qfq"))
        out["daily_source"] = src
        out["daily"] = _records(daily, int(request.get("history_days", 60)))
    except Exception as e:
        out["daily_error"] = str(e)

    try:
        minute, src = fetch_minute(
            code,
            int(request.get("minute_period", 1)),
            str(request.get("minute_start") or ""),
            str(request.get("minute_end") or ""),
            int(request.get("minute_rows", 500)),
        )
        out["minute_source"] = src
        out["minute"] = _records(minute, int(request.get("minute_rows", 500)))
    except Exception as e:
        out["minute_error"] = str(e)

    if spot_df is not None and not spot_df.empty:
        s = _spot_columns(spot_df)
        q = s[s["code"] == str(code).zfill(6)]
        if not q.empty:
            out["quote"] = _records(q.head(1))[0]
    return out


def run(request: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(request.get("mode", "scan")).lower()
    symbols = [str(x).zfill(6) for x in request.get("symbols", [])]
    spot_df: Optional[pd.DataFrame] = None
    spot_source: Optional[str] = None
    warnings: List[str] = []

    try:
        spot_df, spot_source = fetch_realtime_market()
    except Exception as e:
        warnings.append(str(e))

    result: Dict[str, Any] = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "request": request,
        "data_source": {"realtime": spot_source},
        "warnings": warnings,
    }

    if spot_df is not None:
        result["market_breadth"] = build_market_context(spot_df)
        if bool(request.get("include_boards", True)):
            n = int(request.get("board_top_n", 10))
            result["industry_boards"] = fetch_board_rankings("industry", n)
            result["concept_boards"] = fetch_board_rankings("concept", n)

    if symbols:
        result["symbols"] = [fetch_symbol_bundle(code, request, spot_df) for code in symbols]

    if mode in {"scan", "full"}:
        if spot_df is None:
            result["scan_error"] = "realtime market unavailable; scan skipped"
        else:
            filt = request.get("filters", {}) or {}
            result["candidates"] = screen_ultra_short(
                spot_df,
                top_n=int(request.get("top_n", 10)),
                max_prefilter=int(request.get("max_prefilter", 50)),
                exclude_bse=bool(filt.get("exclude_bse", True)),
                exclude_star=bool(filt.get("exclude_star", True)),
                exclude_st=bool(filt.get("exclude_st", True)),
            )
    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--request", default="market_data/request.json")
    p.add_argument("--output", default="market_data/latest.json")
    args = p.parse_args()

    request = json.loads(Path(args.request).read_text(encoding="utf-8"))
    result = run(request)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    print(json.dumps({
        "ok": True,
        "output": str(out),
        "generated_at": result.get("generated_at"),
        "candidate_count": len(result.get("candidates", [])),
        "symbol_count": len(result.get("symbols", [])),
        "warnings": result.get("warnings", []),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
