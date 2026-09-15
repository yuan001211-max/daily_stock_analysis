# -*- coding: utf-8 -*-
"""
游资逻辑每日选股（GitHub Actions 云端自包含版 v2）
====================================================
不依赖豆包环境，纯 Python 运行。数据源全部走新浪/腾讯（东财在云端/本地均不稳定）：
1. 候选池：新浪全市场快照 → 当日涨幅榜前 N 只强势股（动态捕捉当日主线）
   并叠加预设龙头池，保证覆盖面
2. 行情字段：腾讯批量接口补 总市值/PE/换手率/成交额/当日涨幅
3. 技术指标：新浪日线计算 MA20/MACD/量比/5日20日涨幅
4. 四风格透明判定 + 分组 + 推送飞书（环境变量 FEISHU_WEBHOOK_URL）

用法: python youzi_screen_cloud.py [--no-push] [--date YYYY-MM-DD] [--top N]
"""
import os
import sys
import json
import time
import argparse
import urllib.request
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# 预设龙头池（新浪快照失败时使用；与本地实测池一致）
# ---------------------------------------------------------------------------
FALLBACK_POOL = [
    ("002487", "大金重工", "风电设备"), ("002531", "天顺风能", "风电设备"),
    ("300850", "新强联", "风电设备"), ("601218", "吉鑫科技", "风电设备"),
    ("301155", "海力风电", "风电设备"), ("688155", "先惠技术", "锂电设备"),
    ("301662", "宏工科技", "锂电设备"), ("603092", "德力佳", "风电设备"),
    ("300750", "宁德时代", "锂电设备"), ("688223", "晶科能源", "光伏设备"),
    ("300274", "阳光电源", "光伏设备"), ("601012", "隆基绿能", "光伏设备"),
    ("002459", "晶澳科技", "光伏设备"), ("300316", "晶盛机电", "半导体设备"),
    ("688012", "中微公司", "半导体设备"), ("002371", "北方华创", "半导体设备"),
    ("688072", "拓荆科技", "半导体设备"), ("688361", "中科飞测", "半导体设备"),
    ("300604", "长川科技", "半导体设备"), ("688126", "沪硅产业", "半导体设备"),
    ("603501", "韦尔股份", "半导体设备"), ("002049", "紫光国微", "半导体设备"),
    ("688981", "中芯国际", "半导体设备"), ("300782", "卓胜微", "半导体设备"),
    ("688008", "澜起科技", "半导体设备"), ("603986", "兆易创新", "半导体设备"),
    ("688256", "寒武纪", "半导体设备"), ("002230", "科大讯飞", "AI算力"),
    ("300308", "中际旭创", "AI算力"), ("300502", "新易盛", "AI算力"),
    ("002475", "立讯精密", "消费电子"), ("002008", "大族激光", "激光设备"),
    ("601100", "恒立液压", "工程机械"), ("002415", "海康威视", "安防"),
    ("601138", "工业富联", "AI算力"), ("000725", "京东方A", "面板"),
    ("002594", "比亚迪", "新能源车"), ("300014", "亿纬锂能", "锂电设备"),
    ("002709", "天赐材料", "锂电设备"), ("300568", "星源材质", "锂电设备"),
    ("002812", "恩捷股份", "锂电设备"), ("300073", "当升科技", "锂电设备"),
    ("002466", "天齐锂业", "锂电设备"), ("002460", "赣锋锂业", "锂电设备"),
    ("603799", "华友钴业", "锂电设备"), ("300390", "天华新能", "锂电设备"),
    ("600438", "通威股份", "光伏设备"), ("002129", "TCL中环", "光伏设备"),
    ("688599", "天合光能", "光伏设备"), ("601615", "明阳智能", "风电设备"),
    ("300772", "运达股份", "风电设备"), ("002202", "金风科技", "风电设备"),
    ("601016", "节能风电", "风电设备"), ("600875", "东方电气", "风电设备"),
    ("300443", "金雷股份", "风电设备"), ("603218", "日月股份", "风电设备"),
    ("603985", "恒润股份", "风电设备"), ("300607", "拓斯达", "机器人"),
    ("688017", "绿的谐波", "机器人"), ("002747", "埃斯顿", "机器人"),
    ("688169", "石头科技", "机器人"), ("601127", "赛力斯", "新能源车"),
    ("000338", "潍柴动力", "工程机械"), ("300124", "汇川技术", "工控"),
]

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _norm_sym(symbol: str) -> str:
    return symbol.strip().replace(".SH", "").replace(".SZ", "").replace(".BJ", "").zfill(6)


def _tx_symbol(symbol: str) -> str:
    s = _norm_sym(symbol)
    if s.startswith("6"):
        return "sh" + s
    elif s.startswith(("0", "3")):
        return "sz" + s
    elif s.startswith(("4", "8")):
        return "bj" + s
    return "sh" + s


def _safe_float(v):
    try:
        if v is None or v == "" or v == "-" or v == "--":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _ema_series(vals, n):
    if not vals:
        return []
    k = 2 / (n + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def _is_trading_day(d: datetime) -> bool:
    try:
        import akshare as ak
        cal = ak.tool_trade_date_hist_sina()
        if cal is not None and not cal.empty:
            dates = set(cal["trade_date"].astype(str))
            return d.strftime("%Y-%m-%d") in dates
    except Exception:
        pass
    return d.weekday() < 5


# ---------------------------------------------------------------------------
# 候选池获取（新浪涨幅榜 + 预设池兜底）
# ---------------------------------------------------------------------------

def fetch_pool(top_n: int = 120) -> dict:
    """返回 {pool: [...], sectors: [...], source: str, spot_count: int}"""
    pool = []
    sectors = []
    source = "新浪涨幅榜"
    spot_count = 0
    df = None
    # 新浪快照可能限流，重试3次
    for attempt in range(3):
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot()
            if df is not None and not df.empty:
                break
        except Exception as e:
            print(f"[warn] 新浪快照第{attempt+1}次失败: {e}", file=sys.stderr)
            time.sleep(2)
    if df is not None and not df.empty:
        spot_count = len(df)
        df = df.sort_values("涨跌幅", ascending=False)
        top = df.head(top_n)
        for _, r in top.iterrows():
            code = _norm_sym(str(r.get("代码", "")))
            if not code or code.startswith("bj"):
                continue
            pool.append({
                "code": code,
                "name": str(r.get("名称", "")),
                "sector": "",
                "chg_today": _safe_float(r.get("涨跌幅")),
                "close": _safe_float(r.get("最新价")),
                "amount_yi": _safe_float(r.get("成交额")) / 1e8 if r.get("成交额") is not None else None,
                "mcap_yi": None, "pe": None, "turnover": None,
            })
        hot = [{"name": str(r.get("名称", "")), "chg": _safe_float(r.get("涨跌幅"))}
               for _, r in top.head(5).iterrows()]
        sectors = [{"name": f"涨幅榜#{i+1} {h['name']}", "chg": h["chg"]} for i, h in enumerate(hot)]
    else:
        print("[warn] 新浪快照连续失败，尝试新浪行业板块领涨股", file=sys.stderr)
        source = "新浪板块领涨"
        try:
            import akshare as ak
            bd = ak.stock_sector_spot(indicator="新浪行业")
            if bd is not None and not bd.empty:
                bd = bd.sort_values("涨跌幅", ascending=False)
                for _, r in bd.head(10).iterrows():
                    code = _norm_sym(str(r.get("股票代码", "")))
                    if not code or code.startswith("bj"):
                        continue
                    pool.append({
                        "code": code,
                        "name": str(r.get("股票名称", "")),
                        "sector": str(r.get("板块", "")),
                        "chg_today": _safe_float(r.get("个股-涨跌幅")),
                        "close": _safe_float(r.get("个股-当前价")),
                        "amount_yi": None, "mcap_yi": None, "pe": None, "turnover": None,
                    })
                sectors = [{"name": str(r.get("板块", "")), "chg": _safe_float(r.get("涨跌幅"))}
                           for _, r in bd.head(5).iterrows()]
        except Exception as e2:
            print(f"[warn] 新浪板块也失败({e2})，使用预设龙头池", file=sys.stderr)
            source = "预设龙头池"
            for code, name, sector in FALLBACK_POOL:
                pool.append({"code": code, "name": name, "sector": sector,
                             "chg_today": None, "close": None, "amount_yi": None,
                             "mcap_yi": None, "pe": None, "turnover": None})

    # 叠加预设池（确保覆盖面，去重）
    seen = {p["code"] for p in pool}
    for code, name, sector in FALLBACK_POOL:
        if code not in seen:
            seen.add(code)
            pool.append({"code": code, "name": name, "sector": sector,
                         "chg_today": None, "close": None, "amount_yi": None,
                         "mcap_yi": None, "pe": None, "turnover": None})
    return {"pool": pool, "sectors": sectors, "source": source, "spot_count": spot_count}


# ---------------------------------------------------------------------------
# 腾讯批量行情（补市值/PE/换手/成交额/当日涨幅/最新价）
# ---------------------------------------------------------------------------

def fetch_tencent_quotes(pool: list, batch: int = 40) -> dict:
    """批量拉腾讯行情，返回 {code: {...}}"""
    out = {}
    codes = [p["code"] for p in pool]
    for i in range(0, len(codes), batch):
        chunk = codes[i:i + batch]
        q = ",".join(_tx_symbol(c) for c in chunk)
        url = f"http://qt.gtimg.cn/q={q}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                txt = r.read().decode("gbk", errors="ignore")
            for line in txt.split(";"):
                line = line.strip()
                if not line.startswith("v_"):
                    continue
                try:
                    body = line.split("=", 1)[1].strip('"')
                    f = body.split("~")
                    # 字段：1名称 2代码 3最新价 4昨收 5今开 6成交量 30时间 31涨跌幅 32最高 33最低
                    # 37成交额(手?) 38换手率 39PE 43成交额(万) 44总市值(亿) 45流通市值(亿)
                    code = _norm_sym(f[2])
                    out[code] = {
                        "name": f[1],
                        "close": _safe_float(f[3]),
                        "chg_today": _safe_float(f[32]),       # 涨跌幅%
                        "turnover": _safe_float(f[38]),        # 换手率%
                        "pe": _safe_float(f[39]),              # PE(TTM)
                        "amount_yi": _safe_float(f[37]) / 1e4 if len(f) > 37 and f[37] and _safe_float(f[37]) is not None else None,  # 成交额(万元)->亿
                        "mcap_yi": _safe_float(f[45]) if len(f) > 45 else None,  # 总市值(亿)
                    }
                except (IndexError, ValueError):
                    continue
        except Exception as e:
            print(f"[warn] 腾讯行情批次 {i//batch+1} 失败: {e}", file=sys.stderr)
        time.sleep(0.3)
    return out


# ---------------------------------------------------------------------------
# 日线行情（新浪）
# ---------------------------------------------------------------------------

def fetch_kline(symbol: str) -> dict | None:
    sym = _norm_sym(symbol)
    start = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
    end = datetime.now().strftime("%Y%m%d")
    try:
        import akshare as ak
        df = ak.stock_zh_a_daily(symbol=_tx_symbol(sym),
                                 start_date=start, end_date=end, adjust="qfq")
    except Exception as e:
        return {"error": str(e)}
    if df is None or df.empty:
        return {"error": "no data"}
    records = []
    for _, row in df.iterrows():
        records.append({
            "date": str(row.get("date", "")),
            "close": _safe_float(row.get("close")),
            "amount": _safe_float(row.get("amount")),
        })
    return {"source": "sina", "prices": records}


def analyze(sym: str) -> dict | None:
    out = fetch_kline(sym)
    if not out or "error" in out:
        return out
    recs = out.get("prices") or []
    if len(recs) < 70:
        return {"error": "too short"}
    closes = [r["close"] for r in recs]
    amounts = [r.get("amount") or 0 for r in recs]
    last = closes[-1]

    def ma(n):
        return sum(closes[-n:]) / n if len(closes) >= n else None

    ma5, ma20, ma60 = ma(5), ma(20), ma(60)
    ma20_prev = sum(closes[-25:-5]) / 20 if len(closes) >= 25 else None

    e12 = _ema_series(closes, 12)
    e26 = _ema_series(closes, 26)
    dif_s = [a - b for a, b in zip(e12, e26)]
    dea_s = _ema_series(dif_s, 9)
    dif_now, dea_now = dif_s[-1], dea_s[-1]
    dif_prev, dea_prev = dif_s[-2], dea_s[-2]

    amt_now = amounts[-1]
    amt_prev5 = sum(amounts[-6:-1]) / 5 if len(amounts) >= 6 else 0
    vol_ratio = amt_now / amt_prev5 if amt_prev5 > 0 else 0
    chg20 = (last / closes[-21] - 1) * 100 if len(closes) >= 21 else None
    chg5 = (last / closes[-6] - 1) * 100 if len(closes) >= 6 else None

    return {
        "close": round(last, 2), "ma5": round(ma5, 2) if ma5 else None,
        "ma20": round(ma20, 2) if ma20 else None, "ma60": round(ma60, 2) if ma60 else None,
        "ma20_up": bool(ma20 and ma20_prev and ma20 > ma20_prev),
        "above_ma20": bool(ma20 and last > ma20),
        "above_ma5": bool(ma5 and last > ma5),
        "macd_gold": bool(dif_prev <= dea_prev and dif_now > dea_now),
        "macd_above": bool(dif_now > dea_now),
        "vol_ratio": round(vol_ratio, 2),
        "chg20": round(chg20, 2) if chg20 else None,
        "chg5": round(chg5, 2) if chg5 else None,
        "last_date": recs[-1]["date"],
        "source": out.get("source"),
    }


# ---------------------------------------------------------------------------
# 风格判定（与本地口径一致）
# ---------------------------------------------------------------------------

def classify(r: dict) -> list:
    styles = []
    if r.get("error"):
        return styles
    mcap = r.get("mcap_yi")
    if mcap is not None and mcap >= 100 and r.get("above_ma20") and (r.get("ma20_up") or r.get("macd_above")):
        styles.append("趋势波段")
    if (r.get("vol_ratio") or 0) >= 1.5 and (r.get("chg5") or 0) >= 8 and r.get("above_ma5"):
        styles.append("龙头惯性")
    pe = r.get("pe")
    if pe is not None and 0 < pe < 35 and r.get("above_ma20") and r.get("macd_above"):
        styles.append("基本面共振")
    if (r.get("chg_today") or 0) >= 9.5:
        styles.append("涨停候选")
    return styles


# ---------------------------------------------------------------------------
# 推送
# ---------------------------------------------------------------------------

def push_feishu(text: str) -> dict:
    url = os.environ.get("FEISHU_WEBHOOK_URL", "").strip()
    if not url:
        raise RuntimeError("FEISHU_WEBHOOK_URL 未配置")
    payload = json.dumps({"msg_type": "text", "content": {"text": text}}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def build_message(result: dict) -> str:
    d = result["date_str"]
    wk = result["weekday_cn"]
    secs = result["sectors"]
    stats = result["group_stats"]
    multi = result["multi_style"]
    src = result["pool_source"]
    lines = []
    lines.append(f"📊 游资逻辑选股 {d}（{wk}）")
    lines.append("━━━━━━━━━━━━━━")
    if secs:
        sec_str = " ｜ ".join(f"{s['name']}{(' +%.2f%%' % s['chg']) if s.get('chg') is not None else ''}" for s in secs[:5])
        lines.append(f"🎯 主线参考：{sec_str}")
    lines.append(f"🧪 命中统计：趋势波段 {stats.get('trend_band',0)} ｜ 龙头惯性 {stats.get('dragon_inertia',0)} ｜ 基本面共振 {stats.get('fundamental',0)} ｜ 涨停 {stats.get('limit_up',0)}")
    lines.append("━━━━━━━━━━━━━━")
    if multi:
        lines.append("🔥 多风格共振（重点）")
        for i, s in enumerate(multi[:12], 1):
            styles = "、".join(s["styles"])
            mcap = f"市值{s['mcap_yi']:.0f}亿" if s.get("mcap_yi") else ""
            pe = f"PE{s['pe']:.1f}" if s.get("pe") else ""
            vr = f"量比{s['vol_ratio']:.2f}" if s.get("vol_ratio") else ""
            chg = f"{s['chg_today']:+.2f}%" if s.get("chg_today") is not None else ""
            lines.append(f"{i}. {s['name']} {s['code']} {s['sector'] or ''} {chg} {mcap} {pe} {vr}【{s['style_count']}风格】")
    else:
        lines.append("今日候选池未命中多风格共振标的（数据或口径见备注）")
    lines.append("━━━━━━━━━━━━━━")
    lines.append(f"📌 候选池来源：{src}；数据：新浪快照/腾讯行情/新浪日线")
    lines.append("口径：趋势波段=市值≥100亿+站上MA20+均线向上或MACD多头；龙头惯性=量比≥1.5+5日涨≥8%+站上MA5；基本面共振=0<PE<35+站上MA20+MACD多头；涨停=当日≥9.5%。")
    lines.append("⚠️ 以上内容为 AI 自动生成，仅供信息整理与投研辅助参考，不构成任何投资建议。历史表现不代表未来，请独立判断。")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-push", action="store_true", help="只计算不推送")
    ap.add_argument("--date", default=None, help="指定日期 YYYY-MM-DD（测试用）")
    ap.add_argument("--top", type=int, default=120, help="涨幅榜候选数量")
    args = ap.parse_args()

    now = datetime.now()
    if args.date:
        try:
            now = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            pass

    if not _is_trading_day(now):
        print(f"NOT_TRADING_DAY {now.strftime('%Y-%m-%d')}")
        return

    print("== 1/5 获取候选池 ==")
    pool_data = fetch_pool(top_n=args.top)
    pool = pool_data["pool"]
    print(f"候选池 {len(pool)} 只（来源:{pool_data['source']}，快照{pool_data.get('spot_count')}只）")
    print("主线参考:", json.dumps(pool_data["sectors"], ensure_ascii=False))

    print("== 2/5 腾讯行情补字段 ==")
    quotes = fetch_tencent_quotes(pool)
    print(f"腾讯行情成功 {len(quotes)}/{len(pool)}")
    for p in pool:
        q = quotes.get(p["code"])
        if q:
            p.update(q)

    print("== 3/5 计算技术指标 ==")
    results = []
    def work(p):
        tech = analyze(p["code"])
        if tech and "error" in tech:
            p["error"] = tech["error"]
        else:
            p.update(tech or {})
        p["styles"] = classify(p)
        p["style_count"] = len(p["styles"])
        return p
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(work, p): p for p in pool}
        for i, fut in enumerate(as_completed(futs), 1):
            p = fut.result()
            results.append(p)
            if i % 30 == 0:
                print(f"  进度 {i}/{len(pool)}")

    ok = [r for r in results if "error" not in r]
    print(f"成功 {len(ok)}，失败 {len(results) - len(ok)}")

    print("== 4/5 风格分组 ==")
    groups = {
        "trend_band": [r["code"] for r in ok if "趋势波段" in r["styles"]],
        "dragon_inertia": [r["code"] for r in ok if "龙头惯性" in r["styles"]],
        "fundamental": [r["code"] for r in ok if "基本面共振" in r["styles"]],
        "limit_up": [r["code"] for r in ok if "涨停候选" in r["styles"]],
    }
    multi = sorted([r for r in ok if r["style_count"] >= 2], key=lambda x: -x["style_count"])
    stats = {k: len(v) for k, v in groups.items()}
    print("分组:", json.dumps(stats, ensure_ascii=False))
    print("共振:", [(r["code"], r["name"], r["style_count"]) for r in multi])

    result = {
        "date_str": now.strftime("%Y-%m-%d"),
        "weekday_cn": "一二三四五六日"[now.weekday()],
        "sectors": pool_data["sectors"],
        "pool_source": pool_data["source"],
        "spot_count": pool_data.get("spot_count"),
        "quote_ok": len(quotes),
        "group_stats": stats,
        "multi_style": multi,
        "stocks": results,
    }

    with open("youzi_daily_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)

    msg = build_message(result)
    print("== 5/5 推送 ==")
    print("----消息预览----")
    print(msg)
    print("----------------")
    if args.no_push:
        print("PUSH_SKIPPED")
        return
    resp = push_feishu(msg)
    print("PUSH_RESPONSE:", json.dumps(resp, ensure_ascii=False))
    if resp.get("StatusCode") != 0 and resp.get("code") != 0:
        print("PUSH_FAILED", file=sys.stderr)
        sys.exit(1)
    print("DONE")


if __name__ == "__main__":
    main()
