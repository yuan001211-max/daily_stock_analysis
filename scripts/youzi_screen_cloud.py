# -*- coding: utf-8 -*-
"""
游资逻辑每日选股（GitHub Actions 云端自包含版）
================================================
不依赖豆包环境，纯 Python 运行：
1. 动态获取当日主线板块（东财行业板块涨幅榜 → 领涨板块成分股龙头）
2. 对候选池计算 MA20/MACD/量比/阶段涨幅（akshare 东财优先、新浪兜底）
3. 按四大游资派系风格透明判定分组
4. 生成推送文本，POST 到飞书群 webhook（环境变量 FEISHU_WEBHOOK_URL）

用法: python youzi_screen_cloud.py [--no-push] [--date YYYY-MM-DD]
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
# 预设兜底候选池（板块接口全挂时使用；与本地实测池一致，65 只有效）
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
    ("002475", "立讯精密", "消费电子"), ("300124", "汇川技术", "工控"),
    ("002008", "大族激光", "激光设备"), ("601100", "恒立液压", "工程机械"),
    ("300124", "汇川技术", "工控"), ("002415", "海康威视", "安防"),
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
    ("688017", "绿的谐波", "机器人"), ("300124", "汇川技术", "机器人"),
    ("002747", "埃斯顿", "机器人"), ("688169", "石头科技", "机器人"),
    ("601127", "赛力斯", "新能源车"), ("000338", "潍柴动力", "工程机械"),
]

# 板块兜底名单（东财板块榜失败时拉这些板块成分股）
FALLBACK_SECTORS = [
    "风电设备", "光伏设备", "锂电池", "半导体", "工业母机",
    "机器人概念", "人工智能", "算力概念", "汽车整车", "特高压",
]

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _norm_sym(symbol: str) -> str:
    return symbol.strip().replace(".SH", "").replace(".SZ", "").replace(".BJ", "").zfill(6)


def _sym_prefix(symbol: str) -> str:
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
        if v is None or v == "" or v == "-":
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
    """东财交易日历判断；接口失败时退化为周一至周五判断"""
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
# 候选池获取
# ---------------------------------------------------------------------------

def fetch_pool() -> dict:
    """返回 {pool: [...], sectors: [...], source: str}"""
    pool = []
    sectors = []
    source = "板块榜"
    try:
        import akshare as ak
        df = ak.stock_board_industry_name_em()
        if df is None or df.empty:
            raise RuntimeError("板块榜为空")
        df = df.sort_values("涨跌幅", ascending=False)
        top = df.head(4)
        for _, row in top.iterrows():
            name = str(row.get("板块名称", "")).strip()
            chg = _safe_float(row.get("涨跌幅"))
            sectors.append({"name": name, "chg": chg})
            try:
                cons = ak.stock_board_industry_cons_em(symbol=name)
                if cons is None or cons.empty:
                    continue
                cons = cons.sort_values("涨跌幅", ascending=False)
                for _, c in cons.head(15).iterrows():
                    pool.append({
                        "code": _norm_sym(str(c.get("代码", ""))),
                        "name": str(c.get("名称", "")),
                        "sector": name,
                        "chg_today": _safe_float(c.get("涨跌幅")),
                        "mcap_yi": _safe_float(c.get("总市值")) / 1e8 if c.get("总市值") is not None else None,
                        "pe": _safe_float(c.get("市盈率-动态")),
                        "turnover": _safe_float(c.get("换手率")),
                        "amount_yi": _safe_float(c.get("成交额")) / 1e8 if c.get("成交额") is not None else None,
                        "close": _safe_float(c.get("最新价")),
                    })
                time.sleep(0.5)
            except Exception as e:
                print(f"  [warn] 板块 {name} 成分获取失败: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[warn] 东财板块榜失败({e})，改用兜底板块", file=sys.stderr)
        source = "兜底板块"
        try:
            import akshare as ak
            for name in FALLBACK_SECTORS:
                cons = ak.stock_board_industry_cons_em(symbol=name)
                if cons is None or cons.empty:
                    continue
                cons = cons.sort_values("涨跌幅", ascending=False)
                sectors.append({"name": name, "chg": None})
                for _, c in cons.head(12).iterrows():
                    pool.append({
                        "code": _norm_sym(str(c.get("代码", ""))),
                        "name": str(c.get("名称", "")),
                        "sector": name,
                        "chg_today": _safe_float(c.get("涨跌幅")),
                        "mcap_yi": _safe_float(c.get("总市值")) / 1e8 if c.get("总市值") is not None else None,
                        "pe": _safe_float(c.get("市盈率-动态")),
                        "turnover": _safe_float(c.get("换手率")),
                        "amount_yi": _safe_float(c.get("成交额")) / 1e8 if c.get("成交额") is not None else None,
                        "close": _safe_float(c.get("最新价")),
                    })
                time.sleep(0.5)
        except Exception as e2:
            print(f"[warn] 兜底板块也失败({e2})，使用预设龙头池", file=sys.stderr)
            source = "预设龙头池"
            for code, name, sector in FALLBACK_POOL:
                pool.append({"code": code, "name": name, "sector": sector,
                             "chg_today": None, "mcap_yi": None, "pe": None,
                             "turnover": None, "amount_yi": None, "close": None})

    # 去重
    seen = set()
    dedup = []
    for p in pool:
        if p["code"] and p["code"] not in seen:
            seen.add(p["code"])
            dedup.append(p)
    return {"pool": dedup, "sectors": sectors, "source": source}


# ---------------------------------------------------------------------------
# 日线行情
# ---------------------------------------------------------------------------

def fetch_kline(symbol: str) -> dict | None:
    """拉取日线（东财优先、新浪兜底），返回 records 列表或 None"""
    sym = _norm_sym(symbol)
    start = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
    end = datetime.now().strftime("%Y%m%d")
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist(symbol=sym, period="daily",
                                start_date=start, end_date=end, adjust="qfq")
        src = "em"
    except Exception:
        df = None
        src = None
    if df is None or df.empty:
        try:
            import akshare as ak
            df = ak.stock_zh_a_daily(symbol=_sym_prefix(sym),
                                     start_date=start, end_date=end, adjust="qfq")
            src = "sina"
        except Exception as e:
            return {"error": str(e)}
    if df is None or df.empty:
        return {"error": "no data"}
    records = []
    for _, row in df.iterrows():
        if src == "sina":
            records.append({
                "date": str(row.get("date", "")),
                "close": _safe_float(row.get("close")),
                "amount": _safe_float(row.get("amount")),
            })
        else:
            records.append({
                "date": str(row.get("日期", "")),
                "close": _safe_float(row.get("收盘")),
                "amount": _safe_float(row.get("成交额")),
            })
    return {"source": src, "prices": records}


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
    # 趋势波段：市值≥100亿 + 站上MA20 + (MA20向上 或 MACD多头)
    mcap = r.get("mcap_yi")
    if mcap is not None and mcap >= 100 and r.get("above_ma20") and (r.get("ma20_up") or r.get("macd_above")):
        styles.append("趋势波段")
    # 龙头惯性：量比≥1.5 + 5日涨幅≥8% + 站上MA5
    if (r.get("vol_ratio") or 0) >= 1.5 and (r.get("chg5") or 0) >= 8 and r.get("above_ma5"):
        styles.append("龙头惯性")
    # 基本面共振：0<PE<35 + 站上MA20 + MACD多头
    pe = r.get("pe")
    if pe is not None and 0 < pe < 35 and r.get("above_ma20") and r.get("macd_above"):
        styles.append("基本面共振")
    # 涨停候选：当日涨幅≥9.5%
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
        sec_str = " ｜ ".join(f"{s['name']}{(' +%.2f%%' % s['chg']) if s.get('chg') is not None else ''}" for s in secs[:4])
        lines.append(f"🎯 主线板块：{sec_str}")
    lines.append(f"🧪 命中统计：趋势波段 {stats.get('trend_band',0)} ｜ 龙头惯性 {stats.get('dragon_inertia',0)} ｜ 基本面共振 {stats.get('fundamental',0)} ｜ 涨停 {stats.get('limit_up',0)}")
    lines.append("━━━━━━━━━━━━━━")
    if multi:
        lines.append("🔥 多风格共振（重点）")
        for i, s in enumerate(multi[:12], 1):
            styles = "、".join(s["styles"])
            mcap = f"市值{s['mcap_yi']:.1f}亿" if s.get("mcap_yi") else ""
            pe = f"PE{s['pe']:.1f}" if s.get("pe") else ""
            vr = f"量比{s['vol_ratio']:.2f}" if s.get("vol_ratio") else ""
            chg = f"{s['chg_today']:+.2f}%" if s.get("chg_today") is not None else ""
            lines.append(f"{i}. {s['name']} {s['code']} {s['sector']} {chg} {mcap} {pe} {vr}【{s['style_count']}风格】")
    else:
        lines.append("今日候选池未命中多风格共振标的（数据或口径见备注）")
    lines.append("━━━━━━━━━━━━━━")
    lines.append(f"📌 候选池来源：{src}；数据：东财/新浪日线")
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
    args = ap.parse_args()

    now = datetime.now()
    if args.date:
        try:
            now = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            pass

    # 交易日检查
    if not _is_trading_day(now):
        print(f"NOT_TRADING_DAY {now.strftime('%Y-%m-%d')}")
        return

    print("== 1/4 获取候选池 ==")
    pool_data = fetch_pool()
    pool = pool_data["pool"]
    print(f"候选池 {len(pool)} 只（来源:{pool_data['source']}）")
    print("主线板块:", json.dumps(pool_data["sectors"], ensure_ascii=False))

    print("== 2/4 计算技术指标 ==")
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
            if i % 15 == 0:
                print(f"  进度 {i}/{len(pool)}")

    ok = [r for r in results if "error" not in r]
    print(f"成功 {len(ok)}，失败 {len(results) - len(ok)}")

    print("== 3/4 风格分组 ==")
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
        "group_stats": stats,
        "multi_style": multi,
        "stocks": results,
    }

    # 保存 JSON
    with open("youzi_daily_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)

    msg = build_message(result)
    print("== 4/4 推送 ==")
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
