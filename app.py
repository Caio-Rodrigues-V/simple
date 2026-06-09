import os
import io
import time
import threading
import requests
import pandas as pd
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN", "")
HEADERS = {"Authorization": CLICKUP_TOKEN}

LIST_PRIORITY = {
    "Unique Funnel":     0,
    "Activation Funnel": 1,
    "Migration Funnel":  2,
    "Completed Orders":  3,
}

SIGNED_STATUSES  = {"signed", "complete", "contract signed", "contract signature"}
SQL_STATUSES     = {"negotiation", "validation", "signed", "complete", "contract signed",
                    "contract signature", "realized meeting", "schedule meeting",
                    "proposal presentation", "credit analysis", "waiting contract",
                    "legal form", "deliberation/committee"}
MEETING_STATUSES = {"realized meeting", "schedule meeting", "negotiation", "validation",
                    "signed", "complete", "contract signed", "contract signature",
                    "proposal presentation", "credit analysis", "waiting contract",
                    "legal form", "deliberation/committee"}

DATE_FROM_TS = 1735689600000
CAREER_STAGE_MAP = {0:1,1:2,2:3,3:4,4:5,5:6,6:7,7:8,8:9,9:10}
CACHE_TTL = 300

_cache = {"data": None, "ts": 0, "loading": False}
_growth_cache = {"data": None}

def get_field(cf, fid):
    for f in cf:
        if f["id"] == fid:
            return f.get("value")
    return None

def get_dropdown_name(cf, fid):
    for f in cf:
        if f["id"] == fid:
            val = f.get("value")
            if val is None:
                return None
            options = f.get("type_config", {}).get("options", [])
            match = next((o for o in options if o.get("orderindex") == val), None)
            return match.get("name") if match else None
    return None

def normalize_region(country):
    if not country:
        return "BR"
    c = country.strip().upper()
    if c in ("BR", "BRAZIL", "BRASIL"):
        return "BR"
    if c in ("US", "USA", "UNITED STATES"):
        return "US"
    return "ROW"

def ms_to_date(ms_val):
    if not ms_val:
        return None
    try:
        return datetime.fromtimestamp(int(ms_val) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except:
        return None

def transform_task(task, list_name):
    cf     = task.get("custom_fields", [])
    status = (task.get("status", {}).get("status") or "").lower().strip()

    stage_raw    = get_field(cf, "a6b9b406-10f2-40ef-a087-c30c4f84d1ed")
    career_stage = CAREER_STAGE_MAP.get(stage_raw) if stage_raw is not None else None
    executive    = get_dropdown_name(cf, "32498777-fbec-4cd8-90dc-84682016fce3")
    source       = get_dropdown_name(cf, "d31fe5b1-7ade-4e93-b00f-b0d1318a9870")
    currency     = get_dropdown_name(cf, "131a2100-419c-433c-8eea-0c4255e4a34b") or "USD"

    advance_raw = get_field(cf, "25807731-2bf7-4d07-aa0d-6cb64c6f474b")
    try:
        advance = float(advance_raw) if advance_raw is not None else None
    except:
        advance = None

    country      = get_field(cf, "ad90d5ff-539f-46d8-9e76-088399711f05")
    region       = normalize_region(country)
    created_at   = ms_to_date(task.get("date_created"))
    closing_date = ms_to_date(get_field(cf, "824950f2-67b0-4fca-a9e5-7e40d50dabef"))
    new_lead_date= ms_to_date(get_field(cf, "cb73bc46-c08e-4f33-b2cf-c118b2a63683"))

    lead_time = None
    if closing_date and new_lead_date:
        try:
            d1 = datetime.strptime(closing_date, "%Y-%m-%d")
            d2 = datetime.strptime(new_lead_date, "%Y-%m-%d")
            lead_time = (d1 - d2).days
            if lead_time < 0:
                lead_time = None
        except:
            pass

    meetings_raw = get_field(cf, "8ae02492-f8d1-4e75-bb2c-19114cc0c7a9")
    try:
        meetings = int(meetings_raw) if meetings_raw is not None else 0
    except:
        meetings = 0

    is_signed  = status in SIGNED_STATUSES
    is_sql     = status in SQL_STATUSES
    is_meeting = status in MEETING_STATUSES or meetings > 0
    is_eligible= False
    if career_stage:
        is_eligible = career_stage >= 3 if region == "US" else career_stage >= 4
    is_mql = is_eligible

    email_raw = get_field(cf, "1bf8f12f-0b09-45bc-9824-24198ad3e116")
    email = email_raw.strip().lower() if email_raw else None

    return {
        "task_id":       task["id"],
        "name":          task.get("name", ""),
        "email":         email,
        "status":        task.get("status", {}).get("status", ""),
        "status_color":  task.get("status", {}).get("color", "#666"),
        "executive":     executive,
        "source":        source,
        "country":       country,
        "region":        region,
        "career_stage":  career_stage,
        "advance":       advance,
        "currency":      currency,
        "closing_date":  closing_date,
        "new_lead_date": new_lead_date,
        "created_at":    created_at,
        "lead_time":     lead_time,
        "meetings":      meetings,
        "list":          list_name,
        "is_signed":     is_signed,
        "is_sql":        is_sql,
        "is_meeting":    is_meeting,
        "is_mql":        is_mql,
        "is_eligible":   is_eligible,
        "clickup_url":   task.get("url", ""),
    }

def fetch_list(list_id, list_name):
    tasks = []
    page  = 0
    while True:
        url = (
            f"https://api.clickup.com/api/v2/list/{list_id}/task"
            f"?page={page}&include_closed=true&subtasks=false"
            f"&date_created_gt={DATE_FROM_TS}"
        )
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60)
            if resp.status_code != 200:
                print(f"[SYNC] Erro {resp.status_code} em {list_name}")
                break
            data = resp.json()
        except Exception as e:
            print(f"[SYNC] Erro em {list_name} pg {page}: {e}")
            break
        batch = data.get("tasks", [])
        if not batch:
            break
        for t in batch:
            tasks.append(transform_task(t, list_name))
        if data.get("last_page", True):
            break
        page += 1
        time.sleep(0.3)
    return tasks

def _do_sync(force=False):
    global _cache
    now = time.time()
    if not force and _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]
    if _cache["loading"]:
        return _cache["data"]

    _cache["loading"] = True
    target = {
        "Unique Funnel":     "901324657489",
        "Migration Funnel":  "901002241743",
        "Activation Funnel": "901317185660",
        "Completed Orders":  "901002241760",
    }
    all_tasks = []
    for name, lid in target.items():
        print(f"[SYNC] Buscando {name}...")
        all_tasks.extend(fetch_list(lid, name))
        print(f"[SYNC] {name} OK — {len(all_tasks)} acumuladas")

    seen = {}
    for t in all_tasks:
        tid = t["task_id"]
        if tid not in seen:
            seen[tid] = t
        else:
            curr_p = LIST_PRIORITY.get(seen[tid]["list"], 0)
            new_p  = LIST_PRIORITY.get(t["list"], 0)
            if new_p > curr_p:
                seen[tid] = t

    unique = list(seen.values())
    print(f"[SYNC] Total: {len(unique)} deals")
    _cache = {"data": unique, "ts": time.time(), "loading": False}
    return unique

def get_all_deals(force=False):
    return _do_sync(force=force)

def apply_filters(deals, args):
    executive  = args.get("executive")
    status     = args.get("status")
    region     = args.get("region")
    source     = args.get("source")
    list_name  = args.get("list")
    date_from  = args.get("date_from")
    date_to    = args.get("date_to")
    date_field = args.get("date_field", "created_at")

    if executive:  deals = [t for t in deals if t["executive"] == executive]
    if status:     deals = [t for t in deals if t["status"].lower() == status.lower()]
    if region:     deals = [t for t in deals if t["region"] == region]
    if source:     deals = [t for t in deals if t["source"] == source]
    if list_name:  deals = [t for t in deals if t["list"] == list_name]
    if date_from:  deals = [t for t in deals if t.get(date_field) and t[date_field] >= date_from]
    if date_to:    deals = [t for t in deals if t.get(date_field) and t[date_field] <= date_to]
    return deals

def compute_stats(deals):
    total        = len(deals)
    signed       = [t for t in deals if t["is_signed"]]
    sqls         = [t for t in deals if t["is_sql"]]
    meetings     = [t for t in deals if t["is_meeting"]]
    mqls         = [t for t in deals if t["is_mql"]]
    with_advance = [t for t in deals if t["advance"] and t["advance"] > 0]

    total_advance  = sum(t["advance"] for t in with_advance)
    pipeline_total = sum(t["advance"] for t in deals if t["advance"] and t["advance"] > 0 and not t["is_signed"])
    advance_signed = sum(t["advance"] for t in signed if t["advance"] and t["advance"] > 0)
    avg_deal       = (advance_signed / len(signed)) if signed else 0

    lead_times    = [t["lead_time"] for t in signed if t["lead_time"] is not None]
    avg_lead_time = (sum(lead_times) / len(lead_times)) if lead_times else None

    close_rate       = (len(signed) / len(sqls) * 100)     if sqls     else 0
    lead_to_sql      = (len(sqls)   / total * 100)          if total    else 0
    sql_to_meeting   = (len(meetings)/ len(sqls) * 100)     if sqls     else 0
    meeting_to_close = (len(signed)  / len(meetings) * 100) if meetings else 0

    status_dist = {}
    for t in deals:
        s = t["status"]
        status_dist[s] = status_dist.get(s, 0) + 1

    exec_stats = {}
    for t in deals:
        ex = t["executive"] or "Sem executivo"
        if ex not in exec_stats:
            exec_stats[ex] = {"leads":0,"signed":0,"meetings":0,"pipeline":0,"advance_signed":0,"lead_times":[]}
        exec_stats[ex]["leads"] += 1
        if t["is_signed"]:
            exec_stats[ex]["signed"] += 1
            if t["advance"]: exec_stats[ex]["advance_signed"] += t["advance"]
        if t["is_meeting"]: exec_stats[ex]["meetings"] += 1
        if t["advance"] and not t["is_signed"]: exec_stats[ex]["pipeline"] += t["advance"]
        if t["lead_time"] is not None: exec_stats[ex]["lead_times"].append(t["lead_time"])
    for ex in exec_stats:
        lt = exec_stats[ex]["lead_times"]
        exec_stats[ex]["avg_lead_time"] = round(sum(lt)/len(lt),1) if lt else None
        exec_stats[ex]["close_rate"]    = round(exec_stats[ex]["signed"]/exec_stats[ex]["leads"]*100,1)
        del exec_stats[ex]["lead_times"]

    source_stats = {}
    for t in deals:
        src = t["source"] or "Unknown"
        if src not in source_stats:
            source_stats[src] = {"leads":0,"signed":0,"sqls":0,"pipeline":0}
        source_stats[src]["leads"] += 1
        if t["is_signed"]: source_stats[src]["signed"] += 1
        if t["is_sql"]:    source_stats[src]["sqls"]   += 1
        if t["advance"] and not t["is_signed"]: source_stats[src]["pipeline"] += t["advance"]
    for src in source_stats:
        l = source_stats[src]["leads"]
        source_stats[src]["close_rate"] = round(source_stats[src]["signed"]/l*100,1) if l else 0
        source_stats[src]["sql_rate"]   = round(source_stats[src]["sqls"]/l*100,1)   if l else 0

    region_stats = {}
    for t in deals:
        r = t["region"]
        if r not in region_stats:
            region_stats[r] = {"leads":0,"signed":0,"meetings":0,"eligible":0,"pipeline":0,"lead_times":[]}
        region_stats[r]["leads"] += 1
        if t["is_signed"]:   region_stats[r]["signed"]   += 1
        if t["is_meeting"]:  region_stats[r]["meetings"]  += 1
        if t["is_eligible"]: region_stats[r]["eligible"]  += 1
        if t["advance"] and not t["is_signed"]: region_stats[r]["pipeline"] += t["advance"]
        if t["lead_time"] is not None: region_stats[r]["lead_times"].append(t["lead_time"])
    for r in region_stats:
        lt = region_stats[r]["lead_times"]
        region_stats[r]["avg_lead_time"] = round(sum(lt)/len(lt),1) if lt else None
        region_stats[r]["close_rate"]    = round(region_stats[r]["signed"]/region_stats[r]["leads"]*100,1) if region_stats[r]["leads"] else 0
        region_stats[r]["eligible_pct"]  = round(region_stats[r]["eligible"]/region_stats[r]["leads"]*100,1) if region_stats[r]["leads"] else 0
        del region_stats[r]["lead_times"]

    monthly = {}
    for t in deals:
        month = (t["created_at"] or "")[:7]
        if not month: continue
        if month not in monthly:
            monthly[month] = {"leads":0,"signed":0,"sqls":0,"meetings":0}
        monthly[month]["leads"] += 1
        if t["is_signed"]:  monthly[month]["signed"]  += 1
        if t["is_sql"]:     monthly[month]["sqls"]    += 1
        if t["is_meeting"]: monthly[month]["meetings"] += 1

    return {
        "summary": {
            "total_deals":      total,
            "signed":           len(signed),
            "sqls":             len(sqls),
            "meetings":         len(meetings),
            "mqls":             len(mqls),
            "with_advance":     len(with_advance),
            "total_advance":    total_advance,
            "pipeline_total":   pipeline_total,
            "advance_signed":   advance_signed,
            "avg_deal_size":    round(avg_deal, 2),
            "avg_lead_time":    round(avg_lead_time,1) if avg_lead_time else None,
            "close_rate":       round(close_rate, 1),
            "lead_to_sql":      round(lead_to_sql, 1),
            "sql_to_meeting":   round(sql_to_meeting, 1),
            "meeting_to_close": round(meeting_to_close, 1),
        },
        "funnel": {
            "total_leads": total,
            "eligible":    len([t for t in deals if t["is_eligible"]]),
            "mql":         len(mqls),
            "sql":         len(sqls),
            "meeting":     len(meetings),
            "signed":      len(signed),
        },
        "by_status":    status_dist,
        "by_executive": exec_stats,
        "by_source":    source_stats,
        "by_region":    region_stats,
        "monthly":      dict(sorted(monthly.items())),
    }

# ── ROUTES ────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(".", "strm-dashboard.html")

@app.route("/api/ready")
def ready():
    return jsonify({
        "ready":   _cache["data"] is not None,
        "loading": _cache["loading"],
        "total":   len(_cache["data"]) if _cache["data"] else 0,
    })

@app.route("/api/deals")
def deals():
    if not _cache["data"]:
        return jsonify({"total": 0, "deals": [], "loading": True})
    result = apply_filters(_cache["data"], request.args)
    return jsonify({"total": len(result), "deals": result, "loading": False})

@app.route("/api/stats")
def stats():
    if not _cache["data"]:
        return jsonify({"loading": True})
    deals = apply_filters(_cache["data"], request.args)
    return jsonify({**compute_stats(deals), "loading": False})

@app.route("/api/sync")
def sync():
    threading.Thread(target=lambda: _do_sync(force=True), daemon=True).start()
    return jsonify({"ok": True, "message": "Sync iniciado em background"})

@app.route("/api/filters")
def filters():
    return jsonify({
        "executives": ["Lucas Klen","Elias Francisco Jr","Michael Parton","George Odeh",
                       "Thaís Gonzalez","Eric Sanders","Greg","Alê Francisco",
                       "Guilherme","Mateus Gayoso","Carlos Lazzari"],
        "sources":    ["Meta ads","Smart list","Outreach/Network","Marketing",
                       "Clients referral","Partner referral","Website contact",
                       "Social media","Unknown","Others"],
        "regions":    ["BR","US","ROW"],
        "lists":      ["Unique Funnel","Migration Funnel","Activation Funnel","Completed Orders"],
    })

# ── GROWTH ────────────────────────────────────────────────

@app.route("/api/upload-growth", methods=["POST"])
def upload_growth():
    if "file" not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
    file = request.files["file"]
    if not file.filename.endswith((".xlsx", ".xls")):
        return jsonify({"error": "Formato inválido. Use .xlsx"}), 400

    df = pd.read_excel(io.BytesIO(file.read()))

    def parse_stage(val):
        if pd.isna(val) or str(val).strip() in ["-", " - ", ""]:
            return 1
        try:
            return int(str(val).split("-")[0].strip())
        except:
            return 1

    def parse_region(pais, regiao):
        if pd.isna(pais) or str(pais).strip() == "":
            return "BR"
        p = str(pais).strip().upper()
        if p in ("BR", "BRAZIL", "BRASIL"): return "BR"
        if p in ("US", "USA", "UNITED STATES"): return "US"
        return "ROW"

    new_records = []
    for _, row in df.iterrows():
        stage       = parse_stage(row.get("Estagio_Numerico"))
        region      = parse_region(row.get("Pais"), row.get("Regiao"))
        is_eligible = stage >= 3 if region == "US" else stage >= 4
        email       = str(row.get("E-mail", "") or "").lower().strip()
        new_records.append({
            "nome":         str(row.get("Nome", "") or ""),
            "email":        email,
            "regiao":       region,
            "pais":         str(row.get("Pais", "") or "BR"),
            "estagio":      stage,
            "genero":       str(row.get("Genero", "") or ""),
            "verificado":   str(row.get("Verificado", "") or "") == "Sim",
            "advance":      float(row.get("Valor do advance", 0) or 0),
            "data_cadastro":str(row.get("Data_Cadastro", "") or "")[:10],
            "month":        int(row.get("month", 0) or 0),
            "year":         int(row.get("year", 0) or 0),
            "is_eligible":  is_eligible,
        })

    existing = _growth_cache.get("data") or []
    existing_emails = {r["email"] for r in existing if r["email"]}
    added = 0
    for r in new_records:
        if r["email"] and r["email"] in existing_emails:
            continue
        existing.append(r)
        if r["email"]: existing_emails.add(r["email"])
        added += 1
    sem_email = [r for r in new_records if not r["email"]]
    existing.extend(sem_email)
    _growth_cache["data"] = existing

    return jsonify({
        "ok":    True,
        "total": len(existing),
        "added": added,
        "duplicates_skipped": len(new_records) - added - len(sem_email),
    })

@app.route("/api/growth-stats")
def growth_stats():
    records = _growth_cache.get("data")
    if not records:
        return jsonify({"error": "Nenhuma planilha carregada"}), 404

    total       = len(records)
    elegiveis   = [r for r in records if r["is_eligible"]]
    verificados = [r for r in records if r["verificado"]]
    com_advance = [r for r in records if r["advance"] > 0]

    by_region = {}
    for r in records:
        reg = r["regiao"]
        if reg not in by_region:
            by_region[reg] = {"total": 0, "elegiveis": 0}
        by_region[reg]["total"] += 1
        if r["is_eligible"]: by_region[reg]["elegiveis"] += 1

    by_stage = {}
    for r in records:
        s = r["estagio"]
        by_stage[s] = by_stage.get(s, 0) + 1

    by_genre = {}
    for r in records:
        g = r["genero"] or "Não informado"
        by_genre[g] = by_genre.get(g, 0) + 1
    by_genre = dict(sorted(by_genre.items(), key=lambda x: -x[1])[:15])

    by_month = {}
    for r in records:
        if r["year"] and r["month"]:
            key = f"{r['year']}-{str(r['month']).zfill(2)}"
            by_month[key] = by_month.get(key, 0) + 1

    return jsonify({
        "summary": {
            "total":          total,
            "elegiveis":      len(elegiveis),
            "verificados":    len(verificados),
            "com_advance":    len(com_advance),
            "elegivel_pct":   round(len(elegiveis)/total*100,1) if total else 0,
            "verificado_pct": round(len(verificados)/total*100,1) if total else 0,
        },
        "by_region": by_region,
        "by_stage":  dict(sorted(by_stage.items())),
        "by_genre":  by_genre,
        "by_month":  dict(sorted(by_month.items())),
        "records":   records,
    })

@app.route("/api/clear-growth", methods=["POST"])
def clear_growth():
    _growth_cache["data"] = []
    return jsonify({"ok": True})

# ── PRELOAD ───────────────────────────────────────────────

def preload():
    time.sleep(2)
    print("[PRELOAD] Iniciando...")
    try:
        _do_sync(force=True)
        print("[PRELOAD] Pronto.")
    except Exception as e:
        print(f"[PRELOAD] Erro: {e}")
        _cache["loading"] = False

threading.Thread(target=preload, daemon=True).start()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
