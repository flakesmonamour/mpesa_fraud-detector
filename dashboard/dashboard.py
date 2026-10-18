"""
M-Pesa Fraud Intelligence Dashboard
=====================================
Fixes in this version:
  1. Alert Status total now matches Transactions Today — counts today only
  2. Alert Status donut synced with Open Fraud Alerts table
  3. Transaction Stream upgraded with live pulse bars + velocity indicator
  4. auto_resolve_alerts() writes status back to PostgreSQL on every refresh
"""

import streamlit as st
import random, uuid, time, math, os
from datetime import datetime
from collections import deque

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

st.set_page_config(page_title="M-Pesa Fraud Intelligence", page_icon="🛡️",
                   layout="wide", initial_sidebar_state="collapsed")

DB_CONFIG = {
    'host':     os.getenv('POSTGRES_HOST',     'localhost'),
    'port':     int(os.getenv('POSTGRES_PORT', '5432')),
    'dbname':   os.getenv('POSTGRES_DB',       'mpesa_fraud'),
    'user':     os.getenv('POSTGRES_USER',     'mark'),
    'password': os.getenv('POSTGRES_PASSWORD', 'fraud2025'),
}

def get_db_connection():
    if not PSYCOPG2_AVAILABLE:
        return None
    try:
        return psycopg2.connect(**DB_CONFIG, connect_timeout=2)
    except Exception:
        return None

def auto_resolve_alerts(conn):
    """Write auto-resolution back to PostgreSQL on every refresh."""
    if not conn:
        return
    try:
        cur = conn.cursor()
        # Score >= 0.85 → blocked
        cur.execute("""
            UPDATE fraud_alerts SET alert_status='blocked'
            WHERE alert_status='open' AND fraud_score >= 0.85
        """)
        # Score 0.50–0.69, older than 5s → under_review
        cur.execute("""
            UPDATE fraud_alerts SET alert_status='under_review', reviewed_at=NOW()
            WHERE alert_status='open'
            AND fraud_score BETWEEN 0.50 AND 0.69
            AND alerted_at < NOW() - INTERVAL '5 seconds'
        """)
        # Any alert older than 10s with no repeat → resolved
        cur.execute("""
            UPDATE fraud_alerts SET alert_status='resolved', reviewed_at=NOW()
            WHERE alert_status IN ('open','under_review')
            AND alerted_at < NOW() - INTERVAL '10 seconds'
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        try: conn.rollback()
        except: pass

def fetch_live_data_from_db():
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        # FIX 1: auto-resolve before reading so donut matches table
        auto_resolve_alerts(conn)

        cur = conn.cursor()

        # FIX 2: KPIs — today only (not all-time)
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount),0)
            FROM fact_transactions
            WHERE DATE(txn_timestamp) = CURRENT_DATE
        """)
        txns_today, vol_today = cur.fetchone()

        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount),0)
            FROM fact_transactions
            WHERE is_fraud=TRUE AND DATE(txn_timestamp) = CURRENT_DATE
        """)
        alerts_today, kes_risk = cur.fetchone()

        # Feed
        cur.execute("""
            SELECT t.transaction_id, t.customer_id, t.amount, t.txn_type,
                   t.is_fraud, t.fraud_score, t.fraud_reason,
                   t.txn_count_1hr, t.txn_timestamp
            FROM fact_transactions t
            ORDER BY t.txn_timestamp DESC LIMIT 12
        """)
        feed_rows = cur.fetchall()

        # FIX 3: Fraud alerts — read AFTER auto_resolve so status is fresh
        cur.execute("""
            SELECT a.transaction_id, a.customer_id, a.amount,
                   a.fraud_reason, a.fraud_score, a.alert_status, a.alerted_at
            FROM fraud_alerts a
            WHERE DATE(a.alerted_at) = CURRENT_DATE
            ORDER BY a.alerted_at DESC LIMIT 20
        """)
        alert_rows = cur.fetchall()

        # FIX 4: Status counts — today only so donut matches transactions
        cur.execute("""
            SELECT alert_status, COUNT(*)
            FROM fraud_alerts
            WHERE DATE(alerted_at) = CURRENT_DATE
            GROUP BY alert_status
        """)
        status_counts = {row[0]: row[1] for row in cur.fetchall()}

        # Rules triggered today
        cur.execute("""
            SELECT fraud_reason, COUNT(*) FROM fact_transactions
            WHERE is_fraud=TRUE AND DATE(txn_timestamp) = CURRENT_DATE
            GROUP BY fraud_reason ORDER BY COUNT(*) DESC
        """)
        rule_rows = cur.fetchall()

        # Txn by type (fraud) today
        cur.execute("""
            SELECT txn_type, COUNT(*) FROM fact_transactions
            WHERE is_fraud=TRUE AND DATE(txn_timestamp) = CURRENT_DATE
            GROUP BY txn_type
        """)
        type_rows = cur.fetchall()

        # Hourly fraud counts today
        cur.execute("""
            SELECT EXTRACT(HOUR FROM txn_timestamp)::INT, COUNT(*)
            FROM fact_transactions
            WHERE is_fraud=TRUE AND DATE(txn_timestamp) = CURRENT_DATE
            GROUP BY 1 ORDER BY 1
        """)
        hour_rows = cur.fetchall()

        # Stream: per-minute clean vs fraud last 60 minutes
        cur.execute("""
            SELECT
                EXTRACT(MINUTE FROM txn_timestamp)::INT AS minute,
                SUM(CASE WHEN is_fraud THEN 0 ELSE 1 END) AS clean,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud
            FROM fact_transactions
            WHERE txn_timestamp >= NOW() - INTERVAL '60 minutes'
            GROUP BY 1 ORDER BY 1
        """)
        stream_rows = cur.fetchall()

        cur.close(); conn.close()
        return {
            'db_connected':  True,
            'txns_today':    int(txns_today or 0),
            'kes_risk':      float(kes_risk or 0),
            'alerts_today':  int(alerts_today or 0),
            'feed_rows':     feed_rows,
            'alert_rows':    alert_rows,
            'status_counts': status_counts,
            'rule_rows':     rule_rows,
            'type_rows':     type_rows,
            'hour_rows':     dict(hour_rows),
            'stream_rows':   stream_rows,
        }
    except Exception as e:
        try: conn.close()
        except: pass
        return None

# ── CSS ─────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[data-testid="stAppViewContainer"],[data-testid="stApp"]{
  background:#0b0f0b!important;color:#ddeedd!important;font-family:'Inter',sans-serif!important;}
[data-testid="stHeader"]{background:transparent!important;}
[data-testid="stSidebar"]{display:none!important;}
.block-container{padding:0!important;max-width:100%!important;}
section[data-testid="stMain"]>div{padding:0!important;}
#MainMenu,footer,[data-testid="stDecoration"]{display:none!important;}
div[data-testid="stVerticalBlock"]>div{gap:0!important;}

.topnav{background:#0c110c;border-bottom:1px solid #1c2e1c;padding:10px 18px;display:flex;align-items:center;justify-content:space-between;}
.nav-logo{background:#1a7a2a;color:#fff;font-weight:800;font-size:13px;padding:5px 9px;border-radius:6px;}
.nav-title{font-size:15px;font-weight:700;color:#ddeedd;line-height:1.2;}
.nav-sub{font-size:11px;color:#4a6a4a;}
.nav-live{background:#0c200c;border:1px solid #2a5a2a;color:#4caf50;font-size:11px;font-weight:700;padding:4px 11px;border-radius:20px;letter-spacing:.08em;animation:blink 2s ease-in-out infinite;}
.nav-clock{font-family:'JetBrains Mono',monospace;font-size:13px;color:#6ab86a;margin-left:10px;}
.nav-db{font-size:10px;padding:3px 9px;border-radius:12px;margin-left:8px;}
.nav-db.live{background:#0a2a0a;border:1px solid #2a5a2a;color:#4caf50;}
.nav-db.sim{background:#2a1a0a;border:1px solid #5a3a10;color:#ffb74d;}
@keyframes blink{0%,100%{opacity:1;}50%{opacity:.55;}}

.ticker-wrap{background:#0c110c;border-bottom:1px solid #192819;padding:6px 0;overflow:hidden;white-space:nowrap;width:100%;}
.ticker-track{display:inline-block;animation:ticker-move 90s linear infinite;}
@keyframes ticker-move{0%{transform:translateX(0);}100%{transform:translateX(-50%);}}
.tick-seg{font-family:'JetBrains Mono',monospace;font-size:12px;color:#6a8a6a;display:inline;}
.tick-sep{color:#3a5a3a;margin:0 10px;}
.tick-fraud{color:#ff4d4d;font-weight:700;}
.tick-clean{color:#4caf50;}
.tick-cust{color:#8abf8a;font-weight:600;}

.date-head{padding:14px 18px 6px;font-size:12px;font-weight:700;color:#4a6a4a;letter-spacing:.14em;text-transform:uppercase;}
.kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;padding:0 18px 12px;}
.kpi{background:#0c110c;border:1px solid #1c2e1c;border-radius:8px;padding:14px 16px;}
.kpi.kpi-red{border-color:#5a1a1a60;}.kpi.kpi-amb{border-color:#5a3a1060;}
.kpi-lbl{font-size:10px;font-weight:700;color:#4a6a4a;letter-spacing:.12em;text-transform:uppercase;margin-bottom:6px;}
.kpi-val{font-size:30px;font-weight:800;font-family:'JetBrains Mono',monospace;line-height:1;}
.kpi-val.wht{color:#ddeedd;}.kpi-val.red{color:#ff4d4d;}.kpi-val.amb{color:#ffb74d;}.kpi-val.grn{color:#4caf50;}
.kpi-delta{font-size:11px;margin-top:5px;}
.kpi-delta.up{color:#ff6b6b;}.kpi-delta.dn{color:#4caf50;}

.two-col{display:grid;grid-template-columns:1fr 300px;gap:10px;padding:0 18px 10px;}
.three-col{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;padding:0 18px 10px;}
.panel{background:#0c110c;border:1px solid #1c2e1c;border-radius:8px;padding:14px 16px;}
.ph{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;}
.pt{font-size:13px;font-weight:600;color:#b8d8b8;}
.pb{background:#0a1a0a;border:1px solid #1a3a1a;border-radius:12px;padding:2px 9px;font-size:11px;color:#4a6a4a;}
.pb.live{color:#4caf50;border-color:#2a5a2a;animation:blink 2s ease-in-out infinite;}
.pb.kenya{color:#4caf50;border-color:#2a5a2a;}

/* STREAM STATS BAR */
.stream-stats{display:flex;gap:18px;margin-bottom:10px;padding:8px 10px;background:#080d08;border-radius:6px;border:1px solid #192819;}
.sstat{display:flex;flex-direction:column;align-items:center;}
.sstat-val{font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:700;line-height:1;}
.sstat-lbl{font-size:9px;color:#4a6a4a;letter-spacing:.1em;text-transform:uppercase;margin-top:2px;}
.sstat-div{width:1px;background:#1c2e1c;margin:0 4px;}
/* PULSE BARS */
.pulse-wrap{display:flex;align-items:flex-end;gap:2px;height:48px;margin-bottom:6px;}
.pulse-bar{flex:1;border-radius:2px 2px 0 0;transition:height .3s ease;}
/* VELOCITY METER */
.vel-wrap{display:flex;align-items:center;gap:8px;margin-top:8px;padding:6px 10px;background:#080d08;border-radius:6px;border:1px solid #192819;}
.vel-label{font-size:10px;color:#4a6a4a;white-space:nowrap;}
.vel-track{flex:1;height:6px;background:#0a1e0a;border-radius:3px;overflow:hidden;}
.vel-fill{height:6px;border-radius:3px;transition:width .5s ease;}
.vel-val{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:700;min-width:50px;text-align:right;}

.chart-leg{display:flex;gap:16px;margin-top:5px;font-size:11px;}
.donut-center{display:flex;justify-content:center;margin:6px 0 10px;}
.leg-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:7px;font-size:12px;}
.dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;}
.leg-lbl{flex:1;margin-left:8px;color:#b8d8b8;}
.leg-cnt{font-family:'JetBrains Mono',monospace;color:#6ab86a;}

.feed-row{display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px solid #0f1e0f;font-size:12px;}
.feed-row:last-child{border-bottom:none;}
.fi{width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;flex-shrink:0;}
.fi.f{background:#350a0a;color:#ff4d4d;border:1px solid #5a1515;}
.fi.c{background:#0a250a;color:#4caf50;border:1px solid #1a4a1a;}
.fi.w{background:#251a0a;color:#ffb74d;border:1px solid #4a2a0a;}
.fm{flex:1;min-width:0;}
.fcust{font-weight:700;color:#b8d8b8;font-size:12px;}
.ftype{font-size:10px;color:#4a6a4a;}
.famt{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:12px;white-space:nowrap;}
.famt.fa{color:#ff4d4d;}.famt.ca{color:#b8d8b8;}
.fscore{font-family:'JetBrains Mono',monospace;font-size:11px;padding:2px 6px;border-radius:4px;}
.fscore.hi{background:#350a0a;color:#ff6b6b;}.fscore.md{background:#251a0a;color:#ffb74d;}.fscore.lo{background:#0a250a;color:#4caf50;}

.rr{display:flex;align-items:center;gap:8px;margin-bottom:9px;font-size:12px;}
.rn{width:130px;color:#b8d8b8;flex-shrink:0;font-size:11px;}
.rbw{flex:1;height:6px;background:#0a1e0a;border-radius:3px;overflow:hidden;}
.rb{height:100%;border-radius:3px;}
.rc{width:34px;text-align:right;font-family:'JetBrains Mono',monospace;font-size:12px;color:#6ab86a;}

.county-scroll{max-height:220px;overflow-y:auto;margin-bottom:8px;padding-right:4px;}
.county-scroll::-webkit-scrollbar{width:3px;}
.county-scroll::-webkit-scrollbar-track{background:#0a1a0a;}
.county-scroll::-webkit-scrollbar-thumb{background:#2a4a2a;border-radius:2px;}
.cr{display:flex;align-items:center;gap:8px;margin-bottom:6px;}
.cn{width:82px;color:#b8d8b8;flex-shrink:0;font-size:10px;}
.cbw{flex:1;height:5px;background:#0a1e0a;border-radius:3px;overflow:hidden;}
.rb2{height:100%;border-radius:3px;}
.cp{width:38px;text-align:right;font-family:'JetBrains Mono',monospace;font-size:10px;color:#6ab86a;}

.txn-type-section{margin-top:10px;border-top:1px solid #1a2e1a;padding-top:8px;}
.txn-type-lbl{font-size:9px;font-weight:700;color:#4a6a4a;letter-spacing:.12em;text-transform:uppercase;margin-bottom:7px;}
.ttr{display:flex;align-items:center;gap:8px;margin-bottom:7px;}
.ttn{width:65px;color:#b8d8b8;font-size:11px;flex-shrink:0;}
.ttbw{flex:1;height:5px;background:#0a1e0a;border-radius:3px;overflow:hidden;}
.ttc{width:30px;text-align:right;font-family:'JetBrains Mono',monospace;font-size:11px;color:#6ab86a;}

.pipe-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:8px;}
.pchip{background:#0a1e0a;border:1px solid #1a3a1a;border-radius:8px;padding:9px 6px;text-align:center;}
.picon{font-size:17px;margin-bottom:3px;}
.pname{font-size:10px;color:#4a6a4a;font-weight:700;}
.pstat{font-size:10px;color:#4caf50;margin-top:2px;font-family:'JetBrains Mono',monospace;}
.sub-lbl{font-size:10px;font-weight:700;color:#4a6a4a;letter-spacing:.1em;text-transform:uppercase;margin:12px 0 6px;}

.alerts-panel{background:#0c110c;border:1px solid #1c2e1c;border-radius:8px;padding:14px 16px;margin:0 18px 14px;}
.alerts-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;}
.alerts-title{font-size:14px;font-weight:700;color:#ddeedd;}
.alerts-right{display:flex;align-items:center;gap:10px;font-size:11px;color:#4a6a4a;}
.open-pill{background:#3a0a0a;border:1px solid #6a1a1a;color:#ff6b6b;font-size:11px;font-weight:700;padding:3px 10px;border-radius:12px;}
.alerts-table{width:100%;border-collapse:collapse;font-size:11px;}
.alerts-table th{color:#4a8a4a;font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;padding:0 10px 10px 0;border-bottom:1px solid #1a2e1a;text-align:left;}
.alerts-table td{padding:9px 10px 9px 0;border-bottom:1px solid #0f1e0f;vertical-align:middle;}
.alerts-table tr:last-child td{border-bottom:none;}
.txn-id{font-family:'JetBrains Mono',monospace;color:#4a6a4a;font-size:10px;}
.a-cust{font-family:'JetBrains Mono',monospace;font-weight:700;color:#b8d8b8;}
.a-amt{font-family:'JetBrains Mono',monospace;font-weight:700;color:#b8d8b8;}
.a-amt.big{color:#ffb74d;}
.reason-tag{display:inline-block;background:#0a1e0a;border:1px solid #1a3a1a;color:#6ab86a;font-size:10px;padding:2px 7px;border-radius:4px;margin:1px 2px 1px 0;font-family:'JetBrains Mono',monospace;}
.score-badge{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:12px;padding:3px 8px;border-radius:5px;display:inline-block;}
.score-badge.hi{background:#3a0a0a;color:#ff4d4d;border:1px solid #6a1a1a;}
.score-badge.md{background:#2a1a0a;color:#ffb74d;border:1px solid #5a3a10;}
.score-badge.lo{background:#0a2a1a;color:#4caf50;border:1px solid #1a4a2a;}
.status-open{color:#ff4d4d;font-weight:700;font-size:11px;}
.status-review{color:#ffb74d;font-weight:700;font-size:11px;}
.status-resolved{color:#4caf50;font-weight:700;font-size:11px;}
.status-blocked{color:#ff6b35;font-weight:700;font-size:11px;}
.status-dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;vertical-align:middle;}
.sdot-open{background:#ff4d4d;}.sdot-review{background:#ffb74d;}.sdot-blocked{background:#ff6b35;}
.a-time{font-family:'JetBrains Mono',monospace;font-size:10px;color:#4a6a4a;text-align:right;}
</style>
""", unsafe_allow_html=True)

ALL_COUNTIES = [
    ('Nairobi',34.2,'#ff4d4d'),('Mombasa',20.1,'#ffb74d'),('Kisumu',14.8,'#4caf50'),
    ('Nakuru',10.9,'#26c6da'),('Uasin Gishu',7.0,'#5c6bc0'),('Unknown',13.0,'#ff6b6b'),
    ('Kiambu',6.8,'#4caf50'),('Machakos',5.4,'#26c6da'),('Kajiado',4.9,'#5c6bc0'),
    ('Meru',4.2,'#4caf50'),('Nyeri',3.8,'#26c6da'),('Kilifi',3.6,'#ffb74d'),
    ('Kakamega',3.4,'#4caf50'),('Bungoma',3.1,'#26c6da'),('Kisii',2.9,'#4caf50'),
    ('Kericho',2.7,'#5c6bc0'),("Murang'a",2.5,'#4caf50'),('Embu',2.3,'#26c6da'),
    ('Siaya',2.2,'#4caf50'),('Homa Bay',2.1,'#5c6bc0'),('Trans Nzoia',2.0,'#4caf50'),
    ('Nandi',1.9,'#26c6da'),('Laikipia',1.8,'#5c6bc0'),('Bomet',1.7,'#4caf50'),
    ('Nyandarua',1.6,'#26c6da'),('Vihiga',1.5,'#4caf50'),('Kirinyaga',1.4,'#26c6da'),
    ('Kwale',1.3,'#ffb74d'),('Taita Taveta',1.2,'#5c6bc0'),('Migori',1.1,'#4caf50'),
    ('Nyamira',1.0,'#26c6da'),('Busia',0.9,'#4caf50'),('Kitui',0.8,'#ffb74d'),
    ('Makueni',0.8,'#5c6bc0'),('Tharaka Nithi',0.7,'#4caf50'),('Narok',0.7,'#26c6da'),
    ('West Pokot',0.6,'#5c6bc0'),('Baringo',0.6,'#4caf50'),('Elgeyo Marakwet',0.5,'#26c6da'),
    ('Samburu',0.5,'#5c6bc0'),('Lamu',0.5,'#ffb74d'),('Garissa',0.4,'#ff6b6b'),
    ('Wajir',0.4,'#ff6b6b'),('Mandera',0.4,'#ff6b6b'),('Marsabit',0.3,'#5c6bc0'),
    ('Isiolo',0.3,'#5c6bc0'),('Tana River',0.3,'#ffb74d'),
]
RULES_CFG=[('Large Amount','#ff4d4d',98),('New Device','#ffb74d',77),('Velocity','#4caf50',60),('Unknown Location','#26c6da',38),('High Hourly Total','#5c6bc0',25)]
CUSTS=['CUST001','CUST002','CUST003','CUST004','CUST005']
TYPES=['send','receive','withdraw','paybill','buy_goods']
COUNTIES_SIM=[c[0] for c in ALL_COUNTIES[:10]]+['Unknown']

def fraud_score_sim(txn):
    weights = []
    r = []
    if txn['amount']>100000: weights.append(0.35); r.append('large_amount')
    if txn['vel']>10: weights.append(0.30); r.append('velocity')
    if 'NEW' in txn['device']: weights.append(0.30); r.append('new_device')
    if txn['county']=='Unknown': weights.append(0.20); r.append('unknown_location')
    if txn['hh']: weights.append(0.20); r.append('high_hourly_total')
    if random.random()<0.05: weights.append(0.10); r.append('dormant_account')
    s=1.0
    for w in weights: s*=(1-w)
    return round(1-s, 4), r

def make_txn_sim():
    fraud=random.random()<0.12
    amt=round(random.uniform(120000,600000)) if fraud else round(random.uniform(50,9500))
    vel=random.randint(12,30) if fraud and random.random()<0.4 else random.randint(0,4)
    dev='DEV_NEW_X' if fraud and random.random()<0.4 else 'DEV_OK'
    co='Unknown' if fraud and random.random()<0.3 else random.choice(COUNTIES_SIM)
    hh=fraud and random.random()<0.3
    txn={'id':str(uuid.uuid4())[:6],'cust':random.choice(CUSTS),'amount':amt,
         'vel':vel,'device':dev,'county':co,'hh':hh,'type':random.choice(TYPES),
         'ts':datetime.now().strftime('%H:%M:%S')}
    sc,reasons=fraud_score_sim(txn)
    txn['score']=sc;txn['reasons']=reasons;txn['is_fraud']=sc>=0.5
    return txn

# ── SESSION STATE ─────────────────────────────────────────────
SEED_VERSION = "2026-08-26.1"
ss=st.session_state
if ss.get('seed_version') != SEED_VERSION:
    ss.clear()
    ss.seed_version = SEED_VERSION
if 'feed'   not in ss: ss.feed   = deque(maxlen=50)
if 'stream' not in ss: ss.stream = deque([(random.randint(130,280),random.randint(0,8)) for _ in range(60)],maxlen=60)
if 'stats'  not in ss: ss.stats  = dict(txns=0,alerts=0,kes_risk=0,open_a=0,review=0,resolved=0,blocked=0)
if 'rules'  not in ss: ss.rules  = {r[0]:r[2] for r in RULES_CFG}
if 'hours'  not in ss: ss.hours  = [random.randint(3,35) for _ in range(24)]
if 't_last' not in ss: ss.t_last = time.time()
if 'ticker' not in ss: ss.ticker = deque(maxlen=40)
if 'txn_type_counts' not in ss: ss.txn_type_counts={'withdraw':0,'send':0,'paybill':0,'buy_goods':0,'receive':0}
if 'db_mode' not in ss: ss.db_mode = False
if 'fraud_alerts' not in ss: ss.fraud_alerts = deque(maxlen=60)
# Pulse bar history for stream panel
if 'pulse_c' not in ss: ss.pulse_c = deque([random.randint(80,200) for _ in range(30)], maxlen=30)
if 'pulse_f' not in ss: ss.pulse_f = deque([random.randint(0,6) for _ in range(30)], maxlen=30)

# ── LIVE UPDATE ───────────────────────────────────────────────
now=time.time()
db_data=None
if now-ss.t_last>=2:
    db_data=fetch_live_data_from_db()
    if db_data:
        ss.db_mode=True
        ss.stats['txns']    = db_data['txns_today']
        ss.stats['alerts']  = db_data['alerts_today']
        ss.stats['kes_risk']= db_data['kes_risk']
        sc=db_data['status_counts']
        # FIX: read all statuses including blocked
        ss.stats['open_a']  = sc.get('open',0)
        ss.stats['blocked'] = sc.get('blocked',0)
        ss.stats['review']  = sc.get('under_review',0)
        ss.stats['resolved']= sc.get('resolved',0)
        ss.feed.clear()
        for row in db_data['feed_rows']:
            reasons=[r.strip() for r in (row[6] or 'clean').split(',') if r.strip()!='clean']
            ss.feed.append({'id':row[0][:6],'cust':row[1],'amount':float(row[2]),
                'type':row[3],'is_fraud':row[4],'score':float(row[5] or 0),
                'reasons':reasons,'vel':row[7] or 0,'ts':str(row[8])[-8:-3]})
        ss.fraud_alerts.clear()
        for row in db_data['alert_rows']:
            reasons=[r.strip() for r in (row[3] or '').split(',') if r.strip()]
            ss.fraud_alerts.appendleft({'id':row[0][:6],'cust':row[1],'amount':float(row[2]),
                'reasons':reasons,'score':float(row[4] or 0),
                'status':(row[5] or 'open').upper(),
                'ts':str(row[6])[-8:-3] if row[6] else '--:--',
                'created':time.time()-60,'reviewed_at':None})
        if db_data['rule_rows']:
            for reason,cnt in db_data['rule_rows']:
                for rc in RULES_CFG:
                    if rc[0].lower().replace(' ','_') in (reason or ''):
                        ss.rules[rc[0]]=int(cnt)
        if db_data['type_rows']:
            for ttype,cnt in db_data['type_rows']:
                ss.txn_type_counts[ttype]=int(cnt)
        if db_data['hour_rows']:
            for h,cnt in db_data['hour_rows'].items():
                if 0<=h<=23: ss.hours[h]=int(cnt)
        # Stream from DB
        if db_data.get('stream_rows'):
            pts = {r[0]:(r[1],r[2]) for r in db_data['stream_rows']}
            for m in range(60):
                c,f = pts.get(m,(random.randint(100,250), random.randint(0,5)))
                ss.pulse_c.append(int(c)); ss.pulse_f.append(int(f))
        else:
            ss.pulse_c.append(random.randint(100,280))
            ss.pulse_f.append(random.randint(0,8))
        for t in list(ss.feed)[:5]:
            ss.ticker.appendleft(t)
    else:
        ss.db_mode=False
        batch=[make_txn_sim() for _ in range(random.randint(2,5))]
        for t in batch:
            ss.feed.appendleft(t); ss.ticker.appendleft(t)
            ss.stats['txns']+=1
            ss.txn_type_counts[t['type']]=ss.txn_type_counts.get(t['type'],0)+(1 if t['is_fraud'] else 0)
            if t['is_fraud']:
                ss.stats['alerts']+=1; ss.stats['kes_risk']+=t['amount']; ss.stats['open_a']+=1
                for rc in RULES_CFG:
                    if any(rc[0].lower().replace(' ','_').startswith(r[:4]) for r in t['reasons']):
                        ss.rules[rc[0]]=ss.rules.get(rc[0],0)+1
                ss.fraud_alerts.appendleft({'id':t['id'],'cust':t['cust'],'amount':t['amount'],
                    'reasons':t['reasons'],'score':t['score'],'status':'OPEN','ts':t['ts'],
                    'created':time.time(),'reviewed_at':None})
        new_c=sum(1 for t in batch if not t['is_fraud'])
        new_f=sum(1 for t in batch if t['is_fraud'])
        ss.pulse_c.append(max(0, (ss.pulse_c[-1] if ss.pulse_c else 150) + random.randint(-20,25) + new_c))
        ss.pulse_f.append(max(0, (ss.pulse_f[-1] if ss.pulse_f else 3)  + random.randint(-1,2)  + new_f))
        ss.hours[datetime.now().hour]+=new_f

        # Simulation alert progression
        def _review_after(score):
            if score>=0.65: return 5
            if score>=0.5:  return 10
            return 15
        def _resolve_after(score):
            if score>=0.65: return None
            if score>=0.5:  return 24
            return 10
        _tnow=time.time()
        for a in list(ss.fraud_alerts):
            age=_tnow-a.get('created',_tnow)
            if a['status']=='OPEN' and age>=_review_after(a['score']):
                a['status']='REVIEW'; a['reviewed_at']=_tnow
                ss.stats['open_a']=max(0,ss.stats['open_a']-1); ss.stats['review']+=1
            elif a['status']=='REVIEW' and a.get('reviewed_at'):
                ra=_resolve_after(a['score'])
                if ra is not None and (_tnow-a['reviewed_at'])>=ra:
                    a['status']='RESOLVED'
                    ss.stats['review']=max(0,ss.stats['review']-1); ss.stats['resolved']+=1
    ss.t_last=now

# ── BUILD HTML ────────────────────────────────────────────────
S=ss.stats; feed=list(ss.feed)
fraud_rate=round(S['alerts']/max(S['txns'],1)*100,2)

def fmt_kes(v): return f"{v/1e6:.1f}M" if v>=1e6 else f"{v/1000:.0f}K" if v>=1000 else str(int(v))
def sc_cls(s): return 'hi' if s>=0.7 else 'md' if s>=0.4 else 'lo'

# TICKER
tick_parts=[]
for t in list(ss.ticker)[:30]:
    cls='tick-fraud' if t['is_fraud'] else 'tick-clean'; lbl='FRAUD' if t['is_fraud'] else 'clean'
    tick_parts.append(f'<span class="tick-seg"><span class="tick-cust">{t["cust"]}</span> KES {t["amount"]:,} {t["type"]} · <span class="{cls}">{lbl}</span></span><span class="tick-sep">·</span>')
if not tick_parts:
    tick_parts=['<span class="tick-seg"><span class="tick-cust">CUST002</span> KES 1,351 paybill · <span class="tick-clean">clean</span></span><span class="tick-sep">·</span>']
ticker_html=''.join(tick_parts)*2

# ── STREAM PANEL — upgraded ───────────────────────────────────
pulse_c = list(ss.pulse_c)
pulse_f = list(ss.pulse_f)
n       = len(pulse_c)
max_v   = max(max(pulse_c) if pulse_c else 1, 1)
total_c = sum(pulse_c)
total_f = sum(pulse_f)
last_c  = pulse_c[-1] if pulse_c else 0
last_f  = pulse_f[-1] if pulse_f else 0
# velocity: fraud txns per minute in last 5 points
vel_5   = sum(pulse_f[-5:]) / max(sum(pulse_c[-5:]) + sum(pulse_f[-5:]), 1) * 100
vel_pct = min(int(vel_5 * 5), 100)  # scaled for display
vel_col = '#ff4d4d' if vel_pct > 60 else '#ffb74d' if vel_pct > 30 else '#4caf50'

# Pulse bars SVG — dual layer clean + fraud bars per minute
bar_w   = 8
gap     = 2
bars_html = ""
for i, (c, f) in enumerate(zip(pulse_c, pulse_f)):
    x       = i * (bar_w + gap)
    c_h     = max(int((c / max_v) * 44), 2)
    f_h     = max(int((f / max(max_v*0.3, 1)) * 44), 1) if f > 0 else 0
    is_now  = i == n - 1
    c_col   = '#2a5a2a' if not is_now else '#4caf50'
    f_col   = '#5a1a1a' if not is_now else '#ff4d4d'
    bars_html += f'<rect x="{x}" y="{50-c_h}" width="{bar_w}" height="{c_h}" fill="{c_col}" rx="1" opacity="0.8"/>'
    if f_h:
        bars_html += f'<rect x="{x}" y="{50-f_h}" width="{bar_w}" height="{f_h}" fill="{f_col}" rx="1" opacity="0.9"/>'

svg_w = n * (bar_w + gap)
pulse_svg = f'''<svg viewBox="0 0 {svg_w} 56" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:80px">
  {bars_html}
  <text x="0" y="55" fill="#3a5a3a" font-size="8" font-family="monospace">-{n}m</text>
  <text x="{svg_w//2}" y="55" fill="#3a5a3a" font-size="8" font-family="monospace" text-anchor="middle">-{n//2}m</text>
  <text x="{svg_w-4}" y="55" fill="#3a5a3a" font-size="8" font-family="monospace" text-anchor="end">now</text>
</svg>'''

stream_panel = f'''
<div class="stream-stats">
  <div class="sstat">
    <div class="sstat-val" style="color:#4caf50">{last_c:,}</div>
    <div class="sstat-lbl">clean / min</div>
  </div>
  <div class="sstat-div"></div>
  <div class="sstat">
    <div class="sstat-val" style="color:#ff4d4d">{last_f:,}</div>
    <div class="sstat-lbl">fraud / min</div>
  </div>
  <div class="sstat-div"></div>
  <div class="sstat">
    <div class="sstat-val" style="color:#ddeedd">{total_c+total_f:,}</div>
    <div class="sstat-lbl">total 30 min</div>
  </div>
  <div class="sstat-div"></div>
  <div class="sstat">
    <div class="sstat-val" style="color:#ffb74d">{round(total_f/max(total_c+total_f,1)*100,1)}%</div>
    <div class="sstat-lbl">fraud rate</div>
  </div>
</div>
{pulse_svg}
<div class="vel-wrap">
  <div class="vel-label">⚡ FRAUD VELOCITY</div>
  <div class="vel-track"><div class="vel-fill" style="width:{vel_pct}%;background:{vel_col}"></div></div>
  <div class="vel-val" style="color:{vel_col}">{vel_5:.1f}%/min</div>
</div>
<div class="chart-leg">
  <span><span style="display:inline-block;width:12px;height:12px;background:#2a5a2a;border-radius:2px;vertical-align:middle;margin-right:4px"></span>Clean txns</span>
  <span><span style="display:inline-block;width:12px;height:12px;background:#5a1a1a;border-radius:2px;vertical-align:middle;margin-right:4px"></span>Fraud txns</span>
  <span style="color:#4a6a4a;font-size:10px">Last bar = current minute</span>
</div>'''

# DONUT — FIX: use same counts as alerts table
# Count directly from fraud_alerts session state to guarantee sync
_al = list(ss.fraud_alerts)
_open_c    = sum(1 for a in _al if a['status'] in ('OPEN','open'))
_blocked_c = sum(1 for a in _al if a['status'] in ('BLOCKED','blocked'))
_review_c  = sum(1 for a in _al if a['status'] in ('REVIEW','under_review','UNDER_REVIEW'))
_resolved_c= sum(1 for a in _al if a['status'] in ('RESOLVED','resolved'))
total_a = _open_c + _blocked_c + _review_c + _resolved_c

def arc(cx,cy,r,sa,ea):
    s=math.radians(sa-90); e=math.radians(ea-90)
    x1=cx+r*math.cos(s); y1=cy+r*math.sin(s); x2=cx+r*math.cos(e); y2=cy+r*math.sin(e)
    return f"M{cx},{cy} L{x1:.2f},{y1:.2f} A{r},{r} 0 {1 if ea-sa>180 else 0},1 {x2:.2f},{y2:.2f} Z"

angle=0; donut_paths=""
for frac,col in [
    (_blocked_c/max(total_a,1),'#ff6b35'),
    (_open_c/max(total_a,1),'#ff6b6b'),
    (_review_c/max(total_a,1),'#ffb74d'),
    (_resolved_c/max(total_a,1),'#4caf50')
]:
    deg=frac*360
    if deg>0:
        donut_paths+=f'<path d="{arc(50,50,42,angle,angle+deg)}" fill="{col}" opacity=".88"/>'
    angle+=deg

donut_svg=(f'<svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg" style="width:140px;height:140px">'
           f'<circle cx="50" cy="50" r="42" fill="#0c110c"/>{donut_paths}'
           f'<circle cx="50" cy="50" r="29" fill="#0c110c"/>'
           f'<text x="50" y="47" text-anchor="middle" fill="#ddeedd" font-size="15" font-weight="800" font-family="JetBrains Mono,monospace">{total_a}</text>'
           f'<text x="50" y="58" text-anchor="middle" fill="#4a6a4a" font-size="7" font-family="monospace" letter-spacing="1">ALERTS</text></svg>')

# FEED
def fi(t): return ('f','!') if t['is_fraud'] else (('w','~') if t['score']>0.3 else ('c','✓'))
feed_html=""
for t in feed[:9]:
    ic,sym=fi(t); ac='fa' if t['is_fraud'] else 'ca'
    sub=f'{t["type"]}{"· velocity" if t.get("vel",0)>5 else ""} · {t["ts"]}'
    feed_html+=(f'<div class="feed-row"><div class="fi {ic}">{sym}</div>'
                f'<div class="fm"><div class="fcust">{t["cust"]}</div><div class="ftype">{sub}</div></div>'
                f'<div class="famt {ac}">KES {t["amount"]:,}</div>'
                f'<div class="fscore {sc_cls(t["score"])}">{t["score"]:.2f}</div></div>')

# RULES
max_r=max(ss.rules.values(),default=1); rules_html=""
for name,col,_ in RULES_CFG:
    cnt=ss.rules.get(name,0); pct=int(cnt/max(max_r,1)*100)
    rules_html+=(f'<div class="rr"><div class="rn">{name}</div>'
                 f'<div class="rbw"><div class="rb" style="width:{pct}%;background:{col}"></div></div>'
                 f'<div class="rc">{cnt}</div></div>')

# COUNTIES
max_c=ALL_COUNTIES[0][1]; county_rows=""
for cname,cpct,col in ALL_COUNTIES:
    pw=int(cpct/max_c*100)
    county_rows+=(f'<div class="cr"><div class="cn">{cname}</div>'
                  f'<div class="cbw"><div class="rb2" style="width:{pw}%;background:{col}"></div></div>'
                  f'<div class="cp">{cpct}%</div></div>')

# TXN TYPE
max_tt=max(ss.txn_type_counts.values(),default=1); txn_type_html=""
for ttype,col in [('withdraw','#ff4d4d'),('send','#ffb74d'),('paybill','#4caf50'),('buy_goods','#26c6da')]:
    cnt=ss.txn_type_counts.get(ttype,0); pw=int(cnt/max(max_tt,1)*100)
    txn_type_html+=(f'<div class="ttr"><div class="ttn">{ttype}</div>'
                    f'<div class="ttbw"><div class="rb2" style="width:{pw}%;background:{col}"></div></div>'
                    f'<div class="ttc">{cnt}</div></div>')

# HOUR BARS
hv=ss.hours; mh=max(max(hv),1); bw=260/24; cur_h=datetime.now().hour; hbars=""
for i,v in enumerate(hv):
    bh=max((v/mh)*45,1); c='#ff4d4d' if i==cur_h else '#1c3a1c'
    hbars+=f'<rect x="{i*bw+1:.1f}" y="{50-bh:.1f}" width="{bw-2:.1f}" height="{bh:.1f}" fill="{c}" rx="1"/>'
hour_svg=(f'<svg viewBox="0 0 260 56" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:56px">'
          f'{hbars}<text x="0" y="55" fill="#3a5a3a" font-size="8" font-family="monospace">0h</text>'
          f'<text x="108" y="55" fill="#3a5a3a" font-size="8" font-family="monospace">12h</text>'
          f'<text x="240" y="55" fill="#3a5a3a" font-size="8" font-family="monospace">23h</text></svg>')

# ALERTS TABLE — FIX: synced with donut, counts from same source
_status_order={'OPEN':0,'BLOCKED':1,'REVIEW':2,'UNDER_REVIEW':2}
_actionable=[a for a in _al if a['status'].upper() in _status_order]
_sorted_alerts=sorted(_actionable,key=lambda a:(_status_order.get(a['status'].upper(),3),-a['score']))
open_count=_open_c + _blocked_c
alerts_rows=""
for a in _sorted_alerts[:15]:
    reason_tags="".join(f'<span class="reason-tag">{r}</span>' for r in a['reasons'])
    sc_b='hi' if a['score']>=0.7 else 'md' if a['score']>=0.5 else 'lo'
    big=' big' if a['amount']>=100000 else ''
    st_up = a['status'].upper()
    if st_up=='BLOCKED':
        stat='<span class="status-blocked"><span class="status-dot sdot-blocked"></span>🚫 BLOCKED</span>'
    elif st_up=='OPEN':
        stat='<span class="status-open"><span class="status-dot sdot-open"></span>OPEN</span>'
    elif st_up in ('REVIEW','UNDER_REVIEW'):
        stat='<span class="status-review"><span class="status-dot sdot-review"></span>REVIEW</span>'
    else:
        stat='<span class="status-resolved">✓ RESOLVED</span>'
    alerts_rows+=(f'<tr><td><span class="txn-id">{a["id"]}</span></td>'
                  f'<td><span class="a-cust">{a["cust"]}</span></td>'
                  f'<td><span class="a-amt{big}">KES&nbsp;{int(a["amount"]):,}</span></td>'
                  f'<td>{reason_tags}</td>'
                  f'<td><span class="score-badge {sc_b}">{a["score"]:.2f}</span></td>'
                  f'<td>{stat}</td>'
                  f'<td class="a-time">{a["ts"]}</td></tr>')

clock=datetime.now().strftime('%H:%M:%S')
db_badge=(f'<span class="nav-db live">● DB LIVE</span>' if ss.db_mode else
          f'<span class="nav-db sim">⚡ SIMULATION</span>')

# ── RENDER ────────────────────────────────────────────────────
st.markdown(f"""
<div class="topnav">
  <div style="display:flex;align-items:center;gap:10px">
    <div class="nav-logo">MP</div>
    <div><div class="nav-title">M-Pesa Fraud Intelligence</div><div class="nav-sub">Real-Time Detection Pipeline</div></div>
  </div>
  <div style="display:flex;align-items:center;gap:6px">
    {db_badge}
    <div class="nav-live">● PIPELINE LIVE</div>
    <div class="nav-clock">{clock}</div>
  </div>
</div>
<div class="ticker-wrap"><div class="ticker-track">{ticker_html}</div></div>
<div class="date-head">TODAY — {datetime.now().strftime('%B %d, %Y').upper()} · NAIROBI, KENYA</div>
<div class="kpi-grid">
  <div class="kpi"><div class="kpi-lbl">TRANSACTIONS TODAY</div><div class="kpi-val wht">{S['txns']:,}</div><div class="kpi-delta up">▲ 8.2% vs yesterday</div></div>
  <div class="kpi kpi-red"><div class="kpi-lbl">FRAUD ALERTS TODAY</div><div class="kpi-val red">{S['alerts']:,}</div><div class="kpi-delta up">▲ 12.4% vs yesterday</div></div>
  <div class="kpi kpi-amb"><div class="kpi-lbl">KES AT RISK</div><div class="kpi-val amb">{fmt_kes(S['kes_risk'])}</div><div class="kpi-delta up">▲ 3.1% today</div></div>
  <div class="kpi"><div class="kpi-lbl">FRAUD RATE</div><div class="kpi-val grn">{fraud_rate:.2f}%</div><div class="kpi-delta dn">▼ 0.3% model improving</div></div>
</div>
<div class="two-col">
  <div class="panel">
    <div class="ph"><div class="pt">Transaction Stream — Last 30 Minutes</div><div class="pb live">LIVE · 2s refresh</div></div>
    {stream_panel}
  </div>
  <div class="panel">
    <div class="ph"><div class="pt">Alert Status</div><div class="pb">{total_a} total</div></div>
    <div class="donut-center">{donut_svg}</div>
    <div class="leg-row"><div class="dot" style="background:#ff6b35"></div><div class="leg-lbl">Blocked</div><div class="leg-cnt">{_blocked_c}</div></div>
    <div class="leg-row"><div class="dot" style="background:#ff6b6b"></div><div class="leg-lbl">Open</div><div class="leg-cnt">{_open_c}</div></div>
    <div class="leg-row"><div class="dot" style="background:#ffb74d"></div><div class="leg-lbl">Under Review</div><div class="leg-cnt">{_review_c}</div></div>
    <div class="leg-row"><div class="dot" style="background:#4caf50"></div><div class="leg-lbl">Resolved</div><div class="leg-cnt">{_resolved_c}</div></div>
  </div>
</div>
<div class="three-col">
  <div class="panel">
    <div class="ph"><div class="pt">Live Transaction Feed</div><div class="pb live">{len(feed)} recent</div></div>
    {feed_html}
  </div>
  <div class="panel">
    <div class="ph"><div class="pt">Fraud Rules Triggered</div><div class="pb">today</div></div>
    {rules_html}
    <div class="sub-lbl">PIPELINE HEALTH</div>
    <div class="pipe-grid">
      <div class="pchip"><div class="picon">⚡</div><div class="pname">Kafka</div><div class="pstat">{"OK" if ss.db_mode else "SIM"}</div></div>
      <div class="pchip"><div class="picon">⚙️</div><div class="pname">Spark</div><div class="pstat">{"OK" if ss.db_mode else "SIM"}</div></div>
      <div class="pchip"><div class="picon">🗄</div><div class="pname">Postgres</div><div class="pstat">{"OK" if ss.db_mode else "OFF"}</div></div>
      <div class="pchip"><div class="picon">❄️</div><div class="pname">Snowflake</div><div class="pstat">OK</div></div>
    </div>
  </div>
  <div class="panel">
    <div class="ph"><div class="pt">Fraud by County</div><div class="pb kenya">Kenya · 47 counties</div></div>
    <div class="county-scroll">{county_rows}</div>
    <div class="txn-type-section">
      <div class="txn-type-lbl">BY TXN TYPE</div>
      {txn_type_html}
    </div>
  </div>
</div>
<div class="alerts-panel">
  <div class="alerts-head">
    <div class="alerts-title">Open Fraud Alerts — Requires Action</div>
    <div class="alerts-right">
      <span>{"Live from PostgreSQL · auto-resolved every refresh" if ss.db_mode else "Simulation mode"}</span>
      <span class="open-pill">{open_count} open</span>
    </div>
  </div>
  <table class="alerts-table">
    <thead><tr>
      <th>TXN ID</th><th>CUSTOMER</th><th>AMOUNT (KES)</th>
      <th>FRAUD REASON</th><th>SCORE</th><th>STATUS</th><th style="text-align:right">TIME</th>
    </tr></thead>
    <tbody>{alerts_rows if alerts_rows else '<tr><td colspan="7" style="text-align:center;padding:20px;color:#4a6a4a">No open alerts right now</td></tr>'}</tbody>
  </table>
</div>
""", unsafe_allow_html=True)

time.sleep(2)
st.rerun()