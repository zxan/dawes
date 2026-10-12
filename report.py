import sqlite3
from datetime import datetime

DB_FILE = "onions.db"

def report():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("SELECT COUNT(DISTINCT url) FROM pings")
    total = c.fetchone()[0]

    c.execute("SELECT COUNT(DISTINCT timestamp) FROM pings")
    cycles = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM urls WHERE alive_pings > 0")
    ever_alive = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM urls WHERE alive_pings = 0")
    always_dead = c.fetchone()[0]

    c.execute("""SELECT COUNT(*) FROM urls 
                 WHERE alive_pings > 0 
                 AND alive_pings < total_pings""")
    churned = c.fetchone()[0]

    c.execute("""SELECT COUNT(*) FROM urls 
                 WHERE alive_pings = total_pings 
                 AND total_pings > 0""")
    always_alive = c.fetchone()[0]

    print(f"""
=== ONION SERVICE CHURN REPORT ===
Generated: {datetime.utcnow().isoformat()}
Ping cycles completed: {cycles}

Total URLs discovered:     {total}
Ever seen alive:           {ever_alive} ({ever_alive/total*100:.1f}%)
Never responded:           {always_dead} ({always_dead/total*100:.1f}%)
Consistently alive:        {always_alive} ({always_alive/total*100:.1f}%)
Churned (alive then dead): {churned} ({churned/total*100:.1f}%)
""")

    # per-cycle breakdown
    print("Per-cycle breakdown:")
    c.execute("""SELECT timestamp, 
                 COUNT(*) as total,
                 SUM(CASE WHEN status='alive' THEN 1 ELSE 0 END) as alive
                 FROM pings GROUP BY timestamp ORDER BY timestamp""")
    for row in c.fetchall():
        ts, t, a = row
        print(f"  {ts}  —  {a}/{t} alive ({a/t*100:.1f}%)")

if __name__ == "__main__":
    report()