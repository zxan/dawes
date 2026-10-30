import argparse
from datetime import datetime, timezone
from pathlib import Path

from database import connect_db, ensure_schema


DB_FILE = "onions.db"


def percentage(part, whole):
    return (part / whole * 100) if whole else 0.0


def report(db_path=DB_FILE):
    connection = connect_db(db_path)
    ensure_schema(connection)

    total = connection.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
    completed = connection.execute(
        "SELECT COUNT(*) FROM cycles WHERE status = 'completed'"
    ).fetchone()[0]
    aborted = connection.execute(
        "SELECT COUNT(*) FROM cycles WHERE status = 'aborted'"
    ).fetchone()[0]
    ever_alive = connection.execute(
        "SELECT COUNT(*) FROM urls WHERE alive_pings > 0"
    ).fetchone()[0]
    never_alive = connection.execute(
        "SELECT COUNT(*) FROM urls WHERE alive_pings = 0"
    ).fetchone()[0]
    always_alive = connection.execute(
        """
        SELECT COUNT(*) FROM urls
        WHERE total_pings > 0 AND alive_pings = total_pings
        """
    ).fetchone()[0]
    churned = connection.execute(
        """
        SELECT COUNT(DISTINCT alive.url)
        FROM pings alive
        JOIN cycles ac ON ac.cycle_id = alive.cycle_id AND ac.status = 'completed'
        WHERE alive.status = 'alive'
          AND EXISTS (
              SELECT 1 FROM pings dead
              JOIN cycles dc ON dc.cycle_id = dead.cycle_id AND dc.status = 'completed'
              WHERE dead.url = alive.url AND dead.status = 'dead'
                AND dc.started_at > ac.started_at
          )
        """
    ).fetchone()[0]
    # MAX - MIN across alive observations only proves the service was up across
    # that span, so it is a lower bound, never a lifespan. A URL seen alive in a
    # single cycle contributes 0 hours: it was up at one instant, not for zero
    # time. Reported as a bound, with the two groups that make it a bound.
    span = connection.execute(
        """
        SELECT COUNT(*) AS observed,
               AVG(hours_alive) AS mean_hours,
               MAX(hours_alive) AS max_hours,
               SUM(single_observation) AS single
        FROM (
            SELECT (julianday(MAX(p.timestamp)) - julianday(MIN(p.timestamp))) * 24 AS hours_alive,
                   COUNT(*) = 1 AS single_observation
            FROM pings p JOIN cycles c ON c.cycle_id = p.cycle_id
            WHERE c.status = 'completed' AND p.status = 'alive'
            GROUP BY p.url
        )
        """
    ).fetchone()
    # Right-censored: alive when we last looked, so the true lifespan runs past
    # the end of the observation window by an unknown amount.
    censored = connection.execute(
        """
        SELECT COUNT(*) FROM pings
        WHERE status = 'alive'
          AND cycle_id = (
              SELECT cycle_id FROM cycles
              WHERE status = 'completed'
              ORDER BY datetime(started_at) DESC, cycle_id DESC
              LIMIT 1
          )
        """
    ).fetchone()[0]

    print("\n=== ONION SERVICE CHURN REPORT ===")
    print(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    print(f"Completed cycles:          {completed}")
    print(f"Aborted cycles/gaps:       {aborted}")
    print()
    print(f"Total URLs discovered:     {total}")
    print(f"Ever seen alive:           {ever_alive} ({percentage(ever_alive, total):.1f}%)")
    print(f"Never responded:           {never_alive} ({percentage(never_alive, total):.1f}%)")
    print(f"Consistently alive:        {always_alive} ({percentage(always_alive, total):.1f}%)")
    print(f"Churned (alive then dead): {churned} ({percentage(churned, total):.1f}%)")
    print(
        f"Mean observed alive span:  >={(span['mean_hours'] or 0):.1f} hours "
        f"(LOWER BOUND, n={span['observed']})"
    )
    print(f"Longest observed span:     >={(span['max_hours'] or 0):.1f} hours (LOWER BOUND)")
    print(f"Single-observation URLs:   {span['single'] or 0} (alive in one cycle; span >=0h)")
    print(f"Right-censored URLs:       {censored} (still alive in latest completed cycle)")
    print("  Spans are lower bounds: observation starts after a service exists and")
    print("  ends while it may still be running. No survival model is applied.")

    print("\nPer-cycle breakdown:")
    cycles = connection.execute(
        """
        SELECT cycle_id, started_at, total, alive, status
        FROM cycles ORDER BY started_at, cycle_id
        """
    ).fetchall()
    if not cycles:
        print("  No cycles recorded.")
    for cycle in cycles:
        if cycle["status"] == "aborted":
            print(f"  {cycle['started_at']} — GAP (cycle {cycle['cycle_id']} aborted)")
        else:
            print(
                f"  {cycle['started_at']} — {cycle['alive']}/{cycle['total']} alive "
                f"({percentage(cycle['alive'], cycle['total']):.1f}%)"
            )
    connection.close()


def main():
    parser = argparse.ArgumentParser(description="Print onion churn statistics")
    parser.add_argument("--db", type=Path, default=Path(DB_FILE))
    args = parser.parse_args()
    if not args.db.exists():
        raise SystemExit(f"Database not found: {args.db}")
    report(args.db)


if __name__ == "__main__":
    main()
