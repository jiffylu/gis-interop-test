#!/usr/bin/env python3
"""
Fetch the complete 2025 SF 311 case set from DataSF (SODA dataset vw6y-z8j6).

The shipped 09_csv/sf_311_2025.csv is a 300,000-row export cap: it ends on
2025-05-15 not because anything happened then but because the export did.
DataSF holds 872,426 rows for 2025. This writes them to
09_csv/sf_311_2025_full.csv with the identical 25-column header, leaving the
original untouched. run_analysis.py prefers the _full file when it exists.

Pages by calendar week rather than $offset: deep offsets over a large filtered
set are slow and unstable on Socrata, and a week is ~17k rows. Any week that
fills the request limit is split in half and refetched. Output goes to a .part
file and is renamed only after every window has landed, so a partial download
can never be mistaken for the real thing.
"""
import csv, io, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta

BASE  = "https://data.sfgov.org/resource/vw6y-z8j6.csv"
YEAR  = 2025
LIMIT = 100000
HERE  = os.path.dirname(os.path.abspath(__file__))
DST   = os.path.join(HERE, "09_csv", "sf_311_2025_full.csv")
UA    = {"User-Agent": "gis-interop-test/1.0 (full-year 311 refresh)"}

def log(*a): print("[%s]" % datetime.now().strftime("%H:%M:%S"), *a, flush=True)

def soda(where, tries=4):
    url = BASE + "?" + urllib.parse.urlencode({"$where": where, "$limit": str(LIMIT)},
                                              quote_via=urllib.parse.quote)
    for i in range(tries):
        try:
            return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=300).read().decode()
        except Exception as e:
            wait = (5, 15, 45, 90)[i]
            log("  retry %d after %s: %s" % (i + 1, type(e).__name__, str(e)[:80])); time.sleep(wait)
    raise RuntimeError("gave up on window: " + where)

def fetch_window(t0, t1):
    """CSV text (with header) for t0 <= requested_datetime < t1, splitting if it hits LIMIT."""
    w = "requested_datetime >= '%s' and requested_datetime < '%s'" % (t0.isoformat(), t1.isoformat())
    body = soda(w)
    n = body.count("\n") - 1
    if n >= LIMIT - 1:                       # window too dense -> halve it
        mid = t0 + (t1 - t0) / 2
        log("  window %s..%s hit the limit, splitting" % (t0.date(), t1.date()))
        a, b = fetch_window(t0, mid), fetch_window(mid, t1)
        return a + b.split("\n", 1)[1]
    return body

def main():
    part = DST + ".part"
    start, end = datetime(YEAR, 1, 1), datetime(YEAR + 1, 1, 1)
    header, total, t = None, 0, start
    with open(part, "w", newline="") as out:
        while t < end:
            t1 = min(t + timedelta(days=7), end)
            body = fetch_window(t, t1)
            hdr, rest = body.split("\n", 1) if "\n" in body else (body, "")
            if header is None:
                header = hdr; out.write(hdr + "\n")
            elif hdr != header:
                raise RuntimeError("header changed mid-download at %s" % t.date())
            if rest and not rest.endswith("\n"): rest += "\n"
            out.write(rest)
            n = sum(1 for _ in csv.reader(io.StringIO(rest)))
            total += n
            log("%s .. %s  %6d rows  (running total %d)" % (t.date(), t1.date(), n, total))
            t = t1; time.sleep(1.0)
    os.replace(part, DST)
    log("done: %d rows -> %s (%.0f MB)" % (total, DST, os.path.getsize(DST) / 1e6))

if __name__ == "__main__":
    main()
