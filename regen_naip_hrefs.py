#!/usr/bin/env python3
"""
Rewrite 08_geotiff/naip_hrefs.json so it covers the AOI the analysis actually
lands in, instead of the Ano Nuevo quarter-quads it shipped with.

The shipped hrefs are all at lat 37.060-37.127 -- the far south of San Mateo
County -- while the only place the TIGER 06081 roads and the SF 311 cases
overlap is the Daly City county line at lat ~37.70. run_analysis.py works around
that by deriving the covering quad from the same NAIP container; regenerating
this file removes the need for that fallback.

The AOI grows with SNAP_TOL_M (50 m -> one quarter-quad, 100 m -> two), so by
default this covers every tolerance up to MAX_TOL, making the file valid for
both runs rather than only the last one.

  python3 regen_naip_hrefs.py            # cover tolerances up to 150 m
  python3 regen_naip_hrefs.py 100        # cover tolerances up to 100 m
  python3 regen_naip_hrefs.py --dry-run  # show what it would write

The original file is preserved as naip_hrefs.original.json on first run.
"""
import json, os, shutil, sys

import duckdb

import run_analysis as ra   # reuse its constants, quad naming and COG opener

MAX_TOL   = float(os.environ.get("MAX_TOL", 150))  # superset of the 50/100 m AOIs
PAD_M     = 300.0           # matches naip_overlay's window pad, plus slack
TARGET    = ra.p("08_geotiff/naip_hrefs.json")
BACKUP    = ra.p("08_geotiff/naip_hrefs.original.json")


def aoi_bbox_4326(max_tol):
    """lon/lat bbox of the 200 m buffer around every segment that clears the
    case threshold at a snap tolerance of `max_tol`."""
    c = duckdb.connect()
    c.execute("INSTALL spatial; LOAD spatial;")
    c.execute("""
      create table roads as
      select LINEARID, ST_Transform(geom,'EPSG:4269',?,true) g from ST_Read(?)
    """, [ra.METRIC_CRS, ra.p(ra.PROFILE["roads"])])
    c.execute("""
      create table cases_m as
      select ST_Transform(ST_Point(try_cast(long as double),
                                   try_cast(lat  as double)),'EPSG:4326',?,true) g,
             service_request_id sid
      from read_csv(?, header=true, all_varchar=true, ignore_errors=true)
      where try_cast(lat  as double) between  -90 and  90
        and try_cast(long as double) between -180 and 180
        and try_cast(substr(requested_datetime,1,4) as int) = ?
    """, [ra.METRIC_CRS, ra.p(ra.CASES_CSV), ra.YEAR])
    c.execute("""
      create table hot as
      select r.LINEARID, r.g from (
        select LINEARID, count(*) n from (
          select p.sid, r.LINEARID,
                 row_number() over (partition by p.sid
                                    order by ST_Distance(p.g,r.g), r.LINEARID) rn
          from cases_m p join roads r on ST_DWithin(p.g, r.g, ?)
        ) where rn = 1 group by 1
      ) a join roads r using (LINEARID) where a.n > ?
    """, [max_tol, ra.CASE_THRESHOLD])
    n_hot = c.execute("select count(*) from hot").fetchone()[0]
    if not n_hot:
        raise SystemExit("no segment clears the %d-case threshold at %g m"
                         % (ra.CASE_THRESHOLD, max_tol))
    zb = c.execute("""select ST_XMin(g)-?, ST_YMin(g)-?, ST_XMax(g)+?, ST_YMax(g)+?
                      from (select ST_Union_Agg(ST_Buffer(g,?)) g from hot)""",
                   [PAD_M, PAD_M, PAD_M, PAD_M, ra.BUFFER_M]).fetchone()
    ll = c.execute("""select ST_XMin(b),ST_YMin(b),ST_XMax(b),ST_YMax(b) from
        (select ST_Extent(ST_Transform(ST_GeomFromText(?),?,'EPSG:4326',true)) b)""",
        ["POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" % (
            zb[0],zb[1], zb[2],zb[1], zb[2],zb[3], zb[0],zb[3], zb[0],zb[1]),
         ra.METRIC_CRS]).fetchone()
    return n_hot, ll


def main():
    args    = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry_run = "--dry-run" in sys.argv
    max_tol = float(args[0]) if args else MAX_TOL

    n_hot, ll = aoi_bbox_4326(max_tol)
    ra.log("county %s (%s), threshold >%d cases" % (ra.PROFILE["fips"], ra.PROFILE["name"], ra.CASE_THRESHOLD))
    ra.log("AOI at snap tolerance <= %g m: %d hot segments, "
           "lon %.4f..%.4f lat %.4f..%.4f" % (max_tol, n_hot, ll[0], ll[2], ll[1], ll[3]))

    quads = ra.naip_names_for_bbox(*ll)
    ra.log("quarter-quads covering it: %s" % ", ".join(q[1] for q in quads))

    hrefs = ra.naip_resolve_from_container(*ll)
    if not hrefs:
        raise SystemExit("no NAIP .tif resolved from the container for that bbox")

    ok = []
    for h in hrefs:
        label, url, size = ra.open_cog(h)
        ra.log("  verified %s  %s  [%s]" % (os.path.basename(h), size, label))
        ok.append(h)

    old = json.load(open(TARGET))
    ra.log("replacing %d shipped hrefs with %d covering the AOI" % (len(old), len(ok)))
    if dry_run:
        print(json.dumps(ok, indent=2)); return
    if not os.path.exists(BACKUP):
        shutil.copy2(TARGET, BACKUP)
        ra.log("original preserved -> %s" % os.path.basename(BACKUP))
    with open(TARGET, "w") as fh:
        json.dump(ok, fh, indent=2)
        fh.write("\n")
    ra.log("wrote %s" % TARGET)


if __name__ == "__main__":
    main()
