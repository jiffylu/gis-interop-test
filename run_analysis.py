#!/usr/bin/env python3
"""
Overture buildings near 311-hotspot TIGER roads, clipped to San Mateo County.

Pipeline (uses ONLY the data + live endpoints described in README.md):
  09_csv  SF 311 cases 2025      -> snap each case to nearest TIGER road centerline
                                    (sf_311_2025_full.csv from fetch_311_full.py if present,
                                     else the shipped 300k-row / Jan-May slice)
  01_shapefile TIGER roads 06081 -> keep segments with > 20 cases  ("hot segments")
                                 -> 200 m buffer (metric, EPSG:26910)
  07_geoparquet Overture bldgs   -> buildings intersecting that buffer
  03_arcgis_rest Boundaries/0    -> live County_Boundary polygon, used to clip
  01_shapefile CA tracts         -> per-tract counts of matching buildings -> CSV
  08_geotiff naip_hrefs.json     -> NAIP COGs read windowed via /vsicurl -> basemap
                                    (3DEP's Azure container is private now, so COG hrefs
                                     fall back to USGS's public S3 / a PC SAS token)
  -> out/buildings_near_311_hot_roads.html   (single self-contained Leaflet map)
  -> out/buildings_per_tract.csv
  -> out/dataset_report.json / .md           (open/fail status for every README dataset)

Iteration speed: a full run is ~100 s, almost all of it in stages whose output
never changes between edits (CSV parse, 295k-point snap, remote NAIP reads).
analyse() therefore persists its result tables to out/checkpoint.duckdb, and

  python3 run_analysis.py --render-only

rebuilds the HTML, CSV, report and side-car GeoJSONs from that checkpoint in
about two seconds, without touching the network or the source data. It refuses
if COUNTY / SNAP_TOL_M / CASE_THRESHOLD differ from the values the checkpoint
was built with, so a stale checkpoint can never be rendered under new labels.

Deps actually present here: GDAL 3.8.5 CLI, duckdb+spatial, pyarrow, shapely, requests.
(No geopandas / rasterio / pyproj in this interpreter, so GDAL CLI + DuckDB do the work.)
"""
import base64, glob, json, os, re, subprocess, sys
from datetime import datetime

import duckdb
import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "out")
os.makedirs(OUT, exist_ok=True)

# ---- analysis parameters -------------------------------------------------
YEAR              = 2025

# 311 cases. The shipped file is a 300,000-row export cap ending 2025-05-15;
# fetch_311_full.py pulls all 872k rows for 2025 to the _full file, which is
# preferred automatically. Override with CASES_CSV=path.
CASES_CSV = os.environ.get("CASES_CSV") or (
    "09_csv/sf_311_2025_full.csv"
    if os.path.exists(os.path.join(ROOT, "09_csv/sf_311_2025_full.csv"))
    else "09_csv/sf_311_2025.csv")

# Which county's street network the 311 cases are matched against.
#   06075 San Francisco -- the 311 file's own county. Overture's GeoParquet stops
#         at lat 37.72 and misses 95% of the cases, so buildings come from the
#         README's SF footprints instead, and the clip boundary is the dissolved
#         TIGER 06075 tracts (the county Hub in services.json is San Mateo's).
#   06081 San Mateo -- the shipped roads. Only the Daly City line overlaps the
#         311 data, so this yields a thin border strip. Kept for reproducibility.
COUNTY = os.environ.get("COUNTY", "06075")
PROFILES = {
    "06075": dict(
        name="San Francisco", fips="06075", land_km2=121.4,
        roads="01_shapefile/tl_2024_06075_roads.shp",
        buildings=("vector", "04_geojson/sf_buildings.geojson"),
        bldg_fields=[("mblr", "block-lot"), ("hgt_median_m", "height m")],
        boundary=("tracts", None),
        # >20 is meaningless on SF's own network: nearly every segment qualifies
        # and the buffers blanket the city (live figures are in the map panel).
        # 1,500 keeps ~30 segments on the shipped Jan-May slice; the full year
        # has 2.96x the cases, so 4,500 keeps the same selectivity there.
        threshold=int(os.environ.get("CASE_THRESHOLD",
                                     4500 if CASES_CSV.endswith("_full.csv") else 1500))),
    "06081": dict(
        name="San Mateo", fips="06081", land_km2=1163.0,
        roads="01_shapefile/tl_2024_06081_roads.shp",
        buildings=("parquet", "07_geoparquet/overture_buildings.parquet"),
        bldg_fields=[("class", "class"), ("height", "height m")],
        boundary=("arcgis", None),
        threshold=int(os.environ.get("CASE_THRESHOLD", 20))),
}
if COUNTY not in PROFILES:
    raise SystemExit("COUNTY must be one of %s" % ", ".join(PROFILES))
PROFILE           = PROFILES[COUNTY]
CASE_THRESHOLD    = PROFILE["threshold"]   # "more than N" -> strictly greater
SNAP_TOL_M        = float(os.environ.get("SNAP_TOL_M", 50))   # 311 pt -> nearest centerline
BUFFER_M          = 200.0   # "within 200 meters of any ... road segment"
METRIC_CRS        = "EPSG:26910"   # NAD83 / UTM 10N -- matches NAIP, true metres
BOUNDARIES_FS     = ("https://services.arcgis.com/yq3FgOI44hYHAFVZ/arcgis/rest/"
                     "services/Boundaries/FeatureServer")
NAIP_MAX_MPX      = float(os.environ.get("NAIP_MAX_MPX", 24))  # basemap budget, megapixels
                            # (never finer than native 60 cm; a city-wide AOI is
                            #  inherently coarse -- 24 MP over 12 km is ~2.4 m/px)
SIMPLIFY_M        = float(os.environ.get("SIMPLIFY_M", 0.5))  # display-only generalisation
COORD_DP          = 6       # ~0.1 m; full float precision triples the HTML size

CKPT_DB   = os.path.join(OUT, "checkpoint.duckdb")
CKPT_JSON = os.path.join(OUT, "checkpoint.json")
CKPT_PARAMS = dict(county=COUNTY, year=YEAR, snap_tol_m=SNAP_TOL_M,
                   case_threshold=CASE_THRESHOLD, buffer_m=BUFFER_M, cases_csv=CASES_CSV)

def log(*a): print("[%s]" % datetime.now().strftime("%H:%M:%S"), *a, flush=True)
def p(*a):   return os.path.join(ROOT, *a)

def run(cmd, **kw):
    r = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if r.returncode != 0:
        raise RuntimeError("cmd failed: %s\n%s" % (" ".join(cmd[:6]), (r.stderr or "")[-1500:]))
    return r.stdout

# ---- external GDAL binaries ------------------------------------------------
# The analysis itself runs on DuckDB's bundled GDAL, but the NAIP warp, the
# dataset probe and the building-cache conversion shell out to gdalwarp,
# gdalinfo and ogr2ogr. If those aren't on PATH the symptom is a map with no
# imagery and a one-line note -- so find them explicitly and fail loudly.
GDAL_CANDIDATE_DIRS = [
    "/Applications/Postgres.app/Contents/Versions/latest/bin",
    "/opt/homebrew/bin", "/usr/local/bin", "/opt/local/bin",
]
def _ensure_gdal_on_path():
    import shutil
    if shutil.which("gdalinfo") and shutil.which("ogr2ogr"):
        return
    for d in GDAL_CANDIDATE_DIRS:
        if os.path.exists(os.path.join(d, "gdalinfo")):
            os.environ["PATH"] = d + os.pathsep + os.environ.get("PATH", "")
            return
    raise SystemExit("GDAL command-line tools (gdalinfo, gdalwarp, ogr2ogr) not found on PATH "
                     "or in %s. Install GDAL or add its bin directory to PATH." % GDAL_CANDIDATE_DIRS)
_ensure_gdal_on_path()

# ---- remote COG access ---------------------------------------------------
GDAL_ENV = dict(os.environ,
                GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
                CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif",
                VSI_CACHE="TRUE", VSI_CACHE_SIZE="50000000",
                GDAL_HTTP_MULTIPLEX="YES", GDAL_HTTP_VERSION="2")

_SAS_CACHE = {}
def _pc_sas_url(account, container, href):
    """Planetary Computer hands out read SAS tokens anonymously (~45 min TTL)."""
    key = (account, container)
    if key not in _SAS_CACHE:
        r = requests.get("https://planetarycomputer.microsoft.com/api/sas/v1/token/%s/%s"
                         % (account, container), timeout=45)
        r.raise_for_status()
        _SAS_CACHE[key] = r.json()["token"]
    return href + ("&" if "?" in href else "?") + _SAS_CACHE[key]


def cog_routes(href):
    """Ordered candidates for one COG href: the supplied URL first, then mirrors.

    The 3DEP container on ai4edataeuwest went private (HTTP 409
    PublicAccessNotPermitted), so the same rasters are fetched from USGS's own
    public bucket, or from Azure with a signed URL.
    """
    routes = [("as supplied", href)]
    m = re.match(r"https://([a-z0-9]+)\.blob\.core\.windows\.net/([^/]+)/(.+)$", href)
    if m:
        account, container, key = m.groups()
        u = re.match(r"Elevation/([^/]+)/TIFF/([^/]+)/(USGS_[^/]+\.tif)$", key)
        if u:
            res, cell, fn = u.groups()
            routes.append(("USGS public S3 mirror",
                           "https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/"
                           "%s/TIFF/current/%s/%s" % (res, cell, fn)))
        routes.append(("Planetary Computer SAS token",
                       lambda a=account, c=container, h=href: _pc_sas_url(a, c, h)))
    return routes


def http_reason(url, exc):
    """Turn a gdalinfo failure into the server's own explanation where possible."""
    try:
        hr = requests.get(url, headers={"Range": "bytes=0-511"}, timeout=45)
        body = hr.text
        code = body.split("<Code>")[1].split("</Code>")[0] if "<Code>" in body else ""
        msg = (body.split("<Message>")[1].split("</Message>")[0].splitlines()[0]
               if "<Message>" in body else "")
        return ("HTTP %s %s %s" % (hr.status_code, code, msg)).strip()
    except Exception:
        return type(exc).__name__


def open_cog(href, env=None):
    """Read a remote COG's header, walking the route list. -> (route label, size)."""
    tried = []
    for label, cand in cog_routes(href):
        url = cand() if callable(cand) else cand
        try:
            txt = run(["gdalinfo", "/vsicurl/" + url], env=env or GDAL_ENV)
            size = [l for l in txt.splitlines() if l.startswith("Size is")][0]
            return label, url, size.replace("Size is ", "")
        except Exception as e:
            tried.append("%s -> %s" % (label, http_reason(url, e)))
    raise RuntimeError("; ".join(tried))


# =========================================================================
# 0.  Open-ability probe of every dataset the README lists
# =========================================================================
REPORT = []
def probe(key, title, fn):
    rec = {"id": key, "title": title}
    try:
        rec["detail"] = fn(); rec["status"] = "ok"
        log("PROBE ok    %-16s %s" % (key, rec["detail"]))
    except Exception as e:
        rec["status"] = "FAILED"
        rec["error"] = "%s: %s" % (type(e).__name__, str(e).strip().replace("\n", " ")[:400])
        log("PROBE FAIL  %-16s %s" % (key, rec["error"]))
    REPORT.append(rec)
    return rec

def ogr_summary(path, layer=None):
    cmd = ["ogrinfo", "-so", "-ro", path] + ([layer] if layer else ["-al"])
    txt = run(cmd)
    feats = [l.strip() for l in txt.splitlines() if l.strip().startswith("Feature Count")]
    ext   = [l.strip() for l in txt.splitlines() if l.strip().startswith("Extent")]
    return "; ".join(feats[:1] + ext[:1]) or txt.splitlines()[1][:120]

def probe_all():
    log("=== dataset open-ability probe ===")
    probe("01_shapefile", "TIGER roads %s + CA tracts (NAD83)" % PROFILE["fips"],
          lambda: "roads %s: %s | tracts: %s" % (
              PROFILE["fips"], ogr_summary(p(PROFILE["roads"])),
              ogr_summary(p("01_shapefile/tl_2024_06_tract.shp"))))

    def _fgdb():
        txt = run(["ogrinfo", "-ro", p("02_fgdb/NHD_H_18050006_HU8_GDB.gdb")])
        n = len([l for l in txt.splitlines() if l.startswith("Layer:")])
        return "OpenFileGDB, %d layers (e.g. NHDFlowline, NHDWaterbody)" % n
    probe("02_fgdb", "USGS NHD HUC8 18050006 (FGDB)", _fgdb)

    def _rest():
        svc = json.load(open(p("03_arcgis_rest/services.json")))
        alive, dead = [], []
        for s in svc:
            try:
                r = requests.get(s["url"], params={"f": "json"}, timeout=45)
                j = r.json()
                if r.status_code == 200 and "layers" in j and "error" not in j:
                    alive.append("%s(%d lyr)" % (s["title"], len(j["layers"])))
                else:
                    err = j.get("error") or {}
                    dead.append("%s -> ArcGIS error code %s %s (HTTP %s)" % (
                        s["title"], err.get("code", "?"),
                        err.get("message") or "(no message returned)", r.status_code))
            except Exception as e:
                dead.append("%s -> %s" % (s["title"], type(e).__name__))
        if dead:
            REPORT_NOTE.append("03_arcgis_rest unreachable services: " + "; ".join(dead))
        return "%d/%d FeatureServers reachable. %s%s" % (
            len(alive), len(svc), ", ".join(alive[:4]),
            (" | UNREACHABLE: " + "; ".join(dead)) if dead else "")
    probe("03_arcgis_rest", "San Mateo County Hub FeatureServers (live)", _rest)

    def _gj():
        sz = os.path.getsize(p("04_geojson/sf_buildings.geojson")) / 1e6
        txt = run(["ogrinfo", "-so", "-ro", p("04_geojson/sf_buildings.geojson"), "-al"])
        fc = [l.strip() for l in txt.splitlines() if l.strip().startswith("Feature Count")]
        return "%.0f MB; %s" % (sz, fc[0] if fc else "opened")
    probe("04_geojson", "SF building footprints (GeoJSON)", _gj)

    def _gpkg():
        f = glob.glob(p("05_gpkg/packages/*.gpkg"))
        if not f: raise FileNotFoundError("no .gpkg under 05_gpkg/packages")
        txt = run(["ogrinfo", "-ro", f[0]])
        return "%s, %d layers" % (os.path.basename(f[0]),
                                  len([l for l in txt.splitlines() if l.strip()[:1].isdigit()]))
    probe("05_gpkg", "Natural Earth multi-layer GPKG", _gpkg)

    def _kmz():
        kmz = p("06_kml/calfire_perimeters.kmz")
        try:
            return "opened directly: " + ogr_summary(kmz)
        except RuntimeError:
            # This GDAL has the plain 'KML' driver but no LIBKML/KMZ support, so
            # the container itself is unreadable -- GDAL's zip VFS opens the
            # member in place, no unpacking and no second GDAL needed.
            return ("opened via /vsizip (no LIBKML/KMZ driver in this GDAL build, "
                    "so the KMZ is read as a zip): %s" % ogr_summary("/vsizip/" + kmz))

    probe("06_kml", "CAL FIRE perimeters (KMZ)", _kmz)

    def _pq():
        import pyarrow.parquet as pq
        f = pq.ParquetFile(p("07_geoparquet/overture_buildings.parquet"))
        geo = json.loads(f.metadata.metadata[b"geo"].decode())
        return "%d rows, %d row groups, GeoParquet %s, bbox %s" % (
            f.metadata.num_rows, f.metadata.num_row_groups, geo["version"],
            geo["columns"]["geometry"]["bbox"])
    probe("07_geoparquet", "Overture buildings (GeoParquet)", _pq)

    def _cog(fn):
        hrefs = json.load(open(p("08_geotiff", fn)))
        ok, bad, routes = [], [], []
        for h in hrefs:
            try:
                label, url, size = open_cog(h)
                if label not in routes: routes.append(label)
                ok.append("%s %s" % (os.path.basename(h), size))
            except Exception as e:
                bad.append("%s -> %s" % (os.path.basename(h), str(e)[:300]))
        if bad:
            raise RuntimeError("%d/%d hrefs unreadable: %s"
                               % (len(bad), len(hrefs), "; ".join(bad)))
        if routes != ["as supplied"]:
            REPORT_NOTE.append(
                "%s: the supplied hrefs are not publicly readable, so the COGs were opened "
                "via %s instead." % (fn, " / ".join(r for r in routes if r != "as supplied")))
        return "%d/%d COGs readable windowed over /vsicurl [route: %s] (e.g. %s)" % (
            len(ok), len(hrefs), ", ".join(routes), ok[0])

    probe("08_geotiff_naip", "NAIP COGs via STAC hrefs", lambda: _cog("naip_hrefs.json"))
    probe("08_geotiff_3dep", "3DEP elevation COGs via STAC hrefs",
          lambda: _cog("3dep-seamless_hrefs.json"))

    def _csv():
        c = duckdb.connect()
        n, lo, hi, nn = c.execute("""
            select count(*), min(requested_datetime), max(requested_datetime),
                   count(*) filter (where try_cast(lat as double) is null)
            from read_csv(?, header=true, all_varchar=true, ignore_errors=true)
        """, [p(CASES_CSV)]).fetchone()
        return ("%d rows, %s .. %s, %d rows with unusable lat/long; "
                "no CRS declared -> assumed WGS84/EPSG:4326") % (n, lo[:10], hi[:10], nn)
    probe("09_csv", "SF 311 cases 2025 (CSV, lat/long)", _csv)

    def _wfs():
        caps = open(p("10_wfs/usgs_mrds_wfs_capabilities.xml")).read()
        import re
        base = re.search(r'xlink:href="([^"]+)"', caps).group(1)
        r = requests.get(base, params={"service": "WFS", "version": "1.1.0",
                                       "request": "GetFeature", "typename": "ms:mrds",
                                       "bbox": "37.10,-122.55,37.72,-122.08", "maxfeatures": "5"},
                         timeout=60)
        body = r.text[:400].replace("\n", " ")
        if r.status_code != 200 or "Exception" in r.text[:2000]:
            raise RuntimeError("GetFeature HTTP %s: %s" % (r.status_code, body))
        return "MRDS WFS GetFeature HTTP 200 (%d bytes)" % len(r.content)
    probe("10_wfs", "USGS MRDS WFS / Topo WMS (live OGC)", _wfs)

    def _pg():
        env = dict(os.environ, PGPASSWORD="x")
        r = subprocess.run(["psql", "-h", "localhost", "-p", "5432", "-U", "postgres",
                            "-tAc", "select to_regclass('public.tiger_roads')"],
                           capture_output=True, text=True, env=env, timeout=30)
        if r.returncode != 0:
            raise RuntimeError("psql: %s" % (r.stderr or "").strip()[:200])
        tbl = r.stdout.strip()
        ext = subprocess.run(["psql", "-h", "localhost", "-p", "5432", "-U", "postgres",
                              "-tAc", "select string_agg(extname,',') from pg_extension"],
                             capture_output=True, text=True, env=env, timeout=30).stdout.strip()
        if not tbl:
            raise RuntimeError("server reachable, but table public.tiger_roads does not exist "
                               "and PostGIS is not installed (extensions: %s). README marks this "
                               "one '(if loaded)'." % ext)
        return "table tiger_roads present; extensions: %s" % ext
    probe("11_postgis", "PostGIS localhost:5432 tiger_roads (if loaded)", _pg)

REPORT_NOTE = []

# =========================================================================
# 1.  County boundary -- LIVE from the ArcGIS Hub Boundaries FeatureServer
# =========================================================================
def fetch_county_boundary():
    if PROFILE["boundary"][0] == "tracts":
        return boundary_from_tracts()
    log("=== county boundary (live ArcGIS REST) ===")
    meta = requests.get(BOUNDARIES_FS, params={"f": "json"}, timeout=60).json()
    lyr = next(l for l in meta["layers"] if "county" in l["name"].lower())
    log("using layer %s: %s" % (lyr["id"], lyr["name"]))
    url = "%s/%d/query" % (BOUNDARIES_FS, lyr["id"])
    feats, offset = [], 0
    while True:
        j = requests.get(url, params={"where": "1=1", "outFields": "*", "outSR": 4326,
                                      "f": "geojson", "resultOffset": offset,
                                      "resultRecordCount": 1000}, timeout=180).json()
        if "error" in j: raise RuntimeError(j["error"])
        got = j.get("features", [])
        feats += got
        if not j.get("properties", {}).get("exceededTransferLimit") and not j.get("exceededTransferLimit"):
            break
        if not got: break
        offset += len(got)
    fc = {"type": "FeatureCollection", "features": feats}
    dst = os.path.join(OUT, "county_boundary.geojson")
    json.dump(fc, open(dst, "w"))
    log("county boundary: %d polygon(s) -> %s" % (len(feats), os.path.basename(dst)))
    return dst, lyr["name"]


def boundary_from_tracts():
    """Dissolve the county's TIGER tracts into a clip boundary.

    services.json only publishes San Mateo's Hub, so for San Francisco the
    boundary comes from the same TIGER release as the tract roll-up. That has a
    side benefit: clip and tract join then use one rendering of the county line,
    so no buildings land outside the county's own tracts.
    """
    log("=== county boundary (dissolved TIGER %s tracts) ===" % PROFILE["fips"])
    c = duckdb.connect(); c.execute("INSTALL spatial; LOAD spatial;")
    gj_txt = c.execute("""
        select ST_AsGeoJSON(ST_Union_Agg(geom)) from ST_Read(?)
        where STATEFP||COUNTYFP = ?
    """, [p("01_shapefile/tl_2024_06_tract.shp"), PROFILE["fips"]]).fetchone()[0]
    n = c.execute("select count(*) from ST_Read(?) where STATEFP||COUNTYFP = ?",
                  [p("01_shapefile/tl_2024_06_tract.shp"), PROFILE["fips"]]).fetchone()[0]
    fc = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"NAME": PROFILE["name"], "source":
         "dissolved TIGER 2024 tracts %s" % PROFILE["fips"]},
         "geometry": json.loads(gj_txt)}]}
    dst = os.path.join(OUT, "county_boundary.geojson")
    json.dump(fc, open(dst, "w"))
    log("dissolved %d tracts -> %s" % (n, os.path.basename(dst)))
    return dst, "dissolved TIGER %s tracts" % PROFILE["fips"]


def buildings_metric_source():
    """A metric-CRS, spatially indexed copy of a big vector building layer.

    ogr2ogr streams the 321 MB / 177k-feature SF GeoJSON in ~10 s; caching it as
    GeoPackage keeps every later run cheap. Rebuilt whenever the source is newer.
    """
    kind, src = PROFILE["buildings"]
    if kind != "vector":
        return None
    cache = p("cache"); os.makedirs(cache, exist_ok=True)
    dst = os.path.join(cache, os.path.splitext(os.path.basename(src))[0] + "_26910.gpkg")
    if os.path.exists(dst) and os.path.getmtime(dst) >= os.path.getmtime(p(src)):
        return dst
    log("caching %s -> %s (reprojected to %s, spatially indexed)"
        % (os.path.basename(src), os.path.basename(dst), METRIC_CRS))
    run(["ogr2ogr", "-f", "GPKG", dst, p(src), "-t_srs", METRIC_CRS,
         "-nln", "buildings", "-nlt", "MULTIPOLYGON",
         "-select", ",".join(f for f, _ in PROFILE["bldg_fields"]),
         "-lco", "SPATIAL_INDEX=YES"])
    return dst


# =========================================================================
# 2.  The spatial analysis, in DuckDB spatial (metric CRS EPSG:26910)
# =========================================================================
def analyse(county_geojson):
    # File-backed so the result tables outlive this process (see --render-only).
    for f in (CKPT_DB, CKPT_DB + ".wal"):
        if os.path.exists(f): os.remove(f)
    c = duckdb.connect(CKPT_DB)
    c.execute("INSTALL spatial; LOAD spatial;")
    c.execute("SET preserve_insertion_order=false;")

    # -- county boundary (live) -> metric
    c.execute("""
      create table county as
      select ST_Union_Agg(ST_Transform(geom,'EPSG:4326',?,true)) g
      from ST_Read(?)
    """, [METRIC_CRS, county_geojson])

    # -- TIGER roads (EPSG:4269 NAD83) -> metric
    c.execute("""
      create table roads as
      select LINEARID, coalesce(FULLNAME,'(unnamed)') FULLNAME, MTFCC, RTTYP,
             ST_Transform(geom,'EPSG:4269',?,true) g
      from ST_Read(?)
    """, [METRIC_CRS, p(PROFILE["roads"])])
    n_roads = c.execute("select count(*) from roads").fetchone()[0]

    # -- SF 311, 2025 only, valid coords. CSV declares no CRS -> WGS84 assumed.
    c.execute("""
      create table cases as
      select service_request_id sid, service_name, street,
             try_cast(long as double) lon, try_cast(lat as double) lat
      from read_csv(?, header=true, all_varchar=true, ignore_errors=true)
      where try_cast(lat  as double) between  -90 and  90
        and try_cast(long as double) between -180 and 180
        and try_cast(substr(requested_datetime,1,4) as int) = ?
    """, [p(CASES_CSV), YEAR])
    n_cases = c.execute("select count(*) from cases").fetchone()[0]

    # clip candidate cases to the roads' envelope (+1 km) before the metric transform
    rb = c.execute("""select ST_XMin(e),ST_YMin(e),ST_XMax(e),ST_YMax(e)
                      from (select ST_Extent(ST_Union_Agg(g)) e from roads)""").fetchone()
    c.execute("""
      create table cases_m as
      select sid, service_name, street, lon, lat,
             ST_Transform(ST_Point(lon,lat),'EPSG:4326',?,true) g
      from cases
    """, [METRIC_CRS])
    c.execute("""delete from cases_m where ST_X(g) not between ? and ?
                                        or ST_Y(g) not between ? and ?""",
              [rb[0]-1000, rb[2]+1000, rb[1]-1000, rb[3]+1000])
    n_near = c.execute("select count(*) from cases_m").fetchone()[0]

    # -- snap each case to its NEAREST road centreline within SNAP_TOL_M.
    # TIGER splits a street at every intersection, so a case near a corner is
    # exactly equidistant from the two segments meeting there. Break those ties
    # on LINEARID: an arbitrary choice, but a STABLE one -- without it a segment
    # sitting on the > CASE_THRESHOLD boundary flips between runs.
    c.execute("""
      create table assign as
      select sid, LINEARID from (
        select p.sid, r.LINEARID,
               row_number() over (partition by p.sid
                                  order by ST_Distance(p.g, r.g), r.LINEARID) rn
        from cases_m p join roads r on ST_DWithin(p.g, r.g, ?)
      ) where rn = 1
    """, [SNAP_TOL_M])
    n_assigned = c.execute("select count(*) from assign").fetchone()[0]

    n_tied = c.execute("""
      with d as (select p.sid, ST_Distance(p.g, r.g) dist
                 from cases_m p join roads r on ST_DWithin(p.g, r.g, ?)),
           m as (select sid, min(dist) md from d group by 1)
      select count(*) from (
        select d.sid from d join m using (sid)
        where d.dist <= m.md + 1e-9 group by d.sid having count(*) > 1)
    """, [SNAP_TOL_M]).fetchone()[0]
    if n_tied:
        REPORT_NOTE.append(
            "%d of the %d snapped 311 cases (%.1f%%) are exactly equidistant from two or more "
            "TIGER segments, almost always the two halves of a street either side of an "
            "intersection. Ties are broken on LINEARID so runs are reproducible; a segment "
            "sitting right on the %d-case threshold can still be sensitive to that choice."
            % (n_tied, n_assigned, 100.0 * n_tied / max(n_assigned, 1), CASE_THRESHOLD))

    # How selective is the README's literal ">20 cases" rule on this data? Kept
    # in stats so the map panel quotes live figures rather than a stale sweep.
    n_gt20, km2_gt20 = c.execute("""
      select count(*), coalesce(ST_Area(ST_Union_Agg(ST_Buffer(r.g, ?)))/1e6, 0)
      from (select LINEARID, count(*) n from assign group by 1) a
      join roads r using (LINEARID) where a.n > 20
    """, [BUFFER_M]).fetchone()

    c.execute("""
      create table hot as
      select r.LINEARID, r.FULLNAME, r.MTFCC, r.RTTYP, a.n_cases, r.g
      from (select LINEARID, count(*) n_cases from assign group by 1) a
      join roads r using (LINEARID)
      where a.n_cases > ?
    """, [CASE_THRESHOLD])
    n_hot = c.execute("select count(*) from hot").fetchone()[0]
    log("roads=%d | 311 cases %d (%d within roads envelope) | snapped<=%gm=%d | "
        "segments with >%d cases = %d" %
        (n_roads, n_cases, n_near, SNAP_TOL_M, n_assigned, CASE_THRESHOLD, n_hot))

    if n_hot == 0:
        _drop_intermediates(c)
        return c, dict(n_roads=n_roads, n_cases=n_cases, n_near=n_near,
                       n_assigned=n_assigned, n_hot=0, n_bldg=0, n_bldg_clipped=0)

    # -- 200 m buffer around the hot segments, dissolved
    c.execute("create table zone as select ST_Union_Agg(ST_Buffer(g, ?)) g from hot", [BUFFER_M])
    zb = c.execute("""select ST_XMin(g),ST_YMin(g),ST_XMax(g),ST_YMax(g) from zone""").fetchone()

    # zone bbox back to 4326 so we can push a cheap predicate down onto the parquet
    ll = c.execute("""
      select ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b) from (
        select ST_Extent(ST_Transform(ST_GeomFromText(?), ?, 'EPSG:4326', true)) b)
    """, ["POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" % (
            zb[0],zb[1], zb[2],zb[1], zb[2],zb[3], zb[0],zb[3], zb[0],zb[1]), METRIC_CRS]).fetchone()

    # -- building footprints, normalised to a common schema per profile
    kind, src = PROFILE["buildings"]
    if kind == "parquet":
        # Overture GeoParquet; DuckDB exposes the WKB column as GEOMETRY already
        c.execute("""
          create table bldg_all as
          select id, height, num_floors, class, subtype,
                 ST_Transform(geometry,'EPSG:4326',?,true) g
          from read_parquet(?)
          where ST_Intersects(geometry, ST_MakeEnvelope(?,?,?,?))
        """, [METRIC_CRS, p(src), ll[0], ll[1], ll[2], ll[3]])
    else:
        c.execute("""
          create table bldg_all as
          select mblr id, try_cast(hgt_median_m as double) height,
                 null::integer num_floors, null::varchar "class", null::varchar subtype,
                 geom g
          from ST_Read(?)
        """, [buildings_metric_source()])
    log("building footprints loaded: %d (%s)"
        % (c.execute("select count(*) from bldg_all").fetchone()[0], os.path.basename(src)))

    # Select against the INDIVIDUAL buffered segments rather than their union:
    # "within BUFFER_M of any segment" is the same set, but testing 177k polygons
    # against one giant dissolved multipolygon exhausts memory and segfaults.
    c.execute("""
      create table bldg as
      select b.* from bldg_all b
      where exists (select 1 from hot h where ST_DWithin(b.g, h.g, ?))
    """, [BUFFER_M])
    n_bldg = c.execute("select count(*) from bldg").fetchone()[0]

    # -- clip to the county boundary. Split the work: footprints wholly inside
    # keep their own geometry, and only the few straddling the line pay for a
    # constructive ST_Intersection against the (large) county polygon.
    c.execute("""
      create table bldg_clip as
      select b.id, b.height, b.num_floors, b.class, b.subtype, b.g
      from bldg b, county k where ST_Within(b.g, k.g)
    """)
    n_whole = c.execute("select count(*) from bldg_clip").fetchone()[0]
    c.execute("""
      insert into bldg_clip
      select b.id, b.height, b.num_floors, b.class, b.subtype,
             ST_Intersection(b.g, k.g)
      from bldg b, county k
      where not ST_Within(b.g, k.g) and ST_Intersects(b.g, k.g)
    """)
    c.execute("delete from bldg_clip where ST_IsEmpty(g) or ST_Area(g) <= 0")
    log("  %d footprints wholly inside the county, %d trimmed at the boundary"
        % (n_whole, c.execute("select count(*) from bldg_clip").fetchone()[0] - n_whole))
    n_clip = c.execute("select count(*) from bldg_clip").fetchone()[0]
    log("buildings: %d within %gm of a hot segment -> %d after clipping to the county boundary"
        % (n_bldg, BUFFER_M, n_clip))

    # -- census tracts (TIGER CA statewide, EPSG:4269) restricted to the zone bbox
    c.execute("""
      create table tracts_bbox as
      select GEOID, STATEFP, COUNTYFP, TRACTCE, NAMELSAD,
             ST_Transform(geom,'EPSG:4269',?,true) g
      from ST_Read(?)
      where ST_Intersects(geom, ST_MakeEnvelope(?,?,?,?))
    """, [METRIC_CRS, p("01_shapefile/tl_2024_06_tract.shp"),
          ll[0]-0.05, ll[1]-0.05, ll[2]+0.05, ll[3]+0.05])
    # keep only tracts the 200 m buffer actually reaches
    c.execute("""create table tracts as
                 select t.* from tracts_bbox t, zone z where ST_Intersects(t.g, z.g)""")
    log("census tracts intersecting the %gm buffer: %d (of %d in its bbox)" %
        (BUFFER_M, c.execute("select count(*) from tracts").fetchone()[0],
         c.execute("select count(*) from tracts_bbox").fetchone()[0]))

    c.execute("""
      create table tract_counts as
      select t.GEOID, t.STATEFP, t.COUNTYFP, t.TRACTCE, t.NAMELSAD,
             count(b.id) n_buildings
      from tracts t
      left join bldg_clip b on ST_Intersects(t.g, ST_Centroid(b.g))
      group by 1,2,3,4,5
    """)
    # Diagnostic: the county's own "County Land Only" polygon and TIGER's tract
    # geometry do not agree exactly along the Daly City line, so a few buildings
    # that survive the county clip still fall inside TIGER's SF (06075) tracts.
    stray = c.execute("""select coalesce(sum(n_buildings),0) from tract_counts
                         where STATEFP||COUNTYFP <> ?""", [PROFILE["fips"]]).fetchone()[0]
    if stray:
        overlap_m2 = c.execute("""
            select coalesce(ST_Area(ST_Intersection(k.g, ST_Union_Agg(t.g))),0)
            from county k, tracts t where t.STATEFP||t.COUNTYFP <> ?
            group by k.g""", [PROFILE["fips"]]).fetchone()
        note = ("Boundary sources disagree: %d of the %d clipped buildings fall in TIGER "
                "tracts outside county %s even after the clip, because the clip boundary and "
                "the tract layer render the same county line differently (%.1f ha of overlap "
                "inside this AOI)." %
                (stray, n_clip, PROFILE["fips"], (overlap_m2[0] if overlap_m2 else 0) / 1e4))
        log(note); REPORT_NOTE.append(note)

    _drop_intermediates(c)
    return c, dict(n_roads=n_roads, n_cases=n_cases, n_near=n_near, n_assigned=n_assigned,
                   n_hot=n_hot, n_bldg=n_bldg, n_bldg_clipped=n_clip,
                   n_seg_gt20=n_gt20, pct_land_gt20=100.0 * km2_gt20 / PROFILE["land_km2"],
                   zone_bbox_m=list(zb), zone_bbox_ll=list(ll))


def _drop_intermediates(c):
    """Only what the renderers read stays in the checkpoint: county, hot, zone,
    bldg_clip, tracts, tract_counts. The 177k-row building table and the
    295k-row case tables are rebuilt on the next full run anyway."""
    for t in ("cases", "cases_m", "assign", "roads", "bldg_all", "bldg", "tracts_bbox"):
        c.execute("drop table if exists %s" % t)


# =========================================================================
# 2b. Checkpoint -- lets every later edit skip the ~100 s of stable stages
# =========================================================================
def save_checkpoint(c, stats, naip, county_layer):
    meta = dict(params=CKPT_PARAMS, saved=datetime.now().isoformat(timespec="seconds"),
                stats=stats, county_layer=county_layer,
                naip=({k: v for k, v in naip.items() if k != "data_uri"} if naip else None),
                report=REPORT, notes=REPORT_NOTE)
    json.dump(meta, open(CKPT_JSON, "w"), indent=1)
    c.execute("CHECKPOINT")
    log("checkpoint saved -> %s (%.1f MB) + %s" % (
        os.path.basename(CKPT_DB), os.path.getsize(CKPT_DB) / 1e6, os.path.basename(CKPT_JSON)))


def load_checkpoint():
    if not (os.path.exists(CKPT_DB) and os.path.exists(CKPT_JSON)):
        raise SystemExit("--render-only: no checkpoint in %s; run once without the flag first" % OUT)
    meta = json.load(open(CKPT_JSON))
    if meta["params"] != CKPT_PARAMS:
        diff = ["%s: checkpoint=%r now=%r" % (k, meta["params"].get(k), CKPT_PARAMS[k])
                for k in CKPT_PARAMS if meta["params"].get(k) != CKPT_PARAMS[k]]
        raise SystemExit("--render-only: checkpoint was built with different parameters "
                         "(%s). Run the full pipeline instead." % "; ".join(diff))
    log("=== rendering from checkpoint saved %s ===" % meta["saved"])
    c = duckdb.connect(CKPT_DB)
    c.execute("INSTALL spatial; LOAD spatial;")
    REPORT[:] = meta["report"]
    REPORT_NOTE[:] = meta["notes"]
    naip = meta["naip"]
    if naip:
        jpg = os.path.join(OUT, "naip_aoi.jpg")
        if not os.path.exists(jpg):
            raise SystemExit("--render-only: checkpoint expects %s but it is missing" % jpg)
        naip["data_uri"] = "data:image/jpeg;base64," + base64.b64encode(open(jpg, "rb").read()).decode()
    return c, meta["stats"], naip, meta["county_layer"]


# =========================================================================
# 3.  NAIP basemap -- windowed reads of the remote COGs, warped to EPSG:3857
# =========================================================================
def naip_tile_extent(href, env):
    """wgs84 extent of a remote NAIP COG, header reads only."""
    j = json.loads(run(["gdalinfo", "-json", "/vsicurl/" + href], env=env))
    ring = j["wgs84Extent"]["coordinates"][0]
    xs = [c[0] for c in ring]; ys = [c[1] for c in ring]
    return (min(xs), min(ys), max(xs), max(ys))


def naip_names_for_bbox(lonmin, latmin, lonmax, latmax):
    """USGS quarter-quad names covering a bbox: m_<cell><quad><sub>_10_060_ prefixes.

    A 1x1 degree cell is an 8x8 grid of 7.5' quads numbered 1..64, row 1 at the
    north edge and column 1 at the west edge; each quad splits into nw/ne/sw/se.
    Verified against the five hrefs in 08_geotiff/naip_hrefs.json.
    """
    import math
    out = []
    for latd in range(int(math.floor(latmin)), int(math.floor(latmax)) + 1):
        for lond in range(int(math.floor(-lonmax)), int(math.floor(-lonmin)) + 1):
            cell = "%02d%03d" % (latd, lond)
            for row in range(8):
                qlat1 = (latd + 1) - row * 0.125          # north edge of this quad row
                qlat0 = qlat1 - 0.125
                if qlat0 > latmax or qlat1 < latmin: continue
                for col in range(8):
                    qlon0 = -(lond + 1) + col * 0.125     # west edge of this quad column
                    qlon1 = qlon0 + 0.125
                    if qlon0 > lonmax or qlon1 < lonmin: continue
                    q = row * 8 + col + 1
                    for sub, (a, b, cx, dy) in {
                            "nw": (qlon0, qlon0 + .0625, qlat0 + .0625, qlat1),
                            "ne": (qlon0 + .0625, qlon1, qlat0 + .0625, qlat1),
                            "sw": (qlon0, qlon0 + .0625, qlat0, qlat0 + .0625),
                            "se": (qlon0 + .0625, qlon1, qlat0, qlat0 + .0625)}.items():
                        if a > lonmax or b < lonmin or cx > latmax or dy < latmin: continue
                        out.append((cell, "m_%s%02d_%s_10_060_" % (cell, q, sub)))
    return out


def naip_resolve_from_container(lonmin, latmin, lonmax, latmax, year=2022):
    """Resolve real .tif hrefs from the SAME public NAIP container the README's
    hrefs point at, by listing the deterministic quarter-quad prefix."""
    base = "https://naipeuwest.blob.core.windows.net/naip"
    found = []
    for cell, pref in naip_names_for_bbox(lonmin, latmin, lonmax, latmax):
        key = "v002/ca/%d/ca_060cm_%d/%s/%s" % (year, year, cell, pref)
        r = requests.get(base, params={"restype": "container", "comp": "list",
                                       "prefix": key, "maxresults": "50"}, timeout=60)
        r.raise_for_status()
        import re
        for name in re.findall(r"<Name>([^<]+)</Name>", r.text):
            if name.endswith(".tif"):
                found.append(base + "/" + name)
    return sorted(set(found))


# =========================================================================
# 3.  NAIP basemap -- windowed reads of the remote COGs, warped to EPSG:3857
# =========================================================================
def naip_overlay(c, zone_bbox_m, pad_m=250.0):
    log("=== NAIP basemap (windowed /vsicurl reads) ===")
    x0, y0, x1, y1 = zone_bbox_m
    x0 -= pad_m; y0 -= pad_m; x1 += pad_m; y1 += pad_m
    wkt = "POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" % (x0,y0, x1,y0, x1,y1, x0,y1, x0,y0)
    m = c.execute("""select ST_XMin(b),ST_YMin(b),ST_XMax(b),ST_YMax(b) from
                     (select ST_Extent(ST_Transform(ST_GeomFromText(?),?,'EPSG:3857',true)) b)""",
                  [wkt, METRIC_CRS]).fetchone()
    ll = c.execute("""select ST_XMin(b),ST_YMin(b),ST_XMax(b),ST_YMax(b) from
                      (select ST_Extent(ST_Transform(ST_GeomFromText(?),'EPSG:3857','EPSG:4326',true)) b)""",
                   ["POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))" %
                    (m[0],m[1], m[2],m[1], m[2],m[3], m[0],m[3], m[0],m[1])]).fetchone()

    env = GDAL_ENV

    supplied = json.load(open(p("08_geotiff/naip_hrefs.json")))
    hits, source = [], "08_geotiff/naip_hrefs.json"
    for h in supplied:
        a, b, cc, d = naip_tile_extent(h, env)
        if not (a > ll[2] or b > ll[3] or cc < ll[0] or d < ll[1]):
            hits.append(h)
    if not hits:
        msg = ("None of the %d NAIP hrefs in 08_geotiff/naip_hrefs.json intersect the analysis "
               "area: they are all quarter-quads at lat 37.060-37.127 (the far south of the "
               "county, near Ano Nuevo), while the 311/road overlap sits at lat %.3f-%.3f on the "
               "Daly City county line. Resolved the covering quarter-quad(s) from the SAME public "
               "NAIP container instead, using the USGS quad-naming convention."
               % (len(supplied), ll[1], ll[3]))
        log("NAIP: " + msg)
        REPORT_NOTE.append(msg)
        hits = naip_resolve_from_container(ll[0], ll[1], ll[2], ll[3])
        source = "naipeuwest.blob.core.windows.net container listing (derived)"
        if not hits:
            raise RuntimeError("no NAIP quarter-quad found for the AOI")
    log("NAIP tiles used (%s): %s" % (source, ", ".join(os.path.basename(h) for h in hits)))

    vrt = os.path.join(OUT, "_naip.vrt")
    run(["gdalbuildvrt", "-overwrite", vrt] + ["/vsicurl/" + h for h in hits], env=env)

    import math
    k = 1.0 / math.cos(math.radians((ll[1] + ll[3]) / 2.0))
    px = 0.6 * k
    w = max(1, int((m[2] - m[0]) / px)); h_ = max(1, int((m[3] - m[1]) / px))
    # budget on total pixels, not on the long edge: a wide, shallow AOI would
    # otherwise be forced to a far coarser ground resolution than a square one
    if w * h_ > NAIP_MAX_MPX * 1e6:
        s_ = (NAIP_MAX_MPX * 1e6 / float(w * h_)) ** 0.5
        w = max(1, int(w * s_)); h_ = max(1, int(h_ * s_))
    log("warping NAIP -> EPSG:3857 %dx%d px (%.2f m/px) over %.0f x %.0f m"
        % (w, h_, (x1-x0)/w, x1-x0, y1-y0))

    tif = os.path.join(OUT, "naip_aoi_3857.tif")
    run(["gdalwarp", "-overwrite", "-t_srs", "EPSG:3857",
         "-te", str(m[0]), str(m[1]), str(m[2]), str(m[3]),
         "-ts", str(w), str(h_), "-r", "cubic", "-multi",
         "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES",
         "-wo", "NUM_THREADS=ALL_CPUS", vrt, tif], env=env)

    txt = run(["gdalinfo", "-stats", tif], env=env)
    if all(x in txt for x in ("STATISTICS_MAXIMUM=0",)) and txt.count("STATISTICS_MAXIMUM=0") >= 3:
        raise RuntimeError("NAIP warp produced an empty (all-zero) window for the AOI")

    jpg = os.path.join(OUT, "naip_aoi.jpg")
    run(["gdal_translate", "-of", "JPEG", "-co", "QUALITY=82",
         "-b", "1", "-b", "2", "-b", "3", "-ot", "Byte", tif, jpg], env=env)
    for junk in (jpg + ".aux.xml", vrt, tif + ".aux.xml"):
        if os.path.exists(junk): os.remove(junk)
    b64 = base64.b64encode(open(jpg, "rb").read()).decode()
    log("NAIP overlay %.2f MB jpeg (%s)" % (os.path.getsize(jpg)/1e6, os.path.basename(jpg)))
    return {"data_uri": "data:image/jpeg;base64," + b64,
            "bounds": [[ll[1], ll[0]], [ll[3], ll[2]]],
            "tif": os.path.basename(tif), "px": [w, h_],
            "source": source,
            "tiles": [os.path.basename(x) for x in hits]}


# =========================================================================
# 4.  Outputs
# =========================================================================
def gj(c, sql, params=None):
    """Run a query whose last column is a metric geometry -> GeoJSON FeatureCollection."""
    rel = c.execute(sql, params or [])
    cols = [d[0] for d in rel.description]
    feats = []
    for row in rel.fetchall():
        props = {k: (None if v is None else v) for k, v in zip(cols[:-1], row[:-1])}
        geom = json.loads(row[-1])
        geom["coordinates"] = _round(geom["coordinates"])
        feats.append({"type": "Feature", "properties": props, "geometry": geom})
    return {"type": "FeatureCollection", "features": feats}

def to4326(col="g", simplify=False):
    """Metric geometry -> GeoJSON text in WGS84, optionally generalised first.

    Simplification is display-only: it runs in metres, after every count and
    spatial test has been made against the full-resolution footprints.
    """
    expr = col
    if simplify and SIMPLIFY_M > 0:
        expr = "ST_SimplifyPreserveTopology(%s, %g)" % (col, SIMPLIFY_M)
    return "ST_AsGeoJSON(ST_Transform(%s,'%s','EPSG:4326',true))" % (expr, METRIC_CRS)


def _round(o, dp=COORD_DP):
    """Trim coordinate precision in a parsed GeoJSON geometry, in place."""
    if isinstance(o, list):
        return [_round(x, dp) for x in o]
    if isinstance(o, float):
        return round(o, dp)
    return o

def write_tract_csv(c):
    import csv
    dst = os.path.join(OUT, "buildings_per_tract.csv")
    rows = c.execute("""
        select GEOID, STATEFP, COUNTYFP, TRACTCE, NAMELSAD, n_buildings
        from tract_counts order by n_buildings desc, GEOID
    """).fetchall()
    with open(dst, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["GEOID", "STATEFP", "COUNTYFP", "TRACTCE", "NAMELSAD", "n_buildings"])
        w.writerows(rows)
    nz = sum(1 for r in rows if r[5] > 0)
    log("wrote %s: %d tracts (%d with >=1 matching building), total %d buildings"
        % (os.path.basename(dst), len(rows), nz, sum(r[5] for r in rows)))
    return dst, rows

def write_report():
    j = os.path.join(OUT, "dataset_report.json")
    json.dump({"generated": datetime.now().isoformat(timespec="seconds"),
               "datasets": REPORT, "notes": REPORT_NOTE}, open(j, "w"), indent=2)
    md = os.path.join(OUT, "dataset_report.md")
    with open(md, "w") as fh:
        fh.write("# Dataset open-ability report\n\n")
        fh.write("| # | dataset | status | detail |\n|---|---|---|---|\n")
        for r in REPORT:
            fh.write("| %s | %s | %s | %s |\n" % (
                r["id"], r["title"], "OK" if r["status"] == "ok" else "**FAILED**",
                (r.get("detail") or r.get("error", "")).replace("|", "\\|")))
    return j, md


HTML_TMPL = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<style>
  :root{--bg:#0e1116;--panel:#161b22e6;--fg:#e6edf3;--mut:#8b949e;--line:#30363d;
        --bldg:#ff8a3d;--road:#ffd60a;--zone:#4cc9f0;--county:#ff4d6d;--tract:#a78bfa;}
  html,body{margin:0;height:100%;background:var(--bg);color:var(--fg);
        font:13px/1.45 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif}
  #map{position:absolute;inset:0}
  .pane{position:absolute;z-index:1000;background:var(--panel);backdrop-filter:blur(8px);
        border:1px solid var(--line);border-radius:10px;box-shadow:0 8px 28px #0009}
  #info{top:12px;left:12px;width:335px;max-height:calc(100% - 96px);overflow:auto;padding:14px 16px}
  #info.min{max-height:44px;overflow:hidden}
  #toggle{position:absolute;z-index:1001;top:12px;left:359px;padding:7px 11px;cursor:pointer;
          background:var(--panel);border:1px solid var(--line);border-radius:8px;color:var(--fg)}
  #info.min ~ #toggle{left:12px}
  #info h1{font-size:15px;margin:0 0 2px}
  #info h2{font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:var(--mut);
        margin:16px 0 6px;border-top:1px solid var(--line);padding-top:10px}
  #info p.sub{margin:0 0 4px;color:var(--mut);font-size:12px}
  table.kv{width:100%;border-collapse:collapse}
  table.kv td{padding:2px 0;vertical-align:top}
  table.kv td:last-child{text-align:right;font-variant-numeric:tabular-nums;font-weight:600}
  .warn{background:#3d1d20;border:1px solid #7d2b33;border-radius:7px;padding:8px 10px;
        margin:10px 0 0;color:#ffc9cf;font-size:12px}
  .ok{color:#3fb950}.bad{color:#ff7b72}
  ul.ds{list-style:none;margin:0;padding:0;font-size:12px}
  ul.ds li{padding:3px 0;border-bottom:1px dotted #ffffff14}
  ul.ds code{color:var(--mut)}
  .sw{display:inline-block;width:11px;height:11px;border-radius:2px;margin-right:6px;
      vertical-align:-1px;border:1px solid #fff4}
  .leaflet-popup-content-wrapper,.leaflet-popup-tip{background:#161b22;color:var(--fg);
      border:1px solid var(--line)}
  .leaflet-popup-content{margin:9px 12px;font-size:12px}
  .leaflet-container{background:#0e1116}
  #legend{bottom:12px;right:12px;padding:10px 13px;font-size:12px}
  #legend div{margin:3px 0}
  a{color:#58a6ff}
</style></head><body>
<div id="map"></div>
<div class="pane" id="info">__INFO__</div>
<button id="toggle" title="show / hide the panel">&#9776;</button>
<div class="pane" id="legend">__LEGEND__</div>
<script>
const D = __DATA__;
document.getElementById('toggle').onclick = () =>
  document.getElementById('info').classList.toggle('min');
const map = L.map('map',{preferCanvas:true});

const osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
   {maxZoom:20,attribution:'&copy; OpenStreetMap'});
const bases = {};
let naip = null;
if (D.naip){ naip = L.imageOverlay(D.naip.data_uri, D.naip.bounds, {opacity:1, className:'naip'});
             bases['NAIP 2022 aerial (60 cm, windowed from COGs)'] = naip; }
bases['OpenStreetMap'] = osm;
bases['No basemap'] = L.layerGroup([]);
(naip||osm).addTo(map);

const county = L.geoJSON(D.county, {style:{color:'#ff4d6d',weight:2.5,dashArray:'7 5',
   fill:false,interactive:false}});
const zone   = L.geoJSON(D.zone, {style:{color:'#4cc9f0',weight:1,fillColor:'#4cc9f0',
   fillOpacity:.13,interactive:false}});
const roads  = L.geoJSON(D.hot_roads, {style:{color:'#ffd60a',weight:4,opacity:.95},
   onEachFeature:(f,l)=>l.bindPopup(
     `<b>${f.properties.FULLNAME}</b><br>LINEARID ${f.properties.LINEARID}`+
     `<br>MTFCC ${f.properties.MTFCC}<br><b>${f.properties.n_cases}</b> SF 311 cases in ${D.year}`)});
const bldgs  = L.geoJSON(D.buildings, {style:{color:'#ff8a3d',weight:.6,fillColor:'#ff8a3d',
   fillOpacity:.65},
   onEachFeature:(f,l)=>l.bindPopup(
     `<b>Building</b><br><code>${f.properties.id||''}</code>`+
     `<br>class: ${f.properties.class||'n/a'} / ${f.properties.subtype||'n/a'}`+
     `<br>height: ${f.properties.height==null?'n/a':(+f.properties.height).toFixed(1)+' m'}`+
     `<br>floors: ${f.properties.num_floors==null?'n/a':f.properties.num_floors}`)});

const maxT = Math.max(1, ...D.tracts.features.map(f=>f.properties.n_buildings||0));
const ramp = n => `hsl(${272-46*Math.sqrt(n/maxT)} 70% ${34+30*Math.sqrt(n/maxT)}%)`;
const tracts = L.geoJSON(D.tracts, {style:f=>{const n=f.properties.n_buildings||0; return {
   color:'#b7a4f5', weight:1, opacity:.75,
   fillColor:ramp(n), fillOpacity: n ? 0.16+0.26*Math.sqrt(n/maxT) : 0.05};},
   onEachFeature:(f,l)=>l.bindPopup(
     `<b>Tract ${f.properties.GEOID}</b><br>${f.properties.NAMELSAD}`+
     `<br>county FIPS ${f.properties.STATEFP}${f.properties.COUNTYFP}`+
     `<br><b>${f.properties.n_buildings}</b> matching buildings`)});

tracts.addTo(map); zone.addTo(map); county.addTo(map); roads.addTo(map); bldgs.addTo(map);
L.control.layers(bases, {
  [D.bldg_label]:bldgs,
  '200 m buffer of hot road segments':zone,
  ['TIGER road segments > '+D.threshold+' cases']:roads,
  [D.county_label]:county,
  'Census tracts (building counts)':tracts
},{collapsed:false,position:'topright'}).addTo(map);
L.control.scale({imperial:false}).addTo(map);

const b = bldgs.getBounds().isValid() ? bldgs.getBounds() : zone.getBounds();
const target = b.isValid() ? b.pad(0.25) : L.latLngBounds([[37.10,-122.55],[37.72,-122.08]]);

// The container can still be measuring when this runs (the NAIP overlay is a
// multi-hundred-kB data URI), and Leaflet then resolves the fit to zoom 0.
// Keep re-fitting until the view actually matches -- but stop the moment the
// user touches the map, so we never fight their panning.
let settled = false;
const stop = () => { settled = true; };
['mousedown','wheel','touchstart','keydown'].forEach(e =>
  map.getContainer().addEventListener(e, stop, {passive:true, once:true}));
function fit(){
  if (settled) return;
  map.invalidateSize({animate:false});
  const want = map.getBoundsZoom(target);
  if (map.getZoom() !== want) map.fitBounds(target, {animate:false});
}
map.setView(target.getCenter(), 15);
fit();
window.addEventListener('load', fit);
[0, 60, 250, 800].forEach(t => setTimeout(fit, t));
new ResizeObserver(fit).observe(document.getElementById('map'));
</script></body></html>
"""

def build_html(c, stats, naip, county_layer_name, tract_rows, dst):
    hot = gj(c, "select LINEARID, FULLNAME, MTFCC, RTTYP, n_cases, %s from hot order by LINEARID" % to4326()) \
          if stats["n_hot"] else {"type": "FeatureCollection", "features": []}
    zone = gj(c, "select 1 as i, %s from zone" % to4326()) \
          if stats["n_hot"] else {"type": "FeatureCollection", "features": []}
    bl = gj(c, "select id, height, num_floors, class, subtype, %s from bldg_clip order by id, ST_XMin(g), ST_YMin(g)"
                % to4326(simplify=True)) \
          if stats["n_hot"] else {"type": "FeatureCollection", "features": []}
    tr = gj(c, """select t.GEOID, t.STATEFP, t.COUNTYFP, t.NAMELSAD, tc.n_buildings, %s
                  from tracts t join tract_counts tc using (GEOID) order by t.GEOID""" % to4326("t.g")) \
          if stats["n_hot"] else {"type": "FeatureCollection", "features": []}
    county = json.load(open(os.path.join(OUT, "county_boundary.geojson")))

    top = [r for r in tract_rows if r[5] > 0][:6]
    fails = [r for r in REPORT if r["status"] != "ok"]

    info = []
    src_label = {"parquet": "Overture", "vector": "SF"}[PROFILE["buildings"][0]]
    info.append("<h1>%s building footprints within %d m of 311-hotspot TIGER roads</h1>"
                % (src_label, BUFFER_M))
    info.append('<p class="sub">%s County (FIPS %s) &middot; clipped to <i>%s</i> '
                '&middot; NAIP 2022 60&nbsp;cm base</p>'
                % (PROFILE["name"], PROFILE["fips"], county_layer_name))
    info.append("<h2>Rule applied</h2><table class='kv'>"
                "<tr><td>SF 311 cases, %d <span style='color:var(--mut)'>(%s)</span></td><td>%s</td></tr>"
                "<tr><td>snapped to nearest centreline &le;</td><td>%g m</td></tr>"
                "<tr><td>cases snapped</td><td>%s</td></tr>"
                "<tr><td>segments with &gt; %d cases</td><td>%s</td></tr>"
                "<tr><td>buffer</td><td>%g m</td></tr>"
                "<tr><td>buildings in buffer</td><td>%s</td></tr>"
                "<tr><td>after county clip</td><td>%s</td></tr>"
                "</table>" % (YEAR, os.path.basename(CASES_CSV), format(stats["n_cases"], ","), SNAP_TOL_M,
                              format(stats["n_assigned"], ","), CASE_THRESHOLD,
                              format(stats["n_hot"], ","), BUFFER_M,
                              format(stats["n_bldg"], ","), format(stats["n_bldg_clipped"], ",")))
    if PROFILE["fips"] == "06081":
        info.append('<div class="warn"><b>Coverage caveat.</b> These TIGER roads are county '
                    '<b>06081 San&nbsp;Mateo</b> (lat 37.108&ndash;37.709), while the 311 file is '
                    '<b>San&nbsp;Francisco</b> (lat 37.624&ndash;37.832). The two overlap only '
                    'along the Daly City county line and around SFO, so this map covers that '
                    'overlap and nothing more &mdash; it is not a county-wide result. Run with '
                    '<code>COUNTY=06075</code> to match the 311 data properly.</div>')
    else:
        info.append('<div class="warn"><b>Why the threshold is %s, not 20.</b> With SF\'s own '
                    'street network the 311 data is dense: <b>%s of %s cases</b> snap to a '
                    'centreline, and a &gt;20 rule selects %s segments whose %g m buffers cover '
                    '%.0f%% of the city\'s land &mdash; nearly every building matches, so the map '
                    'says nothing. Raising the bar to %s cases isolates the genuinely worst-affected '
                    'streets. Set <code>CASE_THRESHOLD</code> to change it.</div>'
                    % (format(CASE_THRESHOLD, ","), format(stats["n_assigned"], ","),
                       format(stats["n_cases"], ","), format(stats.get("n_seg_gt20", 0), ","),
                       BUFFER_M, stats.get("pct_land_gt20", 0), format(CASE_THRESHOLD, ",")))
        info.append('<div class="warn"><b>Building source.</b> Overture\'s GeoParquet stops at '
                    'lat 37.72 and misses 95%% of the SF cases, so footprints come from '
                    '<code>04_geojson/sf_buildings.geojson</code> (177,023 features). Outlines are '
                    'generalised by %g m for display only &mdash; all counts use full '
                    'resolution.</div>' % SIMPLIFY_M)
    if top:
        info.append("<h2>Top tracts</h2><table class='kv'>" + "".join(
            "<tr><td>%s <span style='color:var(--mut)'>%s</span></td><td>%d</td></tr>"
            % (r[0], r[4], r[5]) for r in top) + "</table>")
    if naip:
        info.append("<h2>Aerial imagery</h2><table class='kv'>"
                    "<tr><td>NAIP 2022, 60&nbsp;cm, warped to EPSG:3857</td><td>%d&times;%d px</td></tr>"
                    "</table><p class='sub' style='margin-top:4px'>tiles: %s<br>source: %s</p>"
                    % (naip["px"][0], naip["px"][1], ", ".join(naip["tiles"]), naip["source"]))
    for n in REPORT_NOTE:
        info.append("<div class='warn'>%s</div>" % n)
    info.append("<h2>Datasets opened</h2><ul class='ds'>" + "".join(
        "<li><span class='%s'>%s</span> <code>%s</code> %s</li>" % (
            "ok" if r["status"] == "ok" else "bad",
            "OK" if r["status"] == "ok" else "FAIL", r["id"], r["title"])
        for r in REPORT) + "</ul>")
    if fails:
        info.append("<h2>Could not open</h2>" + "".join(
            "<div class='warn'><b>%s</b> &mdash; %s</div>" % (r["id"], r["error"]) for r in fails))
    info.append("<p class='sub' style='margin-top:14px'>Generated %s by "
                "<code>run_analysis.py</code>. Metric work in EPSG:26910.</p>"
                % datetime.now().strftime("%Y-%m-%d %H:%M"))

    legend = ("<div><span class='sw' style='background:#ff8a3d'></span>Building (match)</div>"
              "<div><span class='sw' style='background:#ffd60a'></span>TIGER segment &gt; %d cases</div>"
              "<div><span class='sw' style='background:#4cc9f066'></span>%g m buffer</div>"
              "<div><span class='sw' style='background:#a78bfa66'></span>Tract, shaded by count</div>"
              "<div><span class='sw' style='background:#ff4d6d'></span>County boundary (clip)</div>"
              % (CASE_THRESHOLD, BUFFER_M))

    data = {"year": YEAR, "threshold": CASE_THRESHOLD, "county": county, "zone": zone,
            "bldg_label": "Matching %s footprints" % src_label,
            "county_label": "County boundary (%s)" % county_layer_name,
            "hot_roads": hot, "buildings": bl, "tracts": tr, "naip": naip}
    html = (HTML_TMPL
            .replace("__TITLE__", "Buildings near 311-hotspot roads")
            .replace("__INFO__", "".join(info))
            .replace("__LEGEND__", legend)
            .replace("__DATA__", json.dumps(data, sort_keys=True)))   # canonical: byte-identical regardless of dict build order
    open(dst, "w").write(html)
    log("wrote %s (%.1f MB)" % (os.path.basename(dst), os.path.getsize(dst)/1e6))
    return dst


def main():
    render_only = "--render-only" in sys.argv
    if render_only:
        c, stats, naip, county_layer = load_checkpoint()
    else:
        probe_all()
        county_gj, county_layer = fetch_county_boundary()
        c, stats = analyse(county_gj)
        naip = None
        if stats["n_hot"]:
            try:
                naip = naip_overlay(c, stats["zone_bbox_m"])
            except Exception as e:
                log("NAIP overlay FAILED: %s" % e)
                REPORT_NOTE.append("NAIP overlay failed: %s" % e)
                stats["naip_failed"] = str(e)[:200]
        save_checkpoint(c, stats, naip, county_layer)

    # -- everything below is cheap and reads only the checkpointed tables --
    if stats["n_hot"]:
        csv_path, tract_rows = write_tract_csv(c)
    else:
        log("no road segment exceeded the case threshold; skipping tract CSV")
        csv_path, tract_rows = None, []
    write_report()
    html = build_html(c, stats, naip, county_layer, tract_rows,
                      os.path.join(OUT, "buildings_near_311_hot_roads.html"))
    if stats["n_hot"]:
        for name, sql in [
            ("hot_road_segments", "select LINEARID, FULLNAME, MTFCC, n_cases, %s from hot order by LINEARID" % to4326()),
            ("buffer_200m",       "select 1 as i, %s from zone" % to4326()),
            ("matching_buildings","select id, height, num_floors, class, subtype, %s from bldg_clip order by id, ST_XMin(g), ST_YMin(g)" % to4326()),   # full precision on disk
        ]:
            json.dump(gj(c, sql), open(os.path.join(OUT, name + ".geojson"), "w"), sort_keys=True)
    log("=== done%s ===" % (" (render-only)" if render_only else ""))
    print(json.dumps({k: v for k, v in stats.items() if not k.startswith("zone_")}, indent=2))
    return html

if __name__ == "__main__":
    main()
