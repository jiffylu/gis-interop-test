GIS interoperability test set, San Mateo County / SF. Bbox (WGS84): -122.55,37.10,-122.08,37.72
01_shapefile   TIGER roads (06081) and CA tracts. NAD83.
02_fgdb        USGS NHD hydrography, HUC8 18050006. FGDB.
03_arcgis_rest services.json lists live FeatureServer URLs on the county Hub. Query them; do not assume downloads.
04_geojson     SF building footprints.
05_gpkg        Natural Earth, many layers in one file.
06_kml         CAL FIRE historical fire perimeters, KMZ.
07_geoparquet  Overture buildings for the bbox.
08_geotiff     STAC hrefs for NAIP imagery and 3DEP elevation. COGs; read windows, do not download whole tiles.
09_csv         SF 311 cases since 2025. Lat/long columns, no CRS declared.
10_wfs         USGS MRDS WFS (mineral sites, points, filter by bbox) and USGS Topo WMS. Live OGC endpoints.
11 PostGIS     localhost:5432, user postgres, password x, table tiger_roads (if loaded).
