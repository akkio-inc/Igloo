package engine

// The functions media analytics needs come from three DuckDB extensions,
// loaded at startup like the others, since DuckDB cannot read extension
// files once it is locked down:
//
//   - datasketches (community; MIT, around Apache DataSketches): HLL, CPC
//     and theta sketches that can be stored in tables and merged later, for
//     deduplicated reach across campaigns, platforms and days, and quantile
//     and frequent-item sketches.
//   - spatial (core; MIT, bundling GDAL, PROJ and GEOS, the last under
//     LGPL-2.1): geometries, points in polygons, distances.
//   - h3 (community; Apache 2.0): the H3 hexagonal grid.
//
// Spatial opens files two ways. ST_ReadSHP, ST_ReadOSM and their replacement
// scans use DuckDB's file system, so the sandbox confines them as it does
// read_parquet. ST_Read, ST_Read_Meta and COPY ... (FORMAT GDAL) go through
// GDAL, which hands only the top-level path to DuckDB: files a dataset names
// inside itself, such as a VRT file's sources, GDAL opens on its own, from
// the server's disk or the network. Igloo refuses those everywhere (see
// readsThroughGDAL in package pgwire). PROJ, behind ST_Transform, can also
// open a file a coordinate system names (+init=/path:key); it never returns
// the file's contents.
var mediaExtensions = []string{"spatial", "h3", "datasketches"}

// mediaStatements load the media extensions and settle spatial's axis order.
func mediaStatements() []string {
	stmts := make([]string, 0, len(mediaExtensions)+1)
	for _, ext := range mediaExtensions {
		stmts = append(stmts, "LOAD "+ext)
	}
	// Coordinates are (longitude, latitude) everywhere, as in
	// ST_Point(lon, lat). Without this, ST_Transform and ST_Distance_Sphere
	// take the order from the coordinate system, and warn on every call.
	// Extension settings are per session unless set globally.
	return append(stmts, "SET GLOBAL geometry_always_xy = true")
}
