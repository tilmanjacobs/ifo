#This script replaces german special characters to avoid issues later on.

import sys
from pathlib import Path
import geopandas as gpd


# translate() mapping must use code points (ord) → replacement strings
REPLACEMENTS = {
    ord("ä"): "ae",
    ord("ö"): "oe",
    ord("ü"): "ue",
    ord("Ä"): "AE",
    ord("Ö"): "OE",
    ord("Ü"): "UE",
    ord("ß"): "ss",
}


def replace_german_chars(value):
    if isinstance(value, str):
        return value.translate(REPLACEMENTS)
    return value


def process_shapefile(shp_path: Path) -> None:
    # Keep decoding simple and avoid forcing mismatched encodings
    try:
        # Default path (pyogrio engine if available)
        gdf = gpd.read_file(shp_path)
    except UnicodeDecodeError:
        # Retry using Fiona engine which sometimes handles DBF encodings differently
        try:
            gdf = gpd.read_file(shp_path, engine="fiona")
        except UnicodeDecodeError:
            # As a last resort, try Fiona with common single-byte encodings
            for enc in ("latin-1", "cp1252"):
                try:
                    gdf = gpd.read_file(shp_path, engine="fiona", encoding=enc)
                    break
                except UnicodeDecodeError:
                    gdf = None
                    continue
            if gdf is None:
                raise
    # Apply only to string/object columns
    for col in gdf.columns:
        if gdf[col].dtype == object:
            gdf[col] = gdf[col].apply(replace_german_chars)
    gdf.to_file(shp_path, driver="ESRI Shapefile", encoding="UTF-8")
    print(f"Fixed: {shp_path}")


def main() -> None:
    # If no argument is given, default to processing all .shp in ./data
    if len(sys.argv) == 1:
        target = Path("data")
        if not target.exists() or not target.is_dir():
            print("Default 'data' directory not found. Provide a path to a .shp or directory.")
            sys.exit(1)
    elif len(sys.argv) == 2:
        target = Path(sys.argv[1])
    else:
        print("Usage: python code/fix_special_chars.py [<path-to-shp-or-directory>]")
        sys.exit(1)

    
    if not target.exists():
        print(f"Not found: {target}")
        sys.exit(1)

    if target.is_dir():
        files = sorted(target.glob("*.shp"))
        if not files:
            print(f"No .shp files in {target}")
            sys.exit(1)
        print(f"Found {len(files)} shapefile(s).")
        for shp in files:
            print(f"Fixing: {shp}")
            process_shapefile(shp)
        print("Done.")
        sys.exit(0)

    # Single file path
    if target.suffix.lower() != ".shp":
        print("Please provide a .shp file or a directory containing .shp files.")
        sys.exit(1)
    print(f"Fixing: {target}")
    process_shapefile(target)
    print("Done.")
    sys.exit(0)


if __name__ == "__main__":
    main()

