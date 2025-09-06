import argparse
import re
from pathlib import Path

import geopandas as gpd
from pyogrio import read_dataframe as ogr_read_dataframe


def extract_year_from_text(
    text: object,
    year_regex: re.Pattern,
    prefer_last: bool,
    include_quelle: bool,
) -> int | None:
    """Return a four-digit year found in text or None if no year is present.

    If multiple years are present, return last/first depending on prefer_last.
    """
    if not isinstance(text, str):
        return None
    # Remove lines that start with "Quelle:" (source info), case-insensitive
    if not include_quelle:
        text = re.sub(r"(?mi)^\s*Quelle:.*$", "", text)
    matches = year_regex.findall(text)
    if not matches:
        return None
    value = matches[-1] if prefer_last else matches[0]
    try:
        return int(value)
    except ValueError:
        return None


def build_output_path(input_path: Path, output_path: Path | None) -> Path:
    if output_path is not None:
        return output_path
    if input_path.suffix.lower() == ".shp":
        return input_path.with_name(input_path.stem + "_with_year.shp")
    return input_path.parent / (input_path.name + "_with_year.shp")


def find_description_column(columns: list[str], preferred: str) -> str | None:
    # If preferred exists, use it
    if preferred in columns:
        return preferred
    lowered = [c.lower() for c in columns]
    # Heuristic candidates in priority order
    patterns = [
        r"^description$",
        r"^descript.*",
        r"^desc$",
        r"^descr$",
        r"^beschreibung$",
        r"beschr.*",
        r".*desc.*",
    ]
    for pat in patterns:
        for i, c in enumerate(lowered):
            if re.fullmatch(pat, c):
                return columns[i]
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract a sale year from a text field and write to a new shapefile field."
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        help="Path to input .shp file or directory. If omitted, processes all .shp in ./data/",
    )
    parser.add_argument(
        "--description-field",
        dest="description_field",
        default="description",
        help="Name of text field containing description (default: description)",
    )
    parser.add_argument(
        "--out",
        dest="output",
        type=Path,
        default=None,
        help="Path to output .shp (default: <input>_with_year.shp)",
    )
    parser.add_argument(
        "--year-field",
        dest="year_field",
        default="SALE_YEAR",
        help="Output field name (<=10 chars for shapefile; default: SALE_YEAR)",
    )
    parser.add_argument(
        "--prefer-last",
        dest="prefer_last",
        action="store_true",
        help="If multiple years found, take the last one (default)",
    )
    parser.add_argument(
        "--prefer-first",
        dest="prefer_first",
        action="store_true",
        help="If multiple years found, take the first one",
    )
    parser.add_argument(
        "--include-quelle",
        dest="include_quelle",
        action="store_true",
        help='Also consider years on lines starting with "Quelle:"',
    )
    parser.add_argument(
        "--encoding",
        dest="encoding",
        default=None,
        help="Force DBF/text encoding (e.g., cp1252, latin-1). Overrides .cpg.",
    )
    parser.add_argument(
        "--try-encodings",
        dest="try_encodings",
        default="cp1252,latin-1,iso-8859-1",
        help="Comma-separated fallback encodings to try on decode error",
    )
    args = parser.parse_args()

    # Resolve which shapefiles to process
    inputs: list[Path] = []
    if args.input is None:
        default_dir = Path("data")
        if default_dir.exists() and default_dir.is_dir():
            inputs = sorted(default_dir.glob("*.shp"))
            if not inputs:
                raise FileNotFoundError("No .shp files found in ./data. Provide an input path.")
            print(f"No input provided. Processing {len(inputs)} shapefile(s) in 'data/'.")
        else:
            raise FileNotFoundError("Default 'data' directory not found. Provide an input path.")
    else:
        input_path: Path = args.input
        if input_path.is_dir():
            inputs = sorted(input_path.glob("*.shp"))
            if not inputs:
                raise FileNotFoundError(f"No .shp files found in directory: {input_path}")
        else:
            if not input_path.exists():
                raise FileNotFoundError(f"Input file not found: {input_path}")
            inputs = [input_path]

    prefer_last: bool = True if not args.prefer_first else False
    year_field: str = args.year_field
    if len(year_field) > 10:
        raise ValueError("Shapefile field names must be <= 10 characters")

    year_regex = re.compile(r"\b(19\d{2}|20\d{2})\b")

    def read_with_encoding(path: Path) -> gpd.GeoDataFrame:
        # 1) Respect explicit --encoding
        if args.encoding:
            return ogr_read_dataframe(path, encoding=args.encoding)

        # 2) Try encoding from .cpg if present
        cpg_path = path.with_suffix(".cpg")
        if cpg_path.exists():
            try:
                cpg_text = cpg_path.read_text(errors="ignore").strip()
                if cpg_text:
                    return ogr_read_dataframe(path, encoding=cpg_text)
            except Exception:
                pass

        # 3) Default attempt without encoding (let driver decide)
        return gpd.read_file(path)

    for input_path in inputs:
        print(f"Reading: {input_path}")
        try:
            gdf = read_with_encoding(input_path)
        except UnicodeDecodeError as e:
            print(f"  Encoding error on {input_path.name}: {e}. Trying fallbacks...")
            tried = []
            for enc in [e.strip() for e in str(args.try_encodings).split(",") if e.strip()]:
                try:
                    gdf = ogr_read_dataframe(input_path, encoding=enc)
                    print(f"  Read OK with encoding: {enc}")
                    break
                except UnicodeDecodeError:
                    tried.append(enc)
                    continue
            else:
                raise

        desc_col = find_description_column(list(gdf.columns), args.description_field)
        if not desc_col:
            available = ", ".join(map(str, gdf.columns))
            raise KeyError(
                f"No suitable description field found in {input_path.name}. Tried '{args.description_field}'. Available: {available}"
            )
        if desc_col != args.description_field:
            print(f"  Using '{desc_col}' as description field (auto-detected).")

        gdf[year_field] = gdf[desc_col].apply(
            lambda value: extract_year_from_text(
                value,
                year_regex,
                prefer_last,
                bool(args.include_quelle),
            )
        )

        output_path = build_output_path(input_path, args.output)

        output_path.parent.mkdir(parents=True, exist_ok=True)

        gdf.to_file(output_path, driver="ESRI Shapefile")
        print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()


