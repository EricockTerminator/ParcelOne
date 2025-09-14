import argparse
from pathlib import Path
from converters import convert_pages_with_gdal


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert pages using GDAL")
    parser.add_argument("pages", nargs="+", help="Input files to convert")
    parser.add_argument("--driver", default="GPKG", help="GDAL driver name")
    parser.add_argument("--out-dir", default="outputs", help="Directory for outputs")
    args = parser.parse_args()

    pages = [Path(p).read_bytes() for p in args.pages]
    out_paths = convert_pages_with_gdal(pages, args.driver, args.out_dir)
    for p in out_paths:
        print(p)

if __name__ == "__main__":
    main()
