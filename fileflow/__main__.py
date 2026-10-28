import argparse
from pathlib import Path
from .generator import generate
from .pipeline import run_pipeline

def main():
    parser = argparse.ArgumentParser(description="FileFlow reliable file ingestion pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="generate reproducible synthetic CSV data")
    gen.add_argument("--rows", type=int, default=100)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--out", default="data/incoming")
    run = sub.add_parser("run", help="process incoming CSV files")
    run.add_argument("--input", default="data/incoming")
    run.add_argument("--root", default="data")
    args = parser.parse_args()
    if args.command == "generate":
        paths = generate(args.rows, args.seed, args.out)
        print(f"Generated {len(paths)} file(s) in {args.out}")
    else:
        result = run_pipeline(Path(args.input), Path(args.root))
        print(result)

if __name__ == "__main__":
    main()
