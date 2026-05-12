import argparse
import json
from pathlib import Path

from fastapi.openapi.utils import get_openapi

from app.main import app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export FastAPI OpenAPI schema to a JSON file")
    parser.add_argument("--output", type=Path, required=True, help="Path to write OpenAPI schema JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Use get_openapi() directly instead of app.openapi() so the exported
    # schema includes operations tagged "internal" — they are stripped from
    # the public-facing /openapi.json but must remain available for the UI's
    # typed-client generation.
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=app.openapi_tags,
    )
    args.output.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
