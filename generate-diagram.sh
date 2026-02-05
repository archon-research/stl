#!/usr/bin/env python3
"""
Quick wrapper script to generate AWS architecture diagram from your infra/ directory
"""

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
INFRA_DIR = SCRIPT_DIR / "infra"
OUTPUT_FILE = SCRIPT_DIR / "architecture-diagram.md"

if __name__ == '__main__':
    if not INFRA_DIR.exists():
        print(f"Error: infra directory not found at {INFRA_DIR}")
        sys.exit(1)
    
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "terraform-diagram-generator.py"),
        str(INFRA_DIR),
        "--output", str(OUTPUT_FILE),
        "--format", "markdown",
        "--title", "STL Sentinel AWS Architecture"
    ]
    
    print(f"Generating architecture diagram from {INFRA_DIR}...")
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print(f"\n✓ Successfully generated: {OUTPUT_FILE}")
        print(f"Open the file to view the diagram")
    else:
        print("Error generating diagram")
        sys.exit(1)
