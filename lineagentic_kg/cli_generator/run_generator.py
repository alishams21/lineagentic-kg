#!/usr/bin/env python3
"""
Script to run the CLI generator
"""

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lineagentic_kg.cli_generator.generator import CLIGenerator


def main():
    """Run the CLI generator"""
    # Get registry path from environment or use default
    registry_path = os.getenv("REGISTRY_PATH", "config/main_registry.yaml")
    
    # Get output directory from environment or use default
    output_dir = os.getenv("CLI_OUTPUT_DIR", "generated_cli")
    
    print(f"🔧 Registry path: {registry_path}")
    print(f"📁 Output directory: {output_dir}")
    
    # Create generator and generate files
    generator = CLIGenerator(registry_path, output_dir)
    generator.generate_all()
    
    print(f"\n🎉 CLI generation complete!")
    print(f"📂 Generated files are in: {output_dir}")
    print(f"\nTo use the generated CLI:")
    print(f"cd {output_dir}")
    print(f"pip install -r requirements.txt")
    print(f"python lineagentic_cli.py --help")


if __name__ == "__main__":
    main()
