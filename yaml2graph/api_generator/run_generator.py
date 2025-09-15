#!/usr/bin/env python3
"""
Script to run the API generator
"""

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lineagentic_kg.api_generator.generator import APIGenerator


def main():
    """Run the API generator"""
    # Get registry path from environment or use default
    registry_path = os.getenv("REGISTRY_PATH", "config/main_registry.yaml")
    
    # Get output directory from environment or use default
    output_dir = os.getenv("API_OUTPUT_DIR", "generated_api")
    
    print(f"🔧 Registry path: {registry_path}")
    print(f"📁 Output directory: {output_dir}")
    
    # Create generator and generate files
    generator = APIGenerator(registry_path, output_dir)
    generator.generate_all()
    
    print(f"\n🎉 API generation complete!")
    print(f"📂 Generated files are in: {output_dir}")
    print(f"📋 Config files copied to: {output_dir}/config/")
    print(f"\nTo run the generated API:")
    print(f"cd {output_dir}")
    print(f"pip install -r requirements.txt")
    print(f"python main.py")


if __name__ == "__main__":
    main()

