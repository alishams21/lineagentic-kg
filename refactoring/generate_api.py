#!/usr/bin/env python3
"""
Simple script to generate the API from RegistryFactory
"""

import os
import sys
from pathlib import Path

# Add api_generator to path
sys.path.append(os.path.join(os.path.dirname(__file__), "api_generator"))

from api_generator.generator import APIGenerator


def main():
    """Generate API files"""
    # Configuration
    registry_path = "config/main_registry.yaml"
    output_dir = "generated_api"
    
    print("�� Generating FastAPI from RegistryFactory...")
    print(f"📂 Registry: {registry_path}")
    print(f"📁 Output: {output_dir}")
    
    # Create generator
    generator = APIGenerator(registry_path, output_dir)
    
    # Generate all files
    generator.generate_all()
    
    print(f"\n✅ API generation complete!")
    print(f"�� Files generated in: {output_dir}")
    print(f"\n�� To run the API:")
    print(f"   cd {output_dir}")
    print(f"   pip install -r requirements.txt")
    print(f"   python main.py")
    print(f"\n🌐 API will be available at: http://localhost:8000/docs")


if __name__ == "__main__":
    main()

