#!/usr/bin/env python3
"""
CLI Generator Script
This script generates a complete CLI application from the RegistryFactory configuration
"""

import os
import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from cli_generator.generator import CLIGenerator

def main():
    """Main function to generate CLI"""
    
    # Set paths
    registry_path = "config/main_registry.yaml"
    output_dir = "generated_cli"
    
    print(f"🔧 Registry path: {registry_path}")
    print(f"📁 Output directory: {output_dir}")
    
    # Create CLI generator
    generator = CLIGenerator(registry_path, output_dir)
    
    # Generate all CLI files
    print("🔧 Generating CLI commands from RegistryFactory...")
    generator.generate_all()
    
    print(f"\n🎉 CLI generation complete!")
    print(f"📂 Generated files are in: {output_dir}")
    print()
    print("To use the generated CLI:")
    print(f"cd {output_dir}")
    print("pip install -r requirements.txt")
    print("python cli.py --help")
    print()
    print("Or install as package:")
    print("pip install -e .")
    print("registry-cli --help")

if __name__ == "__main__":
    main()
