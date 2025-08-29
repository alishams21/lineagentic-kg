#!/usr/bin/env python3
"""
Entry point script for CLI generation
"""

import os
import sys
from pathlib import Path

# Add the package root to the path for imports
package_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(package_root))

from lineagentic_kg.cli_generator.generator import CLIGenerator


def main():
    """Generate CLI files"""
    # Configuration
    registry_path = "lineagentic_kg/config/main_registry.yaml"
    output_dir = "generated_cli"
    
    print("🚀 Generating CLI from RegistryFactory...")
    print(f"📂 Registry: {registry_path}")
    print(f"📁 Output: {output_dir}")
    
    # Create generator
    generator = CLIGenerator(registry_path, output_dir)
    
    # Generate all files
    generator.generate_all()
    
    print(f"\n✅ CLI generation complete!")
    print(f"📁 Files generated in: {output_dir}")
    print(f"\n🚀 To run the CLI:")
    print(f"   cd {output_dir}")
    print(f"   pip install -r requirements.txt")
    print(f"   python main.py --help")


if __name__ == "__main__":
    main()
