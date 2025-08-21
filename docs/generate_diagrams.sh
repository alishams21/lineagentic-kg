#!/bin/bash

# Generate diagrams from individual Mermaid files
echo "Generating diagrams from individual Mermaid files..."

# Create images directory if it doesn't exist
mkdir -p images

# Generate each diagram from its individual file with high quality settings
echo "Generating Bootstrap Phase diagram..."
mmdc -i diagrams/01_bootstrap_phase.mmd -o images/01_bootstrap_phase.png -b white -s 2.0 -w 1600

echo "Generating Runtime Phase diagram..."
mmdc -i diagrams/02_runtime_phase.mmd -o images/02_runtime_phase.png -b white -s 2.0 -w 1600

echo "Generating Configuration Loading diagram..."
mmdc -i diagrams/03_config_loading.mmd -o images/03_config_loading.png -b white -s 2.0 -w 1600

echo "Generating Method Generation diagram..."
mmdc -i diagrams/04_method_generation.mmd -o images/04_method_generation.png -b white -s 2.0 -w 1600

echo "Generating System Architecture diagram..."
mmdc -i diagrams/05_system_architecture.mmd -o images/05_system_architecture.png -b white -s 2.0 -w 1600

echo "Generating Data Flow Overview diagram..."
mmdc -i diagrams/06_data_flow.mmd -o images/06_data_flow.png -b white -s 2.0 -w 1600

echo "All diagrams generated successfully!"
echo "Images saved in: images/"
ls -la images/
