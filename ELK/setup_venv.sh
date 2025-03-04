#!/bin/bash
# Create and set up a virtual environment for the Elasticsearch project

# Create a virtual environment named 'es_env'
python3 -m venv es_env

# Activate the virtual environment
# For Linux/macOS:
source es_env/bin/activate
# For Windows (Command Prompt):
# es_env\Scripts\activate.bat
# For Windows (PowerShell):
# es_env\Scripts\Activate.ps1

# Install required packages
pip install elasticsearch pandas

# Verify installations
pip list

echo "Virtual environment 'es_env' is set up and activated."
echo "Use 'deactivate' command when you want to exit the virtual environment."