#!/bin/bash
# Azure App Service Startup Script for Streamlit

# 1. Ensure the internal port 8000 is used (Azure default)
# 2. Use 0.0.0.0 to allow the Azure load balancer to reach the container
python -m streamlit run app.py --server.port 8000 --server.address 0.0.0.0
