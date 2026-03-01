"""
Run script for Indian Stock Analyzer.
Usage: python run.py
"""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
