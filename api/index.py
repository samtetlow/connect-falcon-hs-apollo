"""
Vercel Serverless Entry Point for FalconHub
This file serves as the entry point for Vercel's serverless functions.
"""

import sys
import os

# Add the parent directory to the path so we can import from the main app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the FastAPI app
from app import app

# Vercel expects the handler to be named 'app' or 'handler'
handler = app

