"""
Vercel Serverless Entry Point for FalconHub
Uses Mangum to adapt FastAPI (ASGI) for serverless environments.
"""

import sys
import os

# Add the parent directory to the path so we can import from the main app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the FastAPI app
from app import app

# Use Mangum to wrap FastAPI for serverless (AWS Lambda / Vercel)
from mangum import Mangum

# Create the handler that Vercel will use
handler = Mangum(app, lifespan="off")

