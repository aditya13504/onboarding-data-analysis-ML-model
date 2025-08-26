#!/usr/bin/env python3
"""
Local development server for the Onboarding Drop-off Analyzer.
Use this for local testing before deploying to Vercel.
"""

import os
import sys
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Add src to Python path
current_dir = Path(__file__).parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

# Set local development environment
os.environ.setdefault('ENVIRONMENT', 'development')
# Supabase configuration for development
# Set these environment variables or create a .env file:
# SUPABASE_PROJECT_REF=your_project_ref
# SUPABASE_DB_PASSWORD=your_db_password
if not os.getenv('SUPABASE_PROJECT_REF') or not os.getenv('SUPABASE_DB_PASSWORD'):
    print("WARNING: Supabase configuration missing!")
    print("Please set SUPABASE_PROJECT_REF and SUPABASE_DB_PASSWORD environment variables")
    print("Example: SUPABASE_PROJECT_REF=abcdefghijklmnop SUPABASE_DB_PASSWORD=your_password python dev_server.py")

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Onboarding Drop-off Analyzer API")
    print("📊 Dashboard: http://localhost:8000/docs")
    print("🔧 Health Check: http://localhost:8000/health")
    print("📧 Email Digest: http://localhost:8000/api/v1/email-digest/status")
    print("Press Ctrl+C to stop\n")
    
    uvicorn.run(
        "src.api_endpoints:app", 
        host="0.0.0.0", 
        port=8000,
        reload=True,
        log_level="info"
    )
