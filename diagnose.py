import os
import sys
from pathlib import Path

def test_everything():
    results = []
    
    # 1. Check directory
    results.append(f"CWD: {os.getcwd()}")
    
    # 2. Check files
    files = ["app.py", "dashboard.html", "config.yaml", "config.json"]
    for f in files:
        path = Path(f)
        results.append(f"File {f} exists: {path.exists()} (Size: {path.stat().st_size if path.exists() else 'N/A'})")
        
    # 3. Check imports
    packages = ["fastapi", "uvicorn", "hubspot", "yaml", "dateutil"]
    for pkg in packages:
        try:
            __import__(pkg)
            results.append(f"Import {pkg}: OK")
        except ImportError:
            results.append(f"Import {pkg}: FAILED")
            
    # 4. Write results
    with open("test_results.txt", "w") as f:
        f.write("\n".join(results))

if __name__ == "__main__":
    test_everything()



