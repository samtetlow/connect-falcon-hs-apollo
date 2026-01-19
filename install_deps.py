import subprocess
import sys

def install_packages():
    packages = ["hubspot-api-client", "pyyaml", "python-dateutil"]
    for pkg in packages:
        print(f"Installing {pkg}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
            print(f"✅ {pkg} installed successfully")
        except Exception as e:
            print(f"❌ Failed to install {pkg}: {e}")

if __name__ == "__main__":
    install_packages()



