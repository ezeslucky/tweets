#!/usr/bin/env python
"""Build and bundle the production Android App Bundle (.aab) for Google Play Store upload.

Usage:
  python scripts/build_android_bundle.py [--sign-keystore path/to/keystore.jks --alias my-alias]
"""
import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MOBILE_DIR = ROOT / "mobile"
ANDROID_DIR = MOBILE_DIR / "android"
WWW_DIR = MOBILE_DIR / "www"
ASSETS_DIR = ANDROID_DIR / "app" / "src" / "main" / "assets" / "public"

def sync_assets():
    print("--> 1. Syncing www web assets into Android project...")
    if not WWW_DIR.exists():
        raise FileNotFoundError(f"Source www directory not found at {WWW_DIR}")
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    # Clear old public assets
    for item in ASSETS_DIR.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    # Copy fresh
    for item in WWW_DIR.iterdir():
        if item.is_dir():
            shutil.copytree(item, ASSETS_DIR / item.name)
        else:
            shutil.copy2(item, ASSETS_DIR / item.name)
    print("    Synced all web assets successfully.")

def resolve_java_home() -> str:
    candidates = [
        os.getenv("JAVA_HOME"),
        r"C:\Program Files\Android\openjdk\jdk-21.0.8",
        r"C:\Program Files\Android\jdk\jdk-8.0.302.8-hotspot",
        r"C:\Program Files\Java\jdk-23",
        r"C:\Program Files\Android\Android Studio\jbr",
    ]
    for c in candidates:
        if c and os.path.isdir(c) and os.path.exists(os.path.join(c, "bin", "java.exe")):
            return c
    raise EnvironmentError("JDK with bin/java.exe not found. Please install JDK 17 or JDK 21.")

def build_bundle():
    java_home = resolve_java_home()
    print(f"--> 2. Building Android App Bundle (.aab) using JDK at {java_home}...")

    env = os.environ.copy()
    env["JAVA_HOME"] = java_home
    env["PATH"] = f"{java_home}\\bin;{env.get('PATH', '')}"

    gradlew = ANDROID_DIR / "gradlew.bat"
    if not gradlew.exists():
        raise FileNotFoundError(f"gradlew.bat not found at {gradlew}")

    cmd = [str(gradlew), "bundleRelease"]
    res = subprocess.run(cmd, cwd=str(ANDROID_DIR), env=env, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Gradle bundleRelease failed with exit code {res.returncode}")

    # Locate output .aab
    bundle_path = ANDROID_DIR / "app" / "build" / "outputs" / "bundle" / "release" / "app-release.aab"
    if not bundle_path.exists():
        candidates = list((ANDROID_DIR / "app" / "build" / "outputs" / "bundle").glob("**/*.aab"))
        if candidates:
            bundle_path = candidates[0]
        else:
            raise FileNotFoundError("bundleRelease completed but .aab file was not found")

    dest_aab = MOBILE_DIR / "Tweets-Companion.aab"
    shutil.copy2(bundle_path, dest_aab)
    size_mb = dest_aab.stat().st_size / (1024 * 1024)
    print(f"--> 3. Bundle successfully generated!")
    print(f"    Path: {dest_aab}")
    print(f"    Size: {size_mb:.2f} MB")
    return dest_aab

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keystore", help="Path to release keystore (.jks or .keystore)")
    parser.add_argument("--alias", help="Keystore alias name")
    args = parser.parse_args()

    sync_assets()
    aab = build_bundle()
    print("\nNext step: Upload mobile/Tweets-Companion.aab to Google Play Console!")

if __name__ == "__main__":
    main()
