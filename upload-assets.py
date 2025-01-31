import json
import subprocess
import sys
import urllib.request
from pathlib import Path

from tests import model


def upload_assets(tag):
    here = Path(__file__).parent

    for manifest in model.list_manifests():
        for file in manifest.list_files():
            if file.file_location != "release":
                continue

            path = file.path.relative_to(here)
            cmd = ["gh", "release", "upload", ref, path]
            print(cmd)
            subprocess.run(cmd)


def check_urls():
    manifest = Path(__file__).parent / "manifest.json"

    with open(manifest) as f:
        obj = json.load(f)

    for group in obj["groups"]:
        for file in group["files"]:
            url = file["url"]
            print(f"Checking '{url}'")
            with urllib.request.urlopen(url) as f:
                assert f.getcode() < 400
                f.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        raise ValueError("Must pass tag as first arg")

    ref = args[0]
    # upload_assets(ref)
    check_urls()
