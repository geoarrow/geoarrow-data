import subprocess
import sys
from pathlib import Path

from tests import model

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        raise ValueError("Must pass tag as first arg")

    ref = args[0]
    here = Path(__file__).parent

    groups = []
    for manifest in model.list_manifests():
        for file in manifest.list_files():
            if file.file_location != "release":
                continue

            path = file.path.relative_to(here)
            cmd = ["gh", "release", "upload", ref, path]
            print(cmd)
            subprocess.run(cmd)
