import json
import sys
from pathlib import Path

from tests import model

if __name__ == "__main__":
    args = sys.argv[1:]
    ref = "REFERENCE" if len(args) == 0 else args[0]
    here = Path(__file__).parent

    groups = []
    for manifest in model.list_manifests():
        group = {
            "name": manifest.group,
            "files": [f.to_dict(ref) for f in manifest.list_files()],
        }
        groups.append(group)

    obj = {"ref": ref, "groups": groups}
    with open(here / "manifest.json", "w") as f:
        json.dump(obj, f, indent=2)
