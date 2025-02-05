import os
import re
from pathlib import Path

import yaml

_here = Path(__file__).parent

GROUPS = (
    "example",
    "example-crs",
    "microsoft-buildings",
    "natural-earth",
    "quadrangles",
    "ns-water",
)


class File:
    def __init__(self, path, file_location="release"):
        self.path = path
        self.group, self.name, self.suffix = parse_path(path)
        self.format = FORMATS[self.suffix]
        self.file_location = file_location

    def url(self, ref):
        if self.file_location == "repo":
            path = f"{ref}/{self.group}/files/{self.path.name}"
            return f"https://raw.githubusercontent.com/geoarrow/geoarrow-data/{path}"
        elif self.file_location == "release":
            path = f"releases/download/{ref}/{self.path.name}"
            return f"https://github.com/geoarrow/geoarrow-data/{path}"
        else:
            raise ValueError(f"Unknown file location: '{self.file_location}'")

    def to_dict(self, ref):
        return {"name": self.name, "format": self.format, "url": self.url(ref)}

    def __repr__(self):
        return f"File({self.path.name})"


class Manifest:
    def __init__(self, path):
        self.path = path
        with open(path) as f:
            self._obj = yaml.safe_load(f)
        self.group = self._obj["group"]
        self.file_location = (
            "release"
            if "file_location" not in self._obj
            else self._obj["file_location"]
        )
        self._formats = self._obj["format"] if "format" in self._obj else []

    def list_file_info(self):
        for file in self._obj["files"]:
            name = file["name"]
            skip_formats = file["skip_format"] if "skip_format" in file else []
            formats = file["format"] if "format" in file else self._formats
            for format in formats:
                if format in skip_formats:
                    continue
                if format not in SUFFIXES:
                    raise ValueError(
                        f"Unexpected format: '{format}' in manifest '{self.path}'"
                    )
                yield name, format

    def list_files(self):
        for name, format in self.list_file_info():
            yield File(path(self.group, name, format), self.file_location)


def path(group, name, format):
    return _here.parent / group / "files" / f"{group}_{name}{SUFFIXES[format]}"


def parse_filename(filename):
    maybe_match = re.match(r"^([a-z-]+)_([^_.]+)(.*)$", filename)
    if maybe_match is None:
        raise ValueError(f"'{filename}' is not a valid asset filename")

    group, name, suffix = maybe_match.groups()
    if suffix not in FORMATS:
        raise ValueError(f"'{filename}' does not contain a valid suffix")

    return group, name, suffix


def parse_path(path):
    group, name, suffix = parse_filename(path.name)

    if path.parent.name != "files":
        raise ValueError(
            f"Expected path parent to be 'files' but got '{path.parent.name}'"
        )

    if path.parent.parent.name != group:
        raise ValueError(
            f"Expected path parent's parent to be '{group}' but got"
            f" {path.parent.parent.name}"
        )

    return group, name, suffix


def list_paths(groups=GROUPS):
    out = []
    for group in groups:
        files_dir = _here.parent / group / "files"
        out.extend(
            files_dir / file
            for file in os.listdir(files_dir)
            if not file.startswith(".")
        )
    return out


def list_files(groups=GROUPS):
    return [File(path) for path in list_paths(groups)]


def list_manifests(groups=GROUPS):
    return [Manifest(_here.parent / group / "manifest.yaml") for group in groups]


SUFFIXES = {
    "tsv": ".tsv",
    "fgb/zip": ".fgb.zip",
    "fgb": ".fgb",
    "arrows": ".arrows",
    "arrows/interleaved": "_interleaved.arrows",
    "arrows/box": "_box.arrows",
    "arrows/wkb": "_wkb.arrows",
    "arrows/wkt": "_wkt.arrows",
    "geoparquet": ".parquet",
    "geoparquet/native": "_native.parquet",
}

FORMATS = {v: k for k, v in SUFFIXES.items()}
