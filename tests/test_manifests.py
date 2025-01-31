from . import model


def test_files_exist():
    # Parses the names/paths of all the files
    all_files = model.list_files()

    # Ensure all declared files exist
    all_manifests = model.list_manifests()
    files_chk = {f.path.name for f in all_files}
    for manifest in all_manifests:
        for file in manifest.list_files():
            assert file.path.exists()
            files_chk.remove(file.path.name)

    # Check that everything was in a manifest somewhere
    assert len(files_chk) == 0
