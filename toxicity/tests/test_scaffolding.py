from lib import config

def test_paths_exist_or_are_creatable():
    config.ensure_dirs()
    assert config.DATA_DIR.is_dir()
    assert config.REFERENCE_DIR.is_dir()
