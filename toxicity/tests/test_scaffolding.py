from lib import config


def test_paths_exist_or_are_creatable():
    config.ensure_dirs()
    assert config.DATA_DIR.is_dir()
    assert config.REFERENCE_DIR.is_dir()


def test_notebook_has_one_cell_per_phase():
    import json
    nb_path = config.TOX_ROOT / "lung_cancer_ppl_toxicity.ipynb"
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    markdowns = [
        "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        for c in nb["cells"] if c["cell_type"] == "markdown"
    ]
    for tag in ("Phase 1", "Phase 2", "Phase 3", "Phase 4", "Phase 5", "Phase 6"):
        assert any(tag in m for m in markdowns), f"missing {tag}"


def test_notebook_has_colab_setup_cell():
    import json
    nb_path = config.TOX_ROOT / "lung_cancer_ppl_toxicity.ipynb"
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    code_sources = [
        "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        for c in nb["cells"] if c["cell_type"] == "code"
    ]
    # The first code cell should set up Colab detection + git clone
    first = code_sources[0]
    assert "google.colab" in first
    assert "git clone" in first
    assert "REPO_URL" in first
