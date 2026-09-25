"""`core.storage` pins the repository root by counting parents of its own file.

That number is the one thing a move of the module silently breaks: the
default ``data/`` directory would land somewhere inside ``src/`` and every
local run would write its caches there. This is the test that fails instead.
"""

from hbu_dataplatform.core.storage import PROJECT_ROOT


def test_project_root_is_the_repository():
    assert (PROJECT_ROOT / "pyproject.toml").is_file()
    assert (PROJECT_ROOT / "src" / "hbu_dataplatform" / "definitions.py").is_file()
