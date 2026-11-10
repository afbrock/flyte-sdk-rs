"""Warnings `rust_task` gives about a Dockerfile before a reusable pool is built from it."""

from pathlib import Path

import pytest

from flyteplugins_rs._image import _dockerfile_image

LINK = "RUN ln -sf /usr/local/bin/w /usr/local/bin/unionai-actor-bridge\n"


@pytest.mark.parametrize(
    ("body", "warns"),
    [
        ("FROM x\n", "never mentions unionai-actor-bridge"),
        (LINK + "RUN pip install union==0.1.200\n", "installs the union Python package"),
        (
            LINK + 'RUN uv pip install "flyteplugins-union[all]"\n',
            "installs the union Python package",
        ),
        (LINK + "RUN cargo add union-reuse\n", None),
        (LINK, None),
    ],
)
def test_reuse_dockerfile_warnings(tmp_path: Path, capsys, body: str, warns: str | None):
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(body)
    _dockerfile_image(
        dockerfile=dockerfile, binary="w", registry="r", workspace=None, image_name=None, reuse=True
    )
    err = capsys.readouterr().err
    if warns is None:
        assert err == ""
    else:
        assert warns in err
