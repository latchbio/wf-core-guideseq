"""latch.__init__

Some biocompute.

(c) 2021 by latch.ai.
"""

import subprocess
from pathlib import Path

from flytekit import task, workflow
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


def _fmt_dir(bucket_path: str) -> str:
    if bucket_path[-1] == "/":
        return bucket_path[:-1]
    return bucket_path


def _remote_constructor(output_dir: FlyteDirectory, local_path: Path) -> str:
    """Constructs remote output path for a local file."""
    return _fmt_dir(output_dir.remote_source) + "/" + local_path.name


@task
def tsk(input_file: FlyteFile, output_dir: FlyteDirectory) -> FlyteFile:
    """ """

    local_file = Path(input_file).resolve()

    sample_cmd = [
        "cat",
        str(local_file),
    ]
    subprocess.run(sample_cmd)

    return FlyteFile(
        str(local_file),
        remote_path=_remote_constructor(output_dir, local_file),
    )


@workflow
def wf(sample_input: FlyteFile, output_dir: FlyteDirectory):
    """Description...

    Sample Markdown
    ----

    ## Foobar


    __metadata__:
        display_name: Your workflow
        author:
            name: n/a
            email:
            github:
        repository:
        license:
            id:

    Args:

        sample_param:
          A description

          __metadata__:
            display_name: Sample Param
    """

    tsk(sample_input, output_dir)
