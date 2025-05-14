import subprocess
import sys
from pathlib import Path

REPOSITORY_DIR_PATH = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_DIR_PATH / "hooks" / "shared"))

from gitrepository import GitRepository

DOCUMENTED_FILE_PATHS = [REPOSITORY_DIR_PATH / "sqldatabase"]
DOCUMENTATION_DIR_PATH = REPOSITORY_DIR_PATH / "docs"
DOCUMENTATION_FORMAT = "google"


def sort_imports(*files: str | Path) -> None:
    subprocess.run(["isort", *files], check=True)


def format_code(*files: str | Path) -> None:
    subprocess.run(["black", *files], check=True)


def generate_documentation(
    *files: str | Path,
    documentation_dir_path: str | Path,
    documentation_format: str | None = None
) -> None:
    args = ["pdoc", *files, "--output-dir", documentation_dir_path]
    if documentation_format is not None:
        args += ["--docformat", documentation_format]
    subprocess.run(
        args,
        check=True,
    )


def main():
    repository = GitRepository(REPOSITORY_DIR_PATH)
    to_format_staged_files = [
        file_path
        for file_path in repository.get_staged_files()
        if file_path.suffix == ".py" and file_path.is_file()
    ]
    if to_format_staged_files:
        sort_imports(*to_format_staged_files)
        format_code(*to_format_staged_files)
        repository.add(*to_format_staged_files)

    to_document_staged_files = repository.get_staged_files(*DOCUMENTED_FILE_PATHS)
    if to_document_staged_files:
        generate_documentation(
            *DOCUMENTED_FILE_PATHS,
            documentation_dir_path=DOCUMENTATION_DIR_PATH,
            documentation_format=DOCUMENTATION_FORMAT,
        )
        repository.add(DOCUMENTATION_DIR_PATH)


if __name__ == "__main__":
    main()
