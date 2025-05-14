import subprocess
from pathlib import Path


class GitRepository:
    def __init__(self, path: str | Path = "."):
        self.path = Path(path).resolve()

    def run_git_command(self, command: list[str]) -> str:
        command = ["git"] + command
        print(f"Running git command: {' '.join(command)}")
        process = subprocess.run(
            command,
            cwd=self.path,
            text=True,
            capture_output=True,
            check=False,
        )
        if process.returncode != 0:
            raise GitError(
                f"Command '{' '.join(command)}' failed: {process.stderr.strip()}."
            )
        return process.stdout.strip()

    def _get_files(self, command: list[str], *paths: str | Path) -> list[Path]:
        command = command + [str(path) for path in paths]
        stdout = self.run_git_command(command)
        return [self.path / file for file in stdout.splitlines()]

    def get_staged_files(self, *paths: str | Path) -> list[Path]:
        return self._get_files(["diff", "--cached", "--name-only"], *paths)

    def get_unstaged_files(self, *paths: str | Path) -> list[Path]:
        return self._get_files(["diff", "--name-only"], *paths)

    def get_changed_files(self, *paths: str | Path) -> list[Path]:
        return self._get_files(["diff", "HEAD", "--name-only"], *paths)

    def get_tracked_files(self, *paths: str | Path) -> list[Path]:
        return self._get_files(["ls-files"], *paths)

    def get_untracked_files(self, *paths: str | Path) -> list[Path]:
        return self._get_files(["ls-files", "--others", "--exclude-standard"], *paths)

    def add(self, *files: str | Path) -> str:
        return self.run_git_command(["add"] + list(map(str, files)))

    def commit(self, message: str) -> str:
        return self.run_git_command(["commit", "-m", message])


class GitError(Exception):
    pass


if __name__ == "__main__":
    repository = GitRepository()
    print(repository.run_git_command(["status"]))
    for file in repository.get_untracked_files(
        repository.path / "sqldatabase",
        repository.path / "shared",
        repository.path / "requirements.txt",
    ):
        print(file)
