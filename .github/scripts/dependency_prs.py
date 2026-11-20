"""Resolve marked dependency PRs into an immutable nightly build manifest."""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from lib_github_api import (
    GitHubApi,
    GitHubApiError,
    GitHubApiTransportError,
    RoutingError,
    append_summary,
)

START_MARKER = "<!-- dependency-prs:start:v1 -->"
END_MARKER = "<!-- dependency-prs:end:v1 -->"
MAIN_REPOSITORY = "DUNE-DAQ/drunc"
PACKAGE_CONFIG = Path(__file__).resolve().parents[1] / "config" / "nightly-packages.json"
REPOSITORY_PART = r"[A-Za-z0-9][A-Za-z0-9_.-]*"
SHA_PATTERN = re.compile(r"^[0-9a-fA-F]{40,64}$")
TRANSIENT_STATUSES = {429, 500, 502, 503, 504}


class DependencyResolutionError(Exception):
    """An invalid dependency declaration or unresolvable PR selection."""


@dataclass(frozen=True)
class DependencyReference:
    """A normalized owner/repository/pull-number reference."""

    owner: str
    repository: str
    number: int

    @property
    def full_name(self) -> str:
        """Return the repository's owner/name identity."""
        return f"{self.owner}/{self.repository}"


def _repository_identity(value: object, context: str) -> str:
    if not isinstance(value, str) or value.count("/") != 1:
        raise DependencyResolutionError(f"{context} must be owner/repository")
    owner, repository = value.split("/", 1)
    if not re.fullmatch(REPOSITORY_PART, owner) or not re.fullmatch(
        REPOSITORY_PART, repository
    ):
        raise DependencyResolutionError(f"{context} must be owner/repository")
    return f"{owner}/{repository}"


def _configured_packages(registry: dict) -> dict[str, dict]:
    if registry.get("schema_version") != 1 or not isinstance(
        registry.get("packages"), list
    ):
        raise DependencyResolutionError("Package registry must use schema_version 1")

    packages: dict[str, dict] = {}
    for package in registry["packages"]:
        if not isinstance(package, dict):
            raise DependencyResolutionError("Package registry entries must be objects")
        repository = _repository_identity(
            package.get("repository"), "Package repository"
        )
        key = repository.casefold()
        if key in packages:
            raise DependencyResolutionError(
                f"Package registry contains duplicate repository {repository}"
            )
        packages[key] = package
    if MAIN_REPOSITORY.casefold() not in packages:
        raise DependencyResolutionError(
            f"Package registry is missing {MAIN_REPOSITORY}"
        )
    return packages


def _parse_url_reference(value: str) -> DependencyReference:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise ValueError("malformed GitHub URL") from error
    if (
        parsed.scheme.casefold() != "https"
        or parsed.hostname is None
        or parsed.hostname.casefold() != "github.com"
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
    ):
        raise ValueError("dependency URLs must use https://github.com")
    match = re.fullmatch(
        rf"/({REPOSITORY_PART})/({REPOSITORY_PART})/pull/([1-9][0-9]*)/?",
        parsed.path,
    )
    if not match:
        raise ValueError("URL must point to a pull request, not an issue or subpage")
    return DependencyReference(match[1], match[2], int(match[3]))


def _parse_reference(value: str) -> DependencyReference:
    link_match = re.fullmatch(r"\[([^\[\]]+)\]\(([^\s()]+)\)", value)
    if link_match:
        value = link_match[2]
    elif value.startswith("<") and value.endswith(">"):
        value = value[1:-1]

    if value.casefold().startswith("https://"):
        return _parse_url_reference(value)
    match = re.fullmatch(
        rf"({REPOSITORY_PART})/({REPOSITORY_PART})#([1-9][0-9]*)", value
    )
    if not match:
        raise ValueError("expected owner/repository#number or a GitHub pull URL")
    return DependencyReference(match[1], match[2], int(match[3]))


def parse_dependency_references(
    body: str | None, registry: dict
) -> list[DependencyReference]:
    """Parse and validate the versioned dependency block in a PR body."""
    configured = _configured_packages(registry)
    lines = (body or "").splitlines()
    starts = [index for index, line in enumerate(lines) if line.strip() == START_MARKER]
    ends = [index for index, line in enumerate(lines) if line.strip() == END_MARKER]

    if not starts and not ends:
        return []
    if len(starts) != 1 or len(ends) != 1:
        raise DependencyResolutionError(
            "Dependency section must contain exactly one start and one end marker"
        )
    if starts[0] >= ends[0]:
        raise DependencyResolutionError(
            "Dependency section start marker must precede its end marker"
        )

    references: list[DependencyReference] = []
    seen: set[str] = set()
    for raw_line in lines[starts[0] + 1 : ends[0]]:
        line = raw_line.strip()
        if not line:
            continue
        indentation = len(raw_line) - len(raw_line.lstrip(" \t"))
        if indentation >= 2 or raw_line.startswith("\t"):
            raise DependencyResolutionError(
                f"Invalid dependency line {raw_line!r}: nested bullets are not allowed"
            )
        bullet = re.fullmatch(r"[-*+]\s+(.+)", line)
        if not bullet:
            raise DependencyResolutionError(
                f"Invalid dependency line {raw_line!r}: use one reference per bullet"
            )
        try:
            reference = _parse_reference(bullet[1])
        except ValueError as error:
            raise DependencyResolutionError(
                f"Invalid dependency line {raw_line!r}: {error}"
            ) from error

        repository_key = reference.full_name.casefold()
        if repository_key == MAIN_REPOSITORY.casefold():
            raise DependencyResolutionError(
                f"Invalid dependency line {raw_line!r}: {MAIN_REPOSITORY} uses the workflow-selected revision"
            )
        if repository_key not in configured:
            raise DependencyResolutionError(
                f"Invalid dependency line {raw_line!r}: {reference.full_name} is not in the supported package registry"
            )
        if repository_key in seen:
            raise DependencyResolutionError(
                f"Invalid dependency line {raw_line!r}: only one PR per target repository is allowed"
            )
        seen.add(repository_key)
        references.append(reference)
    return references


def _request_with_retry(
    api: GitHubApi,
    owner: str,
    repository: str,
    pull_number: int,
    *,
    attempts: int,
    sleeper: Callable[[float], None],
) -> dict:
    endpoint = f"pulls/{pull_number}"
    for attempt in range(attempts):
        try:
            response = api.request_repository(owner, repository, "GET", endpoint)
            if not isinstance(response, dict):
                raise DependencyResolutionError(
                    f"GitHub returned invalid metadata for {owner}/{repository}#{pull_number}"
                )
            return response
        except GitHubApiError as error:
            if error.status not in TRANSIENT_STATUSES:
                raise DependencyResolutionError(
                    f"Could not resolve {owner}/{repository}#{pull_number}: {error}"
                ) from error
            if attempt + 1 == attempts:
                raise DependencyResolutionError(
                    f"Could not resolve {owner}/{repository}#{pull_number} "
                    f"after {attempts} attempts: {error}"
                ) from error
        except GitHubApiTransportError as error:
            if attempt + 1 == attempts:
                raise DependencyResolutionError(
                    f"Could not resolve {owner}/{repository}#{pull_number} "
                    f"after {attempts} attempts: {error}"
                ) from error
        except RoutingError as error:
            raise DependencyResolutionError(
                f"Could not resolve {owner}/{repository}#{pull_number}: {error}"
            ) from error
        sleeper(0.25 * (attempt + 1))
    raise DependencyResolutionError(
        f"Could not resolve {owner}/{repository}#{pull_number} after {attempts} attempts"
    )


def _resolved_pull(
    api: GitHubApi,
    reference: DependencyReference,
    *,
    attempts: int,
    sleeper: Callable[[float], None],
) -> dict:
    pull = _request_with_retry(
        api,
        reference.owner,
        reference.repository,
        reference.number,
        attempts=attempts,
        sleeper=sleeper,
    )
    pull_number = pull.get("number")
    if (
        isinstance(pull_number, bool)
        or not isinstance(pull_number, int)
        or pull_number != reference.number
    ):
        raise DependencyResolutionError(
            f"GitHub returned mismatched metadata for {reference.full_name}#{reference.number}"
        )
    if pull.get("state") != "open":
        raise DependencyResolutionError(
            f"Dependency PR {reference.full_name}#{reference.number} is not open"
        )
    base = pull.get("base")
    base_repository = base.get("repo") if isinstance(base, dict) else None
    base_name = (
        base_repository.get("full_name")
        if isinstance(base_repository, dict)
        else None
    )
    if not isinstance(base_name, str) or base_name.casefold() != reference.full_name.casefold():
        raise DependencyResolutionError(
            f"Dependency PR {reference.full_name}#{reference.number} targets a different base repository"
        )

    head = pull.get("head")
    if not isinstance(head, dict):
        raise DependencyResolutionError(
            f"Dependency PR {reference.full_name}#{reference.number} has no head metadata"
        )
    head_repository = head.get("repo")
    source_name = (
        head_repository.get("full_name")
        if isinstance(head_repository, dict)
        else None
    )
    branch = head.get("ref")
    sha = head.get("sha")
    try:
        source_name = _repository_identity(source_name, "Head repository")
    except DependencyResolutionError as error:
        raise DependencyResolutionError(
            f"Dependency PR {reference.full_name}#{reference.number} has no accessible head repository"
        ) from error
    if not isinstance(branch, str) or not branch:
        raise DependencyResolutionError(
            f"Dependency PR {reference.full_name}#{reference.number} has no head branch"
        )
    if not isinstance(sha, str) or not SHA_PATTERN.fullmatch(sha):
        raise DependencyResolutionError(
            f"Dependency PR {reference.full_name}#{reference.number} has an invalid head SHA"
        )
    return {"source_repository": source_name, "branch": branch, "sha": sha.lower()}


def resolve_manifest(
    api: GitHubApi | None,
    registry: dict,
    *,
    main_sha: str,
    main_pull_number: int | None = None,
    nightly_identifier: str = "",
    event_name: str = "",
    run_id: str = "",
    run_attempt: str = "",
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleeper: Callable[[float], None] = time.sleep,
    attempts: int = 3,
) -> dict:
    """Resolve the current PR body and return an immutable package manifest."""
    if not isinstance(main_sha, str) or not SHA_PATTERN.fullmatch(main_sha):
        raise DependencyResolutionError("Workflow-selected main revision must be a SHA")
    if attempts < 1:
        raise DependencyResolutionError("Retry attempts must be positive")
    configured = _configured_packages(registry)
    references: list[DependencyReference] = []
    dependency_section = "not_applicable"
    if main_pull_number is not None:
        if (
            isinstance(main_pull_number, bool)
            or not isinstance(main_pull_number, int)
            or main_pull_number <= 0
        ):
            raise DependencyResolutionError("Main PR number must be positive")
        if api is None:
            raise DependencyResolutionError("GitHub API client is required for PR resolution")
        main_pull = _request_with_retry(
            api,
            "DUNE-DAQ",
            "drunc",
            main_pull_number,
            attempts=attempts,
            sleeper=sleeper,
        )
        body = main_pull.get("body")
        if body is not None and not isinstance(body, str):
            raise DependencyResolutionError("Main PR body must be text or null")
        body = body or ""
        references = parse_dependency_references(body, registry)
        has_markers = any(
            line.strip() in {START_MARKER, END_MARKER} for line in body.splitlines()
        )
        dependency_section = (
            "markers_absent"
            if not has_markers
            else "declared"
            if references
            else "empty"
        )

    overrides: dict[str, dict] = {}
    for reference in references:
        overrides[reference.full_name.casefold()] = {
            "reference": reference,
            **_resolved_pull(api, reference, attempts=attempts, sleeper=sleeper),
        }

    packages: list[dict] = []
    for package in sorted(registry["packages"], key=lambda entry: entry.get("order", 0)):
        repository = package["repository"]
        key = repository.casefold()
        if key == MAIN_REPOSITORY.casefold():
            continue
        entry = {
            "target_repository": repository,
            "package": package["name"],
            "kind": package["kind"],
            "destination": package["destination"],
            "python_install": package["python_install"],
            "install_extras": package["install_extras"],
            "order": package["order"],
        }
        override = overrides.get(key)
        if override:
            reference = override["reference"]
            entry.update(
                {
                    "selection": "pull_request",
                    "pull_number": reference.number,
                    "source_repository": override["source_repository"],
                    "branch": override["branch"],
                    "sha": override["sha"],
                    "pull_url": f"https://github.com/{reference.full_name}/pull/{reference.number}",
                }
            )
        else:
            baseline = package.get("baseline")
            if (
                not isinstance(baseline, dict)
                or baseline.get("strategy") != "nightly_supplied"
            ):
                raise DependencyResolutionError(
                    f"Package {repository} must use a nightly_supplied baseline"
                )
            entry.update({"selection": "nightly_supplied", "baseline": baseline})
        packages.append(entry)

    main_package = configured[MAIN_REPOSITORY.casefold()]
    resolved_at = now().astimezone(timezone.utc).isoformat(timespec="seconds")
    resolved_at = resolved_at.replace("+00:00", "Z")
    manifest = {
        "schema_version": 1,
        "resolved_at": resolved_at,
        "run_id": str(run_id),
        "run_attempt": str(run_attempt),
        "main": {
            "repository": MAIN_REPOSITORY,
            "selection": "workflow_selected",
            "revision_kind": (
                "pull_request_merge"
                if event_name == "pull_request"
                else "push"
                if event_name == "push"
                else "workflow_selected"
            ),
            "sha": main_sha.lower(),
            "destination": main_package["destination"],
            "python_install": main_package["python_install"],
            "install_extras": main_package["install_extras"],
        },
        "nightly": {"identifier": nightly_identifier or None},
        "dependency_section": dependency_section,
        "packages": packages,
    }
    if main_pull_number is not None:
        manifest["main_pull_request"] = {
            "repository": MAIN_REPOSITORY,
            "number": main_pull_number,
        }
    return manifest


def _summary_cell(value: object) -> str:
    text = html.escape(str(value), quote=False)
    text = text.replace("\\", "\\\\")
    for character in "|`*_{}[]()#+-.!":
        text = text.replace(character, f"\\{character}")
    return text.replace("\r", " ").replace("\n", " ")


def render_summary(manifest: dict) -> str:
    """Render a Markdown record of the selected build inputs."""
    main = manifest["main"]
    main_sha = main["sha"]
    main_commit = f"https://github.com/{MAIN_REPOSITORY}/commit/{main_sha}"
    rows = [
        "| Package | Selection | Source | Revision / version |",
        "| --- | --- | --- | --- |",
        "| drunc | workflow-selected | DUNE-DAQ/drunc | "
        f"[{_summary_cell(main_sha)}]({main_commit}) |",
    ]
    for package in manifest["packages"]:
        package_name = _summary_cell(package["package"])
        if package["selection"] == "pull_request":
            pull_url = package["pull_url"]
            selection = (
                f"[PR #{package['pull_number']}]({pull_url})"
            )
            branch = _summary_cell(package["branch"])
            source = f"{_summary_cell(package['source_repository'])} ({branch})"
            sha = package["sha"]
            commit_url = f"https://github.com/{package['source_repository']}/commit/{sha}"
            revision = f"[{_summary_cell(sha)}]({commit_url})"
        else:
            selection = "nightly supplied"
            source = _summary_cell(package["baseline"].get("provenance", "nightly"))
            revision = "Version supplied by nightly; SHA not reported"
        rows.append(
            f"| {package_name} | {selection} | {source} | {revision} |"
        )

    metadata = [
        "### Dependency selection",
        "",
        f"Resolved: {_summary_cell(manifest['resolved_at'])} UTC",
        f"Run: {_summary_cell(manifest['run_id'])}, attempt {_summary_cell(manifest['run_attempt'])}",
    ]
    nightly_identifier = manifest.get("nightly", {}).get("identifier")
    if nightly_identifier:
        metadata.append(f"Nightly: {_summary_cell(nightly_identifier)}")
    metadata.append(
        f"Main revision: {_summary_cell(main.get('revision_kind', 'workflow_selected'))}"
    )
    section_status = manifest.get("dependency_section")
    if section_status == "markers_absent":
        metadata.append(
            "Dependency section: absent (legacy PR description); using nightly baselines."
        )
    elif section_status == "empty":
        metadata.append("Dependency section: empty; using nightly baselines.")
    elif section_status == "declared":
        metadata.append("Dependency section: dependency PRs declared.")
    elif section_status == "not_applicable":
        metadata.append("Dependency section: no PR context; using nightly baselines.")
    pull = manifest.get("main_pull_request")
    if pull:
        metadata.append(
            f"Main PR: [DUNE-DAQ/drunc#{pull['number']}](https://github.com/{MAIN_REPOSITORY}/pull/{pull['number']})"
        )
    return "\n".join([*metadata, "", *rows])


def _write_step_outputs(manifest: dict) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    lines: list[str] = []
    main = manifest["main"]
    main_values = {
        "destination": main["destination"],
        "python_install": str(main["python_install"]).lower(),
        "install_extras": ",".join(main["install_extras"]),
    }
    for key, value in main_values.items():
        value = str(value)
        if "\n" in value or "\r" in value:
            raise DependencyResolutionError(
                f"Main package has a multiline Actions output value: {key}"
            )
        lines.append(f"main_{key}={value}\n")
    for package in manifest["packages"]:
        name = package["package"]
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", name):
            raise DependencyResolutionError(
                f"Package name is not a valid GitHub Actions output key: {name!r}"
            )
        values = {
            "selection": package["selection"],
            "source_repository": package.get("source_repository", ""),
            "sha": package.get("sha", ""),
            "destination": package["destination"],
            "python_install": str(package["python_install"]).lower(),
            "install_extras": ",".join(package["install_extras"]),
        }
        for key, value in values.items():
            value = str(value)
            if "\n" in value or "\r" in value:
                raise DependencyResolutionError(
                    f"Package {name} has a multiline Actions output value"
                )
            lines.append(f"{name}_{key}={value}\n")
    with Path(output_path).open("a", encoding="utf-8") as output:
        output.writelines(lines)


def _pull_number_from_environment() -> int | None:
    value = os.environ.get("PR_NUMBER")
    if value:
        try:
            number = int(value)
        except ValueError as error:
            raise DependencyResolutionError("PR_NUMBER must be a positive integer") from error
        if number <= 0:
            raise DependencyResolutionError("PR_NUMBER must be a positive integer")
        return number
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return None
    try:
        event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DependencyResolutionError(
            f"Could not read GitHub event payload: {error}"
        ) from error
    pull = event.get("pull_request") if isinstance(event, dict) else None
    number = pull.get("number") if isinstance(pull, dict) else None
    if isinstance(number, int) and not isinstance(number, bool) and number > 0:
        return number
    return None


def main() -> int:
    """Resolve workflow metadata and write the requested manifest."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", default=PACKAGE_CONFIG, type=Path)
    arguments = parser.parse_args()
    try:
        registry = json.loads(arguments.config.read_text(encoding="utf-8"))
        if not isinstance(registry, dict):
            raise DependencyResolutionError("Package registry must be a JSON object")
        main_pull_number = _pull_number_from_environment()
        manifest = resolve_manifest(
            GitHubApi.from_environment() if main_pull_number is not None else None,
            registry,
            main_sha=os.environ.get("GITHUB_SHA", ""),
            main_pull_number=main_pull_number,
            nightly_identifier=os.environ.get("NIGHTLY_IDENTIFIER", ""),
            event_name=os.environ.get("GITHUB_EVENT_NAME", ""),
            run_id=os.environ.get("GITHUB_RUN_ID", ""),
            run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        )
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        _write_step_outputs(manifest)
        append_summary(render_summary(manifest))
    except (OSError, json.JSONDecodeError, RoutingError, DependencyResolutionError) as error:
        try:
            append_summary(
                "### Dependency resolution failed\n\n" + _summary_cell(error)
            )
        except OSError:
            pass
        print("Dependency resolution failed; see the Actions summary.", file=sys.stderr)
        return 1
    print(f"Dependency manifest written to {arguments.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())