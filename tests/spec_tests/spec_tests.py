"""
Automatically generated pytest suite for WDL spec conformance tests. Parses examples from the WDL
SPEC.md, runs them using miniwdl, and checks outputs against expected results. config.yaml lists
cases that are skip or xfail for various reasons.

The SPEC.md for each supported WDL version is sourced from openwdl/wdl git submodules found under
miniwidl/spec/. When a new patch version is released, the respective submodule revision should be
updated appropriately; then this suite will use that revision at test time.
"""

import re
import json
import subprocess
import sys
import pytest
from pathlib import Path
import os
from ruamel.yaml import YAML

# base directory for spec versions
SPEC_BASE = Path(__file__).parent.parent.parent / "spec"
# list of spec subdirectories to test
VERSIONS = ["wdl-1.1", "wdl-1.2"]

# load conformance config (xfail and skip lists)
CONFIG_FILE = Path(__file__).parent / "config.yaml"
cfg = YAML(typ="safe").load(CONFIG_FILE.read_text(encoding="utf-8")) or {}
# version-specific xfail and skip lists
VERSION_XFAIL = {ver: set(data.get("xfail", [])) for ver, data in cfg.items()}
VERSION_SKIP = {ver: set(data.get("skip", [])) for ver, data in cfg.items()}


def parse_spec_resources(text):
    """
    Parse Example Data appendix resources embedded in newer SPEC.md revisions.
    """
    resources = {}
    for name, _, body in re.findall(
        r"<summary>\s*Resource:\s*(.+?)\s*```(\w+)\n(.*?)\n```\s*</summary>",
        text,
        re.DOTALL,
    ):
        resources[name.strip()] = body + "\n"
    return resources


def parse_spec_outputs(yaml, output_text):
    """
    Parse an example output block from the spec, tolerating a missing comma between entries.
    """
    try:
        return yaml.load(output_text), None
    except Exception as original:
        repaired = re.sub(r'(["}\]])\n(\s*")', r"\1,\n\2", output_text)
        if repaired != output_text:
            try:
                return yaml.load(repaired), None
            except Exception:
                pass
        return {}, str(original)


def parse_spec_for(version):
    """
    Parse SPEC.md and data for a given spec version, yield test cases.
    Each case dict includes 'version', 'name', 'wdl', 'inputs', 'outputs', 'config'.
    """
    spec_dir = SPEC_BASE / version
    spec_md = spec_dir / "SPEC.md"
    text = spec_md.read_text(encoding="utf-8")
    yaml = YAML(typ="safe")
    resources = parse_spec_resources(text)
    blocks = re.findall(r"<details>(.*?)</details>", text, re.DOTALL)
    for block in blocks:
        m = re.search(r"<summary>\s*Example:\s*(.+?)\s*```wdl(.*?)```", block, re.DOTALL)
        if not m:
            continue
        name = m.group(1).strip()
        try:
            wdl_code = m.group(2).strip()
            m_in = re.search(r"Example input:\s*```json(.*?)```", block, re.DOTALL)
            inputs = yaml.load(m_in.group(1)) if m_in else {}
            m_out = re.search(r"Example output:\s*```json(.*?)```", block, re.DOTALL)
            outputs, output_parse_error = (
                parse_spec_outputs(yaml, m_out.group(1)) if m_out else ({}, None)
            )
            m_conf = re.search(r"Test config:\s*```json(.*?)```", block, re.DOTALL)
            config = yaml.load(m_conf.group(1)) if m_conf else {}
            if "type" not in config:
                config["type"] = "task" if name.endswith("_task") else "workflow"
            if "fail" not in config:
                config["fail"] = name.endswith("_fail") or name.endswith("_fail_task")
        except Exception as e:
            raise ValueError(
                f"Error parsing example '{name}' in spec version '{version}': {e}"
            ) from e
        yield {
            "version": version,
            "name": name,
            "wdl": wdl_code,
            "inputs": inputs,
            "outputs": outputs,
            "output_parse_error": output_parse_error,
            "config": config,
            "resources": resources,
        }


CASES = [case for v in VERSIONS for case in parse_spec_for(v)]
CASE_NAMES_BY_VERSION = {version: set() for version in VERSIONS}
for case in CASES:
    CASE_NAMES_BY_VERSION[case["version"]].add(case["name"])


def validate_config_cases():
    """
    Check config.yaml doesn't silently reference nonexistent spec examples.
    """
    errors = []
    for version in cfg:
        if version not in CASE_NAMES_BY_VERSION:
            errors.append(f"{version}: unknown spec version")
            continue
        for section, names in (("xfail", VERSION_XFAIL[version]), ("skip", VERSION_SKIP[version])):
            missing = sorted(names - CASE_NAMES_BY_VERSION[version])
            if missing:
                errors.append(f"{version} {section}: {', '.join(missing)}")
    if errors:
        raise ValueError(
            "tests/spec_tests/config.yaml references nonexistent spec tests:\n" + "\n".join(errors)
        )


validate_config_cases()


@pytest.mark.parametrize("case", CASES, ids=[f"{c['version']}-{c['name']}" for c in CASES])
def test_spec_conformance(tmp_path, case, monkeypatch):
    # run everything in tmp_path
    monkeypatch.chdir(tmp_path)

    name = case["name"]
    assert name.endswith(".wdl")
    # version-specific skips
    skip_list = VERSION_SKIP.get(case["version"], set())
    if name in skip_list:
        pytest.skip(f"Skipped by config.yaml for {case['version']}: {name}")
    inputs = case["inputs"]
    outputs = case["outputs"]
    config = case["config"]

    # version-specific expected failures
    xfail_list = VERSION_XFAIL.get(case["version"], set())
    is_xfail = False
    if (
        name in xfail_list
        or config.get("fail", False)
        or name.endswith("_fail.wdl")
        or name.endswith("_fail_task.wdl")
    ):
        is_xfail = True
    # extract spec test-data resources for this test
    for path, contents in case["resources"].items():
        for resource_path in (
            tmp_path / "data" / path,
            tmp_path / case["version"] / "data" / path,
        ):
            resource_path.parent.mkdir(parents=True, exist_ok=True)
            resource_path.write_text(contents, encoding="utf-8")

    # dump all WDL example files into version-specific subdirectories so imports resolve
    for other in CASES:
        version_dir = tmp_path / other["version"]
        version_dir.mkdir(parents=True, exist_ok=True)
        path = version_dir / other["name"]
        assert not path.exists()
        path.write_text(other["wdl"], encoding="utf-8")

    # write WDL file and input JSON
    wdl_file = tmp_path / case["version"] / name
    with open(wdl_file, "r") as f:
        line = "#"
        while line.startswith("#"):
            line = next(f).strip()
        assert line[8:] == case["version"][4:]
    inp_file = tmp_path / "input.json"
    inp_file.write_text(json.dumps(inputs), encoding="utf-8")
    out_file = tmp_path / "output.json"

    # construct command
    cmd = [
        sys.executable,
        "-m",
        "WDL",
        "run",
        str(wdl_file),
        "-i",
        str(inp_file),
        "-o",
        str(out_file),
        "--verbose",
    ]
    if config.get("type") == "task":
        # specify task name without .wdl suffix
        task_name = case["name"]
        if task_name.endswith("_task"):
            task_name = task_name[: -len("_task")]
        cmd.extend(["--task", task_name])

    # run
    cmd_env = os.environ.copy()
    cmd_env["PYTHONPATH"] = SPEC_BASE.parent.as_posix() + ":" + cmd_env.get("PYTHONPATH", "")

    try:
        result = subprocess.run(cmd, env=cmd_env, capture_output=True)
        if result.returncode != 0:
            print(f"Command failed: {' '.join(cmd)}")
            print(f"stdout:\n{result.stdout.decode('utf-8')}\n--")
            print(f"stderr:\n{result.stderr.decode('utf-8')}")
            assert False, f"miniwdl exit code {result.returncode} dir={tmp_path}"

        # verify outputs
        if case["output_parse_error"]:
            raise ValueError(
                f"Error parsing expected output for {name}: {case['output_parse_error']}"
            )
        got = json.loads(out_file.read_text(encoding="utf-8"))["outputs"]
        exclude = set(config.get("exclude_outputs", []))
        for k, v in outputs.items():
            if k in exclude or k.rsplit(".", 1)[-1] in exclude:
                continue
            got_v = _basenameize(got.get(k))
            assert got_v == v, (
                f"Mismatch for '{k}' in {name}: expected {v}, got {got.get(k)}; dir={tmp_path}"
            )
    except:
        if is_xfail:
            pytest.xfail(
                f"Expected failure for {name} in {case['version']} (tests/spec_tests/config.yaml), dir={tmp_path}"
            )
        raise
    if is_xfail:
        pytest.fail(f"case {name} in {case['version']} passed but was marked xfail, dir={tmp_path}")


def _basenameize(obj):
    if isinstance(obj, str) and obj.startswith("/"):
        return os.path.basename(obj)
    elif isinstance(obj, list):
        return [_basenameize(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: _basenameize(v) for k, v in obj.items()}
    return obj
