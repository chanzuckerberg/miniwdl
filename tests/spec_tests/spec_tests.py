"""
Automatically generated pytest suite for WDL spec conformance tests.
"""

import re
import json
import subprocess
import pytest
from pathlib import Path
import shutil
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


def parse_spec_for(version):
    """
    Parse SPEC.md and data for a given spec version, yield test cases.
    Each case dict includes 'version', 'name', 'wdl', 'inputs', 'outputs', 'config'.
    """
    spec_dir = SPEC_BASE / version
    spec_md = spec_dir / "SPEC.md"
    data_dir = spec_dir / "tests" / "data"
    text = spec_md.read_text(encoding="utf-8")
    yaml = YAML(typ="safe")
    blocks = re.findall(r"<details>(.*?)</details>", text, re.DOTALL)
    for block in blocks:
        m = re.search(
            r"<summary>\s*Example:\s*(.+?)\s*```wdl(.*?)```", block, re.DOTALL
        )
        if not m:
            continue
        name = m.group(1).strip()
        if name in ("multiline_strings2.wdl", "multiline_strings3.wdl"):
            continue  # JSON syntax errors in WDL 1.2 spec
        try:
            wdl_code = m.group(2).strip()
            m_in = re.search(r"Example input:\s*```json(.*?)```", block, re.DOTALL)
            inputs = yaml.load(m_in.group(1)) if m_in else {}
            m_out = re.search(r"Example output:\s*```json(.*?)```", block, re.DOTALL)
            outputs = yaml.load(m_out.group(1)) if m_out else {}
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
            "config": config,
            "data_dir": data_dir,
        }


CASES = [case for v in VERSIONS for case in parse_spec_for(v)]


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
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
    if (
        name in xfail_list
        or name.endswith("_fail.wdl")
        or name.endswith("_fail_task.wdl")
    ):
        pytest.xfail(f"Marked xfail for {case['version']}: {name}")
    # copy spec test-data directory for this test
    shutil.copytree(case["data_dir"], tmp_path, dirs_exist_ok=True)

    # dump all WDL example files into version-specific subdirectories so imports resolve
    for other in CASES:
        version_dir = tmp_path / other["version"]
        version_dir.mkdir(parents=True, exist_ok=True)
        path = version_dir / other["name"]
        assert not path.exists()
        path.write_text(other["wdl"], encoding="utf-8")

    # write WDL file and input JSON
    wdl_file = tmp_path / case["version"] / name
    inp_file = tmp_path / "input.json"
    inp_file.write_text(json.dumps(inputs), encoding="utf-8")
    out_file = tmp_path / "output.json"

    # construct command
    cmd = [
        "python3",
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
    result = subprocess.run(cmd, env=cmd_env, capture_output=True)
    should_fail = config.get("fail", False)
    if should_fail:
        assert result.returncode != 0, f"Expected failure but got success for {name}"
        return
    if result.returncode != 0:
        print(f"Command failed: {' '.join(cmd)}")
        print(f"stdout:\n{result.stdout.decode('utf-8')}\n--")
        print(f"stderr:\n{result.stderr.decode('utf-8')}")
    assert result.returncode == 0

    # verify outputs
    got = json.loads(out_file.read_text(encoding="utf-8"))["outputs"]
    exclude = set(config.get("exclude_outputs", []))
    for k, v in outputs.items():
        if k in exclude:
            continue
        got_v = _basenameize(got.get(k))
        assert got_v == v, (
            f"Mismatch for '{k}' in {name}: expected {v}, got {got.get(k)}"
        )


def _basenameize(obj):
    if isinstance(obj, str) and obj.startswith("/"):
        return os.path.basename(obj)
    elif isinstance(obj, list):
        return [_basenameize(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: _basenameize(v) for k, v in obj.items()}
    return obj
