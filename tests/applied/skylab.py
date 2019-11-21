"""
Harness for testing `miniwdl run` on HumanCellAtlas/skylab test workflows

Based on test WDL filenames, reads the test_inputs.json, downloads gs:// files and rewrites the
input JSON with the local filenames. Then starts the workflow with this rewritten JSON.
"""
import sys
import os
import subprocess
import json
import glob

skylab_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_corpi/HumanCellAtlas/skylab"))
WDL_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../WDL"))

def stage_skylab_test_inputs(test_inputs_json_filename, dir="."):
    def rw(v, index=None):
        if isinstance(v, list):
            return list(map(lambda p: rw(p[1],index=p[0]), enumerate(v)))
        if isinstance(v, str) and v.startswith("gs://"): 
            print(v)
            dn = os.path.join(dir, k)
            if index is not None:
                dn = os.path.join(dn, str(index))
            os.makedirs(dn, exist_ok=True)
            subprocess.check_call(["gsutil", "cp", "-n", v, dn])
            return os.path.abspath(glob.glob(os.path.join(dn, "*"))[0])
        return v
    with open(test_inputs_json_filename, "r") as infile:
        inputs = json.load(infile)
    for k, v in inputs.items():
        inputs[k] = rw(v)
    return inputs

if __name__ == "__main__":
    test_wdl = [
        "test/snap_atac/pr/test_snap_atac_PR.wdl",
        "test/bulk_rna_pipeline/pr/test_bulk_rna_pipeline_PR.wdl",
        "test/optimus/pr/test_optimus_PR.wdl",
        "test/smartseq2_single_sample/pr/test_smartseq2_single_sample_PR.wdl",
        "test/smartseq2_single_sample_unpaired/pr/test_smartseq2_single_sample_unpaired_PR.wdl",
    ]
    for wdl in test_wdl:
        wdl = os.path.join(skylab_dir, wdl)
        staged_inputs = stage_skylab_test_inputs(os.path.join(os.path.dirname(wdl), "test_inputs.json"))

        with open("staged_test_inputs.json", "w") as outfile:
            json.dump(staged_inputs, outfile, indent=2)
        try:
            subprocess.check_call([
                "python3", "-m", "WDL",
                "run", wdl,
                "-p", os.path.join(skylab_dir, "library/tasks"),
                "-p", os.path.join(skylab_dir, "pipelines/bulk_rna_pipeline"),
                "-p", os.path.join(skylab_dir, "pipelines/optimus"),
                "-p", os.path.join(skylab_dir, "pipelines/smartseq2_single_sample"),
                "-p", os.path.join(skylab_dir, "pipelines/smartseq2_single_sample_unpaired"),
                "-p", os.path.join(skylab_dir, "pipelines/snap-atac"),
                "-i", "staged_test_inputs.json",
                "--verbose", "--no-quant-check"
            ], env={"PYTHONPATH": os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))})
        except subprocess.CalledProcessError:
            sys.exit(1)
