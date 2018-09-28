#!/usr/bin/env python3
#
# runs analyze_tasks.py on HCA skylab tasks

import subprocess, tempfile, os, glob
import WDL

# fetch tip of master from github.com/HCA/skylab and crawl
# library/tasks/*.wdl files 
def fetch_skylab_tasks():
    tdn = tempfile.mkdtemp(prefix='analyze_HCAskylab_tasks_')
    subprocess.check_call(['wget', '-q', 'https://github.com/HumanCellAtlas/skylab/archive/master.zip'], cwd=tdn)
    subprocess.check_call(['unzip', '-q', 'master.zip'], cwd=tdn)
    return glob.glob(os.path.join(tdn, 'skylab-master', 'library', 'tasks', '*.wdl'))

subprocess.check_call(["python3", os.path.join(os.path.dirname(__file__), "analyze_tasks.py")] + fetch_skylab_tasks())

