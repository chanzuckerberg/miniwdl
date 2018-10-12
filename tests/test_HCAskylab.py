import unittest, inspect, subprocess, tempfile, os, glob
from .context import WDL

# Crawl HumanCellAtlas/skylab/library/tasks/*.wdl and assert successful parsing
# and type-checking of all tasks
class TestHCAskylab(unittest.TestCase):
    pass

tdn = tempfile.mkdtemp(prefix='miniwdl_test_HCAskylab_')
subprocess.check_call(['wget', '-q', '-O', 'skylab.zip', 'https://github.com/HumanCellAtlas/skylab/archive/face906.zip'], cwd=tdn)
subprocess.check_call(['unzip', '-q', 'skylab.zip'], cwd=tdn)
task_files = glob.glob(os.path.join(tdn, 'skylab-*', 'library', 'tasks', '*.wdl'))
for fn in task_files:
    name = os.path.split(fn)[1]
    name = name[:-4]
    name = 'test_HCAskylab_task_' + name.replace('.', '_')
    def t(self, fn=fn):
        with open(fn) as infile:
            for task in WDL.parse_tasks(infile.read()):
                task.typecheck()
    setattr(TestHCAskylab, name, t)
