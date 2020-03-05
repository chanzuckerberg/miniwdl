from setuptools import setup

setup(
    name='miniwdl_log_task_usage',
    version='0.0.1',
    description='miniwdl task plugin to log container cpu+mem usage',
    author='Wid L. Hacker',
    py_modules=["miniwdl_log_task_usage"],
    python_requires='>=3.6',
    setup_requires=['reentry'],
    reentry_register=True,
    entry_points={
        'miniwdl.plugin.task': ['log_task_usage = miniwdl_log_task_usage:main'],
    }
)
