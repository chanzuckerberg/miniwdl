from setuptools import setup, find_packages

setup(
    name='miniwdl_task_omnibus_example',
    version='0.0.1',
    description='miniwdl task runtime plugin (omnibus example)',
    author='Wid L. Hacker',
    py_modules=["miniwdl_task_omnibus_example"],
    python_requires='>=3.6',
    setup_requires=['reentry'],
    install_requires=["boto3"],
    reentry_register=True,
    entry_points={
        'miniwdl.plugin.task': ['omnibus = miniwdl_task_omnibus_example:main'],
    }
)
