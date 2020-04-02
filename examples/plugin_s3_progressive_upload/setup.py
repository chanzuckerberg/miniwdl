from setuptools import setup, find_packages

setup(
    name='miniwdl_s3_progressive_upload',
    version='0.0.1',
    description='miniwdl plugin for progressive upload of task output files to Amazon S3',
    author='Wid L. Hacker',
    py_modules=["miniwdl_s3_progressive_upload"],
    python_requires='>=3.6',
    setup_requires=['reentry'],
    install_requires=["boto3"],
    reentry_register=True,
    entry_points={
        'miniwdl.plugin.task': ['s3_progressive_upload_task = miniwdl_s3_progressive_upload:task'],
        'miniwdl.plugin.workflow': ['s3_progressive_upload_workflow = miniwdl_s3_progressive_upload:workflow'],
    }
)
