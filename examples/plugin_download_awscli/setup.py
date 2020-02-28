from setuptools import setup, find_packages

setup(
    name='miniwdl_download_awscli',
    version='0.0.1',
    description='miniwdl download plugin for s3:// using awscli',
    author='Wid L. Hacker',
    py_modules=["miniwdl_download_awscli"],
    python_requires='>=3.6',
    setup_requires=['reentry'],
    install_requires=["boto3"],
    reentry_register=True,
    entry_points={
        'miniwdl.plugin.file_download': ['s3 = miniwdl_download_awscli:main'],
    }
)
