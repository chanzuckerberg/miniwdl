import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="miniwdl",
    version="0.0.1",
    author="CZI",
    description="Static analysis toolkit for Workflow Description Language",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chanzuckerberg/miniwdl",
    packages=["WDL"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        'console_scripts': [
            'miniwdl = WDL.CLI:main'
        ]
    },
    install_requires=[
        'lark-parser==0.6.4',
    ]
)
