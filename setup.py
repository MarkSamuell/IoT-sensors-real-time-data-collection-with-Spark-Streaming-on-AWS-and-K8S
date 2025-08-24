from setuptools import setup, find_packages

setup(
    name="streaming_pipeline",
    version="0.0.1",
    packages=find_packages(where="src") + ["configs"],
    package_dir={
        "": "src",
        "configs": "configs"
    },
    include_package_data=True,
    install_requires=[
        "elasticsearch==8.0.0",
        "jsonschema",
        "aws_glue_schema_registry",
        "boto3"

    ],
    entry_points={
        "console_scripts": [
            "run_streaming_pipeline=streaming_pipeline.main:main",
        ],
    },
    package_data={
        "configs": ["*.py"],
    },
)