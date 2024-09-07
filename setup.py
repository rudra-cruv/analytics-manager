from setuptools import setup, find_packages

setup(
    name="analytics_manager",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "newrelic_telemetry_sdk==0.5.1"
    ],
    author="Rudra Sikri",
    author_email="rudrasikri12@gmail.com",
    description="A small package to easily interface with the New Relic Telemetry SDK",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/rudra-cruv/analytics-manager",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
