from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="girder-wholetale",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="2.0.7",
    description="Girder plugin implementing Whole Tale core functionality.",
    packages=find_packages(),
    include_package_data=True,
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.10",
    setup_requires=["setuptools-git"],
    install_requires=[
        "girder>=5.0.0a5.dev0",
        "girder-jobs>=5.0.0a5.dev0",
        "girder-plugin-worker>=5.0.0a5.dev0",
        "girder-virtual-resources",
        "girder-oauth>=5.0.0a5.dev0",
        "rdflib",
        "celery[redis]",
        "pathvalidate",
        "influxdb-client",
        "python-magic",
        "requests",
        "validators",
        "html2markdown",
        "lxml_html_clean",
        "GitPython",
        "httpio>=0.3.0",
        "fs",
        "gwvolman>=2.1.2",
    ],
    entry_points={"girder.plugin": ["wholetale = girder_wholetale:WholeTalePlugin"]},
    zip_safe=False,
)
