from setuptools import find_packages, setup

setup(
    name="girder-wholetale",
    version="2.0.0",
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
        "girder>=3",
        "girder-jobs",
        "girder-worker",
        "girder-globus-handler",
        "girder-virtual-resources",
        "rdflib",
        "celery[redis]",
        "python-magic",
        "requests",
        "validators",
        "html2markdown",
        "lxml",
        "GitPython",
        "httpio>=0.3.0",
        "fs",
    ],
    entry_points={
        "girder.plugin": ["girder_wholetale = girder_wholetale:WholeTalePlugin"]
    },
    zip_safe=False,
)


#git+https://github.com/whole-tale/girderfs@master#egg=girderfs
#git+https://github.com/whole-tale/gwvolman@master#egg=gwvolman
