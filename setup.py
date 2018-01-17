from setuptools import setup

requirements = [
    # package requirements go here
]

setup(
    name='dask.targeted',
    version='0.0.0',
    description="Using dask delayed with luigi targets",
    author="Noah D. Brenowitz",
    author_email='nbren12@uw.edu',
    url='https://github.com/nbren12/dask.targeted',
    packages=['targeted'],
    entry_points={
        'console_scripts': [
            'daskluigi=daskluigi.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='dask.targeted',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ]
)
