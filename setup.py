from setuptools import setup, find_packages

setup(
    name='kommatipara',
    version='1.0',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'pyspark',
        'python',
        'chispa'
    ],
    entry_points={
        'console_scripts': [
            'kommatipara = main:main'
        ]
    },
)
