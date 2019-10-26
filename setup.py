from setuptools import find_packages, setup

setup(
    name='siaslice',
    version='0.0',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'aiofile',
        'aiohttp'
    ],
    entry_points={
        'console_scripts': [
            'siaslice=siaslice:main'
        ]
    }
)
