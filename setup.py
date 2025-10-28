'''
This setup script is for use in the climate data api package
'''

from setuptools import setup, find_packages


setup(
    # --- PROJECT METATDATA ---
    name='climate_data_API',
    version='1.0.0',
    description='A Python-based API was developed to streamline the process of downloading, processing, and analyzing PRISM Climate Group datasets.',
    author='Bri Moore',
    url='https://github.com/BSEACD/Climate-Data-API',
    license='GPL-3.0',

    # --- PACKAGES & MODULES ---
    packages=find_packages(),
    py_modules=['main'],

    # --- REQUIRED DEPENDENCIES ---
    install_requires=[
        'requests>=2.20.0',
        'numpy>=1.20.0',
        'rasterio>=1.2.0',
        'pandas>=1.3.0',
        'geopandas>=0.10.0',
        'python-dateutil>=2.8.0', 
        'shapely>=1.8.0',         
        'regex',
    ],

    # --- ENTRY POINTS ---
    entry_points={
        'console_scripts': [
            'run_api=main:main',
        ],
    },
)
