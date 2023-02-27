from setuptools import setup, find_packages


with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    
setup(
    name='my_fastapi_app',
    version='0.1.0',
    author='Your Name',
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'my_fastapi_app=my_fastapi_app.main:app',
        ],
    },
)
