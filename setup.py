from setuptools import find_packages, find_namespace_packages, setup

setup(
    name='fbadsearch',
    version='0.1.1',
    description='DMRC fb ad library search',
    author='Nic Suzor',
    author_email='n.suzor@qut.edu.au',
    url='https://github.com/qut-dmrc/fbadsearch',
    packages=find_packages(),
    namespace_packages=find_namespace_packages()
    #['fbadsearch']
)