from os.path import join, dirname
from setuptools import find_packages, setup

__version__ = None
exec(open('easybase/_version.py', 'r').read())


def get_file_contents(filename):
    with open(join(dirname(__file__), filename)) as fp:
        return fp.read()


def get_install_requires():
    requirements = get_file_contents('requirements.txt')
    install_requires = []
    for line in requirements.split('\n'):
        line = line.strip()
        if line and not line.startswith('-'):
            install_requires.append(line)
    return install_requires


setup(
    name='easybase',
    version=__version__,
    description="""A developer-friendly Python library to interact with Apache HBase,support thrift2. It's heavily references to happybase. thank Wouter Bolsterlee""",
    long_description=get_file_contents('README.md'),
    author="wgzhao",
    author_email="wgzhao@gmail.com",
    url='https://github.com/wgzhao/easybase',
    install_requires=get_install_requires(),
    keywords="HBase,easybase,happybase",
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
