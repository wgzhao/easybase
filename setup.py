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
    description="""Python/Python3 library to interact with Apache HBase,support HBase 2.0, time-range scan and HBase thrift 2 procotol. """,
    long_description=get_file_contents('README.rst'),
    author="wgzhao",
    author_email="wgzhao@gmail.com",
    url='https://github.com/wgzhao/easybase',
    install_requires=get_install_requires(),
    keywords="HBase,easybase,thrift2",
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
