from distutils.core import setup

setup(
    name='ParallelPark',
    version='0.0.1',
    author='Willet Inc',
    author_email='brian@willetinc.com',
    packages=['parallelpark'],
    scripts=[],
    url='http://pypi.python.org/pypi/ParallelPark/',
    license='LICENSE.txt',
    description='Run over things in parallel.',
    install_requires=[
        "futures >= 2.1.5",
    ],
)
