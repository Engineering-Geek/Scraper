from setuptools import setup, find_packages

setup(
    name='GoogleNewsArticleScraper',
    version='0.1',
    description='Scrapes Google News for Articles and scrapes those Articles',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Nikhil Melgiri',
    author_email='melgirinik@gmail.com',
    url='https://github.com/yourusername/yourpackage',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=open('requirements.txt').read().splitlines(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
