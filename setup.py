"""
Setup configuration for ER Harness Validation
"""

from setuptools import setup, find_packages

# Read requirements
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

# Read README for long description
with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='er-harness-validation',
    version='1.0.0',
    description='A validation framework for Entity Resolution outputs',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Your Team',
    author_email='your.email@example.com',
    url='https://github.com/emeron1234/er-harness-validation',
    packages=find_packages(exclude=['tests*', 'docs*']),
    install_requires=requirements,
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Quality Assurance',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='entity-resolution validation data-quality pyspark',
    project_urls={
        'Source': 'https://github.com/emeron1234/er-harness-validation',
        'Bug Reports': 'https://github.com/emeron1234/er-harness-validation/issues',
    },
)
