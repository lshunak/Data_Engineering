from setuptools import setup, find_packages

setup(
    name="queue_service",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pika',
    ]
)