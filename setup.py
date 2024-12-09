from setuptools import setup, find_packages

setup(
    name="polymorphic-data-model",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'tqdm',
        'numpy',
        'pandas',
        'plotly',
        'python-dotenv'
    ]
)
