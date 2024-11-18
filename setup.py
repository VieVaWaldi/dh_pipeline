import os
from setuptools import setup, find_packages

os.makedirs("build", exist_ok=True)

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="DIGICHER_Pipeline",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=required,
    options={
        "build": {"build_base": "build/"},
        "bdist_egg": {"dist_dir": "dist/"},
        "egg_info": {"egg_base": "build/"},  # This will put egg-info in build/
    },
)
