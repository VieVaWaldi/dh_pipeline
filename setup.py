import os
from setuptools import setup, find_packages

os.makedirs(".build", exist_ok=True)

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="dh_pipeline",
    version="0.2",
    install_requires=required,
    packages=find_packages(where="src", exclude=["tests*"]),
    package_dir={"": "src"},
    python_requires=">=3.11",
    # options={
    #     "build": {"build_base": ".build/"},
    #     "bdist_egg": {"dist_dir": ".dist/"},
    #     "egg_info": {"egg_base": ".build/"},  # This will put egg-info in build/
    # },
)
