import setuptools

with open("version.txt", "r") as f:
    version = f.read()
    if version:
        versionno = version.split("=")[-1].strip("' ")
        arr = [int(item) for item in versionno.split(".")]
        arr[2] += 1
        version = "%d.%d.%d" % tuple(arr)
        with open("version.txt", "w") as w:
            w.write(version)

setuptools.setup(
    name="gecosistema_database",
    version=version,
    author="Valerio Luzzi",
    author_email="valluzzi@gmail.com",
    description="A small example package",
    long_description="A small example package",
    url="https://github.com/valluzzi/gecosistema_database.git",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    install_requires=['gecosistema_core']
)
