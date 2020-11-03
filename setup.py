from setuptools import setup, find_namespace_packages


def _read(filename: str) -> str:
    """
    Reads in the content of the file.

    :param filename:    The file to read.
    :return:            The file content.
    """
    with open(filename, "r") as file:
        return file.read()


setup(
    name="ufdl-annotations-plugin",
    description="Repository with plugins for the wai.annotations library to tie into the UFDL backend.",
    long_description=f"{_read('DESCRIPTION.rst')}\n"
                     f"{_read('CHANGES.rst')}",
    url="https://github.com/waikato-ufdl/ufdl-annotations-plugin",
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Programming Language :: Python :: 3',
    ],
    license='Apache License Version 2.0',
    package_dir={
        '': 'src'
    },
    packages=find_namespace_packages(where='src'),
    namespace_packages=[
        "ufdl"
    ],
    version="0.0.1",
    author='Corey Sterling',
    author_email='coreytsterling@gmail.com',
    install_requires=[
        "ufdl.pythonclient==0.0.1",
        "ufdl.json-messages==0.0.1",
        "ufdl-annotation-utils==0.0.1"
    ],
    entry_points={
        "wai.annotations.plugins": [
            "from-ufdl-classify=ufdl.annotations_plugin.specifiers:UFDLClassifyInputFormatSpecifier",
            "to-ufdl-classify=ufdl.annotations_plugin.specifiers:UFDLClassifyOutputFormatSpecifier",
            "from-ufdl-objdet=ufdl.annotations_plugin.specifiers:UFDLObjDetInputFormatSpecifier",
            "to-ufdl-objdet=ufdl.annotations_plugin.specifiers:UFDLObjDetOutputFormatSpecifier"
        ]
    }
)
