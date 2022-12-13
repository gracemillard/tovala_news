import setuptools

setuptools.setup(
    name="generic_setup",
    version="0.0.1",
    description="Generic Python Setup",
    long_description_content_type="text/markdown",
    author="author",
    install_requires=[
        "black",
        "pytest",
        "gspread",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
