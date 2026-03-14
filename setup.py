# setup.py
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy

extensions = [
    Extension(
        "bs_solver",
        sources=["bs_solver.pyx"],
        language="c++",
        extra_compile_args=["-std=c++11", "-O3"],
    ),
    Extension(
        "mcts_solver",
        sources=["mcts_solver.pyx"],
        language="c++",
        extra_compile_args=["-std=c++11", "-O3"],
    )
]

setup(
    name="BRP_Solvers",
    ext_modules=cythonize(extensions, language_level="3"),
    include_dirs=[numpy.get_include()]
)