from distutils.core import setup
from Cython.Build import cythonize

setup(
    ext_modules = cythonize("parquet_row_gen.pyx", annotate=True)
)
