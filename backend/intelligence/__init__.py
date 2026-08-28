"""StreamETL Pipeline Intelligence layer.

The package keeps AI assistance outside the deterministic DAG -> PySpark
compiler. LLMs may propose a native StreamETL DAG, but Pydantic and the
semantic advisor validate it before the existing generator can execute it.
"""

__version__ = "1.1.0"
