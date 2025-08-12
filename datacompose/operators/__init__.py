"""
Datacompose Operators Module
=======================

This module provides the core framework for building composable data transformation pipelines.

Main Components:
- SmartPrimitive: Enables partial application of transformations
- PrimitiveRegistry: Container for organizing related transformations
- PipelineCompiler: Compiles declarative syntax into executable pipelines
- StablePipeline: Runtime executor for compiled pipelines
"""

from .primitives import SmartPrimitive, PrimitiveRegistry

__all__ = [
    "SmartPrimitive",
    "PrimitiveRegistry",
]

__version__ = "0.2.4"
