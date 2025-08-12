"""
datacompose/operators/primitives.py
=====================================
Simple and elegant compose decorator framework for building data pipelines.
"""

import ast
import inspect
import logging
import textwrap
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence

# Set up module logger
logger = logging.getLogger(__name__)

try:
    from pyspark.sql import Column  # type: ignore
except ImportError:
    pass


class SmartPrimitive:
    """Wraps a PySpark column transformation function to enable partial application.

    SmartPrimitive allows column transformation functions to be:
    1. Called directly with a column: `primitive(col)`
    2. Pre-configured with parameters: `primitive(param=value)` returns a configured function

    This enables building reusable, parameterized transformations that can be composed
    into data pipelines.

    Example:
        >>> def trim_spaces(col, chars=' '):
        ...     return f.trim(col, chars)
        >>>
        >>> trim = SmartPrimitive(trim_spaces)
        >>>
        >>> # Direct usage
        >>> df.select(trim(f.col("text")))
        >>>
        >>> # Pre-configured usage
        >>> trim_tabs = trim(chars='\t')
        >>> df.select(trim_tabs(f.col("text")))


    Please note that you will not use this directly. It will be used in the PrimitiveRegistry class
    """

    def __init__(self, func: Callable, name: Optional[str] = None):
        """Initialize a SmartPrimitive.

        Args:
            func: The column transformation function to wrap
            name: Optional name for the primitive (defaults to func.__name__)
        """
        self.func = func
        self.name = name or func.__name__
        self.__doc__ = func.__doc__

    def __call__(self, col: Optional[Column] = None, **kwargs):  # type: ignore
        """Apply the transformation or return a configured version.

        Args:
            col: Optional PySpark Column to transform. If provided, applies the
                 transformation immediately. If None, returns a configured function.
            **kwargs: Parameters to pass to the transformation function

        Returns:
            If col is provided: The transformed Column
            If col is None: A configured function that takes a Column
        """
        if col is not None:  # type: ignore
            return self.func(col, **kwargs)  # type: ignore
        else:

            @wraps(self.func)
            def configured(col: Column):  # type: ignore
                return self.func(col, **kwargs)  # type: ignore

            configured.__name__ = (
                f"{self.name}({', '.join(f'{k}={v}' for k, v in kwargs.items())})"
            )
            return configured


class PrimitiveRegistry:
    """Container for organizing related column transformation primitives.

    PrimitiveRegistry groups related SmartPrimitive transformations under a common
    namespace, making them accessible as attributes. This provides a clean API for
    organizing and accessing transformation functions.

    Example:
        >>> # Create a registry for string operations
        >>> string = PrimitiveRegistry("string")
        >>>
        >>> # Register transformations
        >>> @string.register()
        >>> def lowercase(col):
        ...     return f.lower(col)
        >>>
        >>> @string.register()
        >>> def trim(col, chars=' '):
        ...     return f.trim(col, chars)
        >>>
        >>> # Use the transformations
        >>> df.select(string.lowercase(f.col("text")))
        >>> df.select(string.trim(chars='\t')(f.col("text")))
    """

    def __init__(self, namespace_name: str):
        """Initialize a PrimitiveRegistry.

        Args:
            namespace_name: Name for this namespace (used in error messages)
        """
        self.namespace_name = namespace_name
        self._primitives = {}
        self._conditionals = {}

    def register(self, name: Optional[str] = None, is_conditional: bool = False):
        """Decorator to register a function as a SmartPrimitive in this namespace.

        Args:
            name: Optional name for the primitive (defaults to function name)

        Returns:
            Decorator function that wraps the target function as a SmartPrimitive

        Example:
            >>> ns = PrimitiveRegistry("text")
            >>> @ns.register()
            >>> def clean(col):
            ...     return f.trim(f.lower(col))
        """

        def decorator(func: Callable):
            primitive_name = name or func.__name__

            if is_conditional:
                self._conditionals[primitive_name] = SmartPrimitive(
                    func, primitive_name
                )
                setattr(self, primitive_name, self._conditionals[primitive_name])
            else:
                self._primitives[primitive_name] = SmartPrimitive(func, primitive_name)
                setattr(self, primitive_name, self._primitives[primitive_name])
            # return self._primitives[primitive_name]
            return func

        return decorator

    def __getattr__(self, name):
        if name in self._primitives:
            return self._primitives[name]
        elif name in self._conditionals:
            return self._conditionals[name]
        else:
            raise AttributeError(f"No primitive '{name}' in {self.namespace_name}")

    def compose(
        self,
        func: Optional[Callable] = None,
        *,
        debug: bool = False,
        steps: Optional[list] = None,
        **namespaces,
    ):
        """Decorator that converts a function body into a composed transformation pipeline.

        The compose decorator analyzes the AST of a function and extracts a sequence of
        transformation calls, creating a pipeline that applies them in order. This allows
        declarative pipeline definitions using natural function call syntax.

        Args:
            func: Function to convert into a pipeline
            debug: If True, prints each transformation as it's applied
            steps: Optional list of pre-configured steps (bypasses AST parsing)
            **namespaces: Namespace objects to use for resolving transformations

        Returns:
            A composed function that applies all transformations in sequence

        Example:
            >>> string = PrimitiveRegistry("string")
            >>>
            >>> @string.register()
            >>> def trim(col, chars=' '):
            ...     return f.trim(col, chars)
            >>>
            >>> @string.register()
            >>> def lowercase(col):
            ...     return f.lower(col)
            >>>
            >>> @compose(string=string)
            >>> def clean_text():
            ...     string.trim()
            ...     string.lowercase()
            >>>
            >>> # Use the composed pipeline
            >>> df.select(clean_text(f.col("text")))
        """

        def decorator(func: Callable):
            # Check if steps are provided directly
            if steps is not None:
                # Old style with explicit steps
                def pipeline(col: Column) -> Column:  # type: ignore
                    result = col  # type: ignore
                    for step in steps:  # type: ignore
                        result = step(result)
                    return result

                pipeline.__name__ = func.__name__
                pipeline.__doc__ = func.__doc__
                return pipeline

            # Try to get the function as a string and parse it
            try:
                compiler = PipelineCompiler(namespaces, debug)
                pipeline = compiler.compile(func)

                if debug and pipeline.steps:
                    logger.info(
                        f"Successfully compiled '{func.__name__}' with {len(pipeline.steps)} steps"
                    )

                return pipeline
            except Exception as e:
                logger.warning(
                    f"Advanced compilation failed for '{func.__name__}': {e}. "
                    f"Falling back to sequential extraction."
                )
                if debug:
                    logger.debug("Compilation error details:", exc_info=True)

                # Fallback: Extract just the function calls sequentially
                # This maintains backward compatibility
                return _fallback_compose(func, namespaces, debug)

        if func is None:
            # Called with arguments @compose(debug=True, email=email_namespace)
            return decorator
        else:
            # Called without arguments @compose
            return decorator(func)


def _fallback_compose(func: Callable, namespaces: Dict, debug: bool) -> Callable:
    """Fallback for when compilation fails - extracts sequential calls only"""
    try:
        source = inspect.getsource(func)
        source = textwrap.dedent(source)
        tree = ast.parse(source)
        func_def = tree.body[0]

        # Extract only simple function calls (old behavior)
        steps = []
        if isinstance(func_def, ast.FunctionDef):
            for node in func_def.body:
                if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                    if isinstance(node.value.func, ast.Attribute):
                        namespace_name = (
                            node.value.func.value.id
                            if isinstance(node.value.func.value, ast.Name)
                            else None
                        )
                        method_name = node.value.func.attr
                        namespace = (
                            namespaces.get(namespace_name) if namespace_name else None
                        ) or (globals().get(namespace_name) if namespace_name else None)
                        if namespace and hasattr(namespace, method_name):
                            method = getattr(namespace, method_name)

                            kwargs = {}
                            for keyword in node.value.keywords:
                                try:
                                    kwargs[keyword.arg] = ast.literal_eval(
                                        keyword.value
                                    )
                                except Exception:
                                    pass

                            steps.append(method(**kwargs) if kwargs else method)

        def pipeline(col: Column) -> Column:  # type: ignore
            result = col  # type: ignore
            for step in steps:
                if debug:
                    logger.debug(f"Executing step: {getattr(step, '__name__', step)}")
                result = step(result)  # type: ignore
            return result

        pipeline.__name__ = func.__name__
        pipeline.__doc__ = func.__doc__
        return pipeline

    except Exception as e:
        logger.error(
            f"Failed to create pipeline for '{func.__name__}': {e}. "
            f"Returning identity function."
        )

        # Ultimate fallback - return identity function
        def pipeline(col: Column) -> Column:  # type: ignore
            return col  # type: ignore

        pipeline.__name__ = func.__name__
        pipeline.__doc__ = f"Failed to compile {func.__name__}"
        return pipeline


try:
    from pyspark.sql import Column
    from pyspark.sql import functions as F
except ImportError:
    logging.debug("PySpark not available")

# Set up module logger
logger = logging.getLogger(__name__)


@dataclass
class CompiledStep:
    """A compiled pipeline step"""

    step_type: str
    action: Optional[Callable] = None
    condition: Optional[Callable] = None
    then_branch: Optional[List["CompiledStep"]] = None
    else_branch: Optional[List["CompiledStep"]] = None

    def __post_init__(self):
        """Validate the compiled step after initialization"""
        self.validate()

    def validate(self):
        """Validate that the step is properly configured"""
        valid_types = {"transform", "conditional"}

        if self.step_type not in valid_types:
            raise ValueError(
                f"Invalid step_type '{self.step_type}'. "
                f"Must be one of {valid_types}"
            )

        if self.step_type == "transform":
            if not callable(self.action):
                raise ValueError(
                    f"Transform step requires a callable action, "
                    f"got {type(self.action).__name__}"
                )
            if self.condition is not None:
                logger.warning("Transform step has condition which will be ignored")
            if self.then_branch is not None or self.else_branch is not None:
                logger.warning("Transform step has branches which will be ignored")

        elif self.step_type == "conditional":
            if not callable(self.condition):
                raise ValueError(
                    f"Conditional step requires a callable condition, "
                    f"got {type(self.condition).__name__ if self.condition else 'None'}"
                )
            if not self.then_branch:
                raise ValueError("Conditional step requires at least a then_branch")
            if self.action is not None:
                logger.warning("Conditional step has action which will be ignored")

            # Validate nested steps
            for step in self.then_branch:
                if not isinstance(step, CompiledStep):
                    raise TypeError(
                        f"then_branch must contain CompiledStep instances, "
                        f"got {type(step).__name__}"
                    )

            if self.else_branch:
                for step in self.else_branch:
                    if not isinstance(step, CompiledStep):
                        raise TypeError(
                            f"else_branch must contain CompiledStep instances, "
                            f"got {type(step).__name__}"
                        )


class StablePipeline:
    """Stable runtime pipeline executor"""

    def __init__(self, steps: Optional[List[CompiledStep]] = None, debug=False):
        self.steps = steps or []
        self.debug = debug
        self.__name__ = "pipeline"
        self.__doc__ = "Compiled pipeline"
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Validate all steps
        self._validate_pipeline()

    def __call__(self, col: Column) -> Column:  # type: ignore
        """Execute the pipeline"""
        return self._execute_steps(self.steps, col)  # type: ignore

    def _execute_steps(self, steps: List[CompiledStep], col: Column) -> Column:  # type: ignore
        result = col  # type: ignore

        for step in steps:
            if self.debug:
                step_name = (
                    getattr(step.action, "__name__", step.step_type)
                    if step.action
                    else step.step_type
                )
                self.logger.debug(f"Executing step: {step_name}")

            if step.step_type == "transform":
                if callable(step.action):
                    result = step.action(result)

            elif step.step_type == "conditional":
                if step.then_branch:
                    then_result = self._execute_steps(step.then_branch, result)  # type: ignore

                    if step.else_branch:
                        else_result = self._execute_steps(step.else_branch, result)  # type: ignore
                        result = F.when(step.condition(result), then_result).otherwise(  # type: ignore
                            else_result
                        )
                    else:
                        result = F.when(step.condition(result), then_result).otherwise(  # type: ignore
                            result
                        )

        return result

    def _validate_pipeline(self):
        """Validate all steps in the pipeline"""
        if not self.steps:
            self.logger.debug("Empty pipeline - no steps to validate")
            return

        for i, step in enumerate(self.steps):
            if not isinstance(step, CompiledStep):
                raise TypeError(
                    f"Pipeline step {i} must be a CompiledStep instance, "
                    f"got {type(step).__name__}"
                )
            # Step validation happens in CompiledStep.__post_init__

        self.logger.debug(f"Pipeline validated with {len(self.steps)} steps")


class PipelineCompiler:
    def __init__(self, namespaces: Dict[str, Any], debug: bool = False):
        self.namespaces = namespaces
        self.debug = debug

    def compile(self, func: Callable) -> StablePipeline:
        try:
            source = inspect.getsource(func)
            source = textwrap.dedent(source)
            tree = ast.parse(source)
            func_def = tree.body[0]

            # Ensure we have a function definition
            if not isinstance(func_def, ast.FunctionDef):
                raise ValueError(f"Expected FunctionDef, got {type(func_def).__name__}")

            steps = self._compile_body(func_def.body)
            pipeline = StablePipeline(steps, self.debug)
            pipeline.__name__ = func.__name__
            pipeline.__doc__ = func.__doc__

            return pipeline

        except Exception as e:
            logger.warning(
                f"Failed to compile '{func.__name__}': {e}. "
                f"Creating empty pipeline as fallback."
            )
            if self.debug:
                logger.debug(f"Compilation error details: {e}", exc_info=True)
            # Return empty pipeline on failure
            return StablePipeline([], self.debug)

    def _compile_body(self, nodes: Sequence[ast.AST]) -> List[CompiledStep]:
        """Compile AST nodes to steps"""
        steps = []

        for node in nodes:
            if isinstance(node, ast.If):
                step = self._compile_if(node)
                if step:
                    steps.append(step)

            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                step = self._compile_call(node.value)
                if step:
                    steps.append(step)

        return steps

    def _compile_if(self, node: ast.If) -> Optional[CompiledStep]:
        """Compile if/else statement"""
        condition = self._compile_condition(node.test)
        then_branch = self._compile_body(node.body)
        else_branch = self._compile_body(node.orelse) if node.orelse else None

        try:
            return CompiledStep(
                step_type="conditional",
                condition=condition,
                then_branch=then_branch,
                else_branch=else_branch,
            )
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to compile conditional: {e}")
            if self.debug:
                logger.debug("Conditional compilation error details:", exc_info=True)
            return None

    def _compile_condition(self, node: ast.AST) -> Callable:
        """Compile condition expression"""
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            namespace_name = (
                node.func.value.id if isinstance(node.func.value, ast.Name) else None
            )
            method_name = node.func.attr

            namespace = (
                self.namespaces.get(namespace_name) if namespace_name else None
            ) or (globals().get(namespace_name) if namespace_name else None)
            if namespace and hasattr(namespace, method_name):
                method = getattr(namespace, method_name)

                kwargs = {}
                for keyword in node.keywords:
                    kwargs[keyword.arg] = self._get_value(keyword.value)

                return method(**kwargs) if kwargs else method

        return lambda col: True

    def _compile_call(self, node: ast.Call) -> Optional[CompiledStep]:
        """Compile function call"""
        if isinstance(node.func, ast.Attribute):
            namespace_name = (
                node.func.value.id if isinstance(node.func.value, ast.Name) else None
            )
            method_name = node.func.attr

            namespace = (
                self.namespaces.get(namespace_name) if namespace_name else None
            ) or (globals().get(namespace_name) if namespace_name else None)
            if namespace and hasattr(namespace, method_name):
                method = getattr(namespace, method_name)

                kwargs = {}
                for keyword in node.keywords:
                    kwargs[keyword.arg] = self._get_value(keyword.value)

                action = method(**kwargs) if kwargs else method

                try:
                    return CompiledStep(step_type="transform", action=action)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to compile transform: {e}")
                    if self.debug:
                        logger.debug(
                            "Transform compilation error details:", exc_info=True
                        )
                    return None

        return None

    def _get_value(self, node: ast.AST) -> Any:
        """Extract value from AST node"""
        if isinstance(node, ast.Constant):
            return node.value
        # Python 3.7 compatibility - handle legacy literal nodes
        elif hasattr(ast, "Num") and isinstance(node, (ast.Num, ast.Str, ast.Bytes, ast.NameConstant)):  # type: ignore
            return node.value  # type: ignore
        else:
            try:
                return ast.literal_eval(node)
            except Exception as e:
                logger.debug(f"Failed to extract value from AST node: {e}")
                return None


__all__ = [
    "SmartPrimitive",
    "PrimitiveRegistry",
]
