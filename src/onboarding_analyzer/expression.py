"""Expression validation utilities for user-provided rule conditions.

We intentionally restrict the grammar to a safe subset:
  - Boolean operations (and/or/not)
  - Comparisons (==, !=, <, <=, >, >=)
  - Numeric & string constants
  - Function calls to count(<event_name>) and feature(<feature_key>) only
  - Names resolved via the above calls (no free variable access allowed)

Anything else (attribute access, subscripting, comprehensions, lambdas, imports,
arbitrary function names) is rejected. This prevents abuse while keeping
expressiveness needed for basic personalization logic.
"""
from __future__ import annotations
import ast
from typing import Tuple

ALLOWED_CALLS = {"count", "feature"}
ALLOWED_COMPARE_OPS = (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)
ALLOWED_BOOL_OPS = (ast.And, ast.Or)
ALLOWED_UNARY_OPS = (ast.Not,)


class _Validator(ast.NodeVisitor):
    def __init__(self) -> None:
        self.ok = True
        self.reason: str | None = None

    def fail(self, node: ast.AST, msg: str):  # pragma: no cover - defensive
        if self.ok:  # record first failure
            self.ok = False
            self.reason = msg

    # Disallow any attribute access
    def visit_Attribute(self, node: ast.Attribute):  # pragma: no cover - simple reject
        self.fail(node, "attribute_access_not_allowed")

    def visit_Subscript(self, node: ast.Subscript):  # pragma: no cover
        self.fail(node, "subscript_not_allowed")

    def visit_ListComp(self, node: ast.ListComp):  # pragma: no cover
        self.fail(node, "comprehension_not_allowed")

    visit_DictComp = visit_ListComp
    visit_SetComp = visit_ListComp
    visit_GeneratorExp = visit_ListComp

    def visit_Call(self, node: ast.Call):
        if not isinstance(node.func, ast.Name) or node.func.id not in ALLOWED_CALLS:
            self.fail(node, "call_not_allowed")
            return
        # Args must be simple constants / strings
        for a in node.args:
            if not isinstance(a, (ast.Constant,)):
                self.fail(node, "arg_type_not_allowed")
        # No kwargs
        if node.keywords:
            self.fail(node, "kwargs_not_allowed")
        # Visit children to ensure no nested complexity
        self.generic_visit(node)

    def visit_BoolOp(self, node: ast.BoolOp):
        if not isinstance(node.op, ALLOWED_BOOL_OPS):
            self.fail(node, "bool_op_not_allowed")
            return
        for v in node.values:
            self.visit(v)

    def visit_UnaryOp(self, node: ast.UnaryOp):
        if not isinstance(node.op, ALLOWED_UNARY_OPS):
            self.fail(node, "unary_op_not_allowed")
            return
        self.visit(node.operand)

    def visit_Compare(self, node: ast.Compare):
        for op in node.ops:
            if not isinstance(op, ALLOWED_COMPARE_OPS):
                self.fail(node, "compare_op_not_allowed")
                return
        # Validate left + comparators recursively
        self.visit(node.left)
        for comp in node.comparators:
            self.visit(comp)

    def visit_Name(self, node: ast.Name):
        # Names should only appear as function identifiers (handled in Call) or booleans
        if node.id not in ("True", "False") and node.id not in ALLOWED_CALLS:
            # If it appears standalone it's suspicious
            self.fail(node, "name_not_allowed")

    def visit_IfExp(self, node: ast.IfExp):  # pragma: no cover
        self.fail(node, "ternary_not_allowed")

    def visit_Lambda(self, node: ast.Lambda):  # pragma: no cover
        self.fail(node, "lambda_not_allowed")

    def generic_visit(self, node: ast.AST):
        if not self.ok:
            return
        super().generic_visit(node)


def validate_condition_expr(expr: str) -> Tuple[bool, str | None]:
    try:
        tree = ast.parse(expr, mode="eval")  # Expect a single expression
    except SyntaxError as e:  # pragma: no cover - simple syntax error path
        return False, f"syntax_error:{e.msg}"
    v = _Validator()
    v.visit(tree)
    return v.ok, v.reason


class _Evaluator(ast.NodeVisitor):
    """Evaluate a previously validated expression AST.

    Only supports the safe subset enforced by _Validator. Evaluation relies on two helper
    callables provided in the eval() entrypoint: count(name)->int, feature(key)->float.
    """
    def __init__(self, helpers: dict[str, callable]):
        self.helpers = helpers

    def visit_Expression(self, node: ast.Expression):  # pragma: no cover - simple pass-through
        return self.visit(node.body)

    def visit_BoolOp(self, node: ast.BoolOp):
        values = [self.visit(v) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(values)
        if isinstance(node.op, ast.Or):
            return any(values)
        raise ValueError("bool_op_not_allowed_eval")

    def visit_UnaryOp(self, node: ast.UnaryOp):
        operand = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            return not bool(operand)
        raise ValueError("unary_op_not_allowed_eval")

    def visit_Compare(self, node: ast.Compare):
        left = self.visit(node.left)
        result = True
        cur = left
        for op, comp in zip(node.ops, node.comparators):
            right = self.visit(comp)
            if isinstance(op, ast.Eq):
                ok = cur == right
            elif isinstance(op, ast.NotEq):
                ok = cur != right
            elif isinstance(op, ast.Lt):
                ok = cur < right
            elif isinstance(op, ast.LtE):
                ok = cur <= right
            elif isinstance(op, ast.Gt):
                ok = cur > right
            elif isinstance(op, ast.GtE):
                ok = cur >= right
            else:  # pragma: no cover
                raise ValueError("compare_op_not_allowed_eval")
            if not ok:
                result = False
                break
            cur = right
        return result

    def visit_Call(self, node: ast.Call):
        fn_name = node.func.id  # validated already
        helper = self.helpers.get(fn_name)
        if not helper:
            raise ValueError("helper_not_provided")
        args = []
        for a in node.args:
            args.append(self.visit(a))
        return helper(*args)

    def visit_Name(self, node: ast.Name):
        if node.id in ("True", "False"):
            return node.id == "True"
        # function identifiers resolved in Call; standalone names invalid
        raise ValueError("unexpected_name")

    def visit_Constant(self, node: ast.Constant):  # noqa: D401
        return node.value

    def generic_visit(self, node: ast.AST):  # pragma: no cover
        raise ValueError(f"unsupported_node:{type(node).__name__}")


def evaluate_condition_expr(expr: str, helpers: dict[str, callable]) -> bool:
    """Validate then safely evaluate condition expression with provided helpers.

    Returns False if invalid or evaluation error occurs.
    """
    ok, _reason = validate_condition_expr(expr)
    if not ok:
        return False
    try:
        tree = ast.parse(expr, mode="eval")
        evaluator = _Evaluator(helpers)
        return bool(evaluator.visit(tree))
    except Exception:
        return False

__all__ = ["validate_condition_expr", "evaluate_condition_expr"]
