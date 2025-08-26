"""Top-level package exports for onboarding_analyzer.

Expose commonly imported submodules so tests using `from onboarding_analyzer.tasks import X` or
`onboarding_analyzer.infrastructure.db` resolve without explicit import ordering side effects.
"""

from importlib import import_module as _imp

_submodules = [
	"config",
	"infrastructure.db",
	# Avoid auto-importing models.tables to prevent duplicate table registration during certain test loaders.
	"tasks.analytics",
	"tasks.ingestion",
	"tasks.maintenance",
	"tasks.clustering",
	"tasks.anomaly",
	"tasks.kafka_audit",
]

for _m in _submodules:
	try:
		_imp(f"onboarding_analyzer.{_m}")
	except Exception:  # pragma: no cover
		pass

__all__ = ["config", "models", "tasks"]
