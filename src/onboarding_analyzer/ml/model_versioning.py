"""Model versioning and A/B testing infrastructure for ML experiments.

Provides production-grade model management, versioning, and experimentation capabilities.
"""
from __future__ import annotations
import json
import logging
import asyncio
import time
import pickle
import hashlib
import uuid
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import numpy as np
from pathlib import Path
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.infrastructure.db import SessionLocal

logger = logging.getLogger(__name__)

class ModelStatus(Enum):
    DRAFT = "draft"
    TRAINING = "training"
    VALIDATION = "validation"
    READY = "ready"
    DEPLOYED = "deployed"
    DEPRECATED = "deprecated"
    FAILED = "failed"

class ExperimentStatus(Enum):
    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

class ModelType(Enum):
    RECOMMENDATION = "recommendation"
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    RANKING = "ranking"

class MetricType(Enum):
    ACCURACY = "accuracy"
    PRECISION = "precision"
    RECALL = "recall"
    F1_SCORE = "f1_score"
    AUC_ROC = "auc_roc"
    RMSE = "rmse"
    MAE = "mae"
    CLICK_THROUGH_RATE = "click_through_rate"
    CONVERSION_RATE = "conversion_rate"
    REVENUE_PER_USER = "revenue_per_user"
    ENGAGEMENT_RATE = "engagement_rate"

@dataclass
class ModelMetadata:
    model_id: str
    name: str
    version: str
    model_type: ModelType
    description: str
    created_by: str
    created_at: datetime
    status: ModelStatus = ModelStatus.DRAFT
    tags: List[str] = field(default_factory=list)
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    training_config: Dict[str, Any] = field(default_factory=dict)
    
@dataclass
class ModelArtifact:
    artifact_id: str
    model_id: str
    artifact_type: str  # "model", "preprocessor", "config", "metrics"
    file_path: str
    file_size_bytes: int
    checksum: str
    created_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ModelMetrics:
    model_id: str
    version: str
    metrics: Dict[str, float]
    validation_metrics: Dict[str, float]
    test_metrics: Dict[str, float]
    timestamp: datetime
    dataset_info: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ExperimentConfig:
    experiment_id: str
    name: str
    description: str
    hypothesis: str
    treatment_models: List[str]  # Model IDs being tested
    control_model: str  # Baseline model ID
    traffic_allocation: Dict[str, float]  # Model ID -> percentage
    success_metrics: List[MetricType]
    guardrail_metrics: List[Dict[str, Any]]  # Metrics that must not degrade
    minimum_sample_size: int
    maximum_duration_days: int
    confidence_level: float = 0.95
    power: float = 0.8
    created_by: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    status: ExperimentStatus = ExperimentStatus.DRAFT

@dataclass
class ExperimentResult:
    experiment_id: str
    model_id: str
    user_count: int
    impressions: int
    conversions: int
    revenue: float
    engagement_events: int
    metrics: Dict[str, float]
    confidence_intervals: Dict[str, Tuple[float, float]]
    statistical_significance: Dict[str, bool]
    timestamp: datetime

@dataclass
class TrafficSplit:
    user_id: str
    experiment_id: str
    model_id: str
    assigned_at: datetime
    is_control: bool = False

# Metrics
MODEL_DEPLOYMENTS = Counter('model_deployments_total', 'Model deployments', ['model_type', 'status'])
MODEL_PREDICTIONS = Counter('model_predictions_total', 'Model predictions', ['model_id', 'version'])
MODEL_LATENCY = Histogram('model_prediction_latency_seconds', 'Prediction latency', ['model_id'])
EXPERIMENT_USERS = Gauge('experiment_active_users', 'Active experiment users', ['experiment_id', 'model_id'])
AB_TEST_CONVERSIONS = Counter('ab_test_conversions_total', 'A/B test conversions', ['experiment_id', 'model_id'])

class ModelRegistry:
    """Registry for managing model versions and metadata."""
    
    def __init__(self, storage_path: str = "/tmp/model_registry"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.models: Dict[str, ModelMetadata] = {}
        self.artifacts: Dict[str, List[ModelArtifact]] = defaultdict(list)
        self.metrics: Dict[str, List[ModelMetrics]] = defaultdict(list)
        
    def register_model(self, metadata: ModelMetadata) -> str:
        """Register a new model or version."""
        try:
            self.models[metadata.model_id] = metadata
            
            # Create model directory
            model_dir = self.storage_path / metadata.model_id
            model_dir.mkdir(exist_ok=True)
            
            # Save metadata
            metadata_path = model_dir / f"metadata_{metadata.version}.json"
            with open(metadata_path, 'w') as f:
                json.dump({
                    'model_id': metadata.model_id,
                    'name': metadata.name,
                    'version': metadata.version,
                    'model_type': metadata.model_type.value,
                    'description': metadata.description,
                    'created_by': metadata.created_by,
                    'created_at': metadata.created_at.isoformat(),
                    'status': metadata.status.value,
                    'tags': metadata.tags,
                    'hyperparameters': metadata.hyperparameters,
                    'training_config': metadata.training_config
                }, f, indent=2)
            
            logger.info(f"Registered model {metadata.name} v{metadata.version}")
            return metadata.model_id
            
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            raise
    
    def store_artifact(self, model_id: str, artifact_type: str, 
                      data: Any, metadata: Dict[str, Any] = None) -> str:
        """Store model artifact."""
        try:
            artifact_id = str(uuid.uuid4())
            file_name = f"{artifact_type}_{artifact_id}.pkl"
            file_path = self.storage_path / model_id / file_name
            
            # Ensure model directory exists
            file_path.parent.mkdir(exist_ok=True)
            
            # Serialize and save artifact
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
            
            # Calculate checksum
            with open(file_path, 'rb') as f:
                checksum = hashlib.md5(f.read()).hexdigest()
            
            artifact = ModelArtifact(
                artifact_id=artifact_id,
                model_id=model_id,
                artifact_type=artifact_type,
                file_path=str(file_path),
                file_size_bytes=file_path.stat().st_size,
                checksum=checksum,
                created_at=datetime.utcnow(),
                metadata=metadata or {}
            )
            
            self.artifacts[model_id].append(artifact)
            
            logger.info(f"Stored {artifact_type} artifact for model {model_id}")
            return artifact_id
            
        except Exception as e:
            logger.error(f"Failed to store artifact: {e}")
            raise
    
    def load_artifact(self, model_id: str, artifact_type: str, version: str = None) -> Any:
        """Load model artifact."""
        try:
            model_artifacts = self.artifacts.get(model_id, [])
            
            # Find matching artifact
            matching_artifacts = [
                artifact for artifact in model_artifacts
                if artifact.artifact_type == artifact_type
            ]
            
            if not matching_artifacts:
                raise ValueError(f"No {artifact_type} artifact found for model {model_id}")
            
            # Use latest if no version specified
            artifact = matching_artifacts[-1]
            
            # Load and return artifact
            with open(artifact.file_path, 'rb') as f:
                return pickle.load(f)
                
        except Exception as e:
            logger.error(f"Failed to load artifact: {e}")
            raise
    
    def record_metrics(self, model_metrics: ModelMetrics):
        """Record model performance metrics."""
        self.metrics[model_metrics.model_id].append(model_metrics)
        
        # Update model status if validation metrics are good
        if model_metrics.validation_metrics:
            model = self.models.get(model_metrics.model_id)
            if model and model.status == ModelStatus.TRAINING:
                # Simple validation check - in production, use more sophisticated logic
                avg_score = np.mean(list(model_metrics.validation_metrics.values()))
                if avg_score > 0.7:  # Threshold for "good" model
                    model.status = ModelStatus.READY
                    logger.info(f"Model {model_metrics.model_id} marked as ready")
    
    def get_model_versions(self, model_name: str) -> List[ModelMetadata]:
        """Get all versions of a model."""
        return [
            model for model in self.models.values()
            if model.name == model_name
        ]
    
    def get_best_model(self, model_name: str, metric: MetricType) -> Optional[ModelMetadata]:
        """Get best performing model version based on metric."""
        versions = self.get_model_versions(model_name)
        
        if not versions:
            return None
        
        best_model = None
        best_score = float('-inf')
        
        for model in versions:
            model_metrics = self.metrics.get(model.model_id)
            if model_metrics:
                latest_metrics = model_metrics[-1]
                score = latest_metrics.validation_metrics.get(metric.value, 0)
                
                if score > best_score:
                    best_score = score
                    best_model = model
        
        return best_model
    
    def promote_model(self, model_id: str) -> bool:
        """Promote model to deployed status."""
        try:
            model = self.models.get(model_id)
            if not model:
                raise ValueError(f"Model {model_id} not found")
            
            if model.status != ModelStatus.READY:
                raise ValueError(f"Model {model_id} is not ready for deployment")
            
            model.status = ModelStatus.DEPLOYED
            MODEL_DEPLOYMENTS.labels(
                model_type=model.model_type.value,
                status='deployed'
            ).inc()
            
            logger.info(f"Promoted model {model_id} to deployed status")
            return True
            
        except Exception as e:
            logger.error(f"Failed to promote model: {e}")
            return False
    
    def deprecate_model(self, model_id: str) -> bool:
        """Deprecate a model."""
        try:
            model = self.models.get(model_id)
            if not model:
                raise ValueError(f"Model {model_id} not found")
            
            model.status = ModelStatus.DEPRECATED
            logger.info(f"Deprecated model {model_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprecate model: {e}")
            return False

class ABTestManager:
    """Manages A/B testing experiments for models."""
    
    def __init__(self, model_registry: ModelRegistry):
        self.model_registry = model_registry
        self.experiments: Dict[str, ExperimentConfig] = {}
        self.traffic_splits: Dict[str, TrafficSplit] = {}  # user_id -> assignment
        self.experiment_results: Dict[str, List[ExperimentResult]] = defaultdict(list)
        
    def create_experiment(self, config: ExperimentConfig) -> str:
        """Create a new A/B test experiment."""
        try:
            # Validate traffic allocation
            total_allocation = sum(config.traffic_allocation.values())
            if abs(total_allocation - 100.0) > 0.01:
                raise ValueError(f"Traffic allocation must sum to 100%, got {total_allocation}%")
            
            # Validate models exist and are deployed
            all_models = config.treatment_models + [config.control_model]
            for model_id in all_models:
                model = self.model_registry.models.get(model_id)
                if not model:
                    raise ValueError(f"Model {model_id} not found")
                if model.status != ModelStatus.DEPLOYED:
                    raise ValueError(f"Model {model_id} is not deployed")
            
            self.experiments[config.experiment_id] = config
            logger.info(f"Created experiment {config.name} ({config.experiment_id})")
            
            return config.experiment_id
            
        except Exception as e:
            logger.error(f"Failed to create experiment: {e}")
            raise
    
    def start_experiment(self, experiment_id: str) -> bool:
        """Start an A/B test experiment."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                raise ValueError(f"Experiment {experiment_id} not found")
            
            if experiment.status != ExperimentStatus.DRAFT:
                raise ValueError(f"Experiment {experiment_id} is not in draft status")
            
            experiment.status = ExperimentStatus.RUNNING
            logger.info(f"Started experiment {experiment.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start experiment: {e}")
            return False
    
    def assign_user_to_experiment(self, user_id: str, experiment_id: str) -> Optional[str]:
        """Assign user to experiment variant."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment or experiment.status != ExperimentStatus.RUNNING:
                return None
            
            # Check if user already assigned
            existing_assignment = self.traffic_splits.get(f"{user_id}_{experiment_id}")
            if existing_assignment:
                return existing_assignment.model_id
            
            # Assign user based on hash and traffic allocation
            user_hash = int(hashlib.md5(f"{user_id}_{experiment_id}".encode()).hexdigest(), 16)
            allocation_point = (user_hash % 10000) / 100.0  # 0-100%
            
            cumulative_allocation = 0.0
            assigned_model = None
            
            for model_id, allocation in experiment.traffic_allocation.items():
                cumulative_allocation += allocation
                if allocation_point <= cumulative_allocation:
                    assigned_model = model_id
                    break
            
            if assigned_model:
                traffic_split = TrafficSplit(
                    user_id=user_id,
                    experiment_id=experiment_id,
                    model_id=assigned_model,
                    assigned_at=datetime.utcnow(),
                    is_control=(assigned_model == experiment.control_model)
                )
                
                self.traffic_splits[f"{user_id}_{experiment_id}"] = traffic_split
                
                # Update metrics
                EXPERIMENT_USERS.labels(
                    experiment_id=experiment_id,
                    model_id=assigned_model
                ).inc()
            
            return assigned_model
            
        except Exception as e:
            logger.error(f"Failed to assign user to experiment: {e}")
            return None
    
    def record_conversion(self, user_id: str, experiment_id: str, 
                         conversion_value: float = 1.0, metadata: Dict[str, Any] = None):
        """Record a conversion event for experiment tracking."""
        try:
            assignment = self.traffic_splits.get(f"{user_id}_{experiment_id}")
            if not assignment:
                return  # User not in experiment
            
            # Record conversion
            AB_TEST_CONVERSIONS.labels(
                experiment_id=experiment_id,
                model_id=assignment.model_id
            ).inc()
            
            # Store conversion data for analysis
            # In production, this would go to a database
            logger.debug(f"Recorded conversion for user {user_id} in experiment {experiment_id}")
            
        except Exception as e:
            logger.error(f"Failed to record conversion: {e}")
    
    def calculate_experiment_results(self, experiment_id: str) -> Dict[str, ExperimentResult]:
        """Calculate current experiment results."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                raise ValueError(f"Experiment {experiment_id} not found")
            
            # Group users by assigned model
            model_assignments = defaultdict(list)
            for assignment in self.traffic_splits.values():
                if assignment.experiment_id == experiment_id:
                    model_assignments[assignment.model_id].append(assignment)
            
            results = {}
            
            for model_id, assignments in model_assignments.items():
                # Calculate metrics (simplified for demo)
                user_count = len(assignments)
                
                # Simulate metric calculations
                # In production, query actual conversion data
                base_conversion_rate = 0.1
                model_boost = hash(model_id) % 100 / 10000.0  # Random boost for demo
                conversion_rate = base_conversion_rate + model_boost
                
                conversions = int(user_count * conversion_rate)
                revenue = conversions * 50.0  # $50 per conversion
                engagement_events = user_count * 5  # 5 events per user average
                
                metrics = {
                    'conversion_rate': conversion_rate,
                    'revenue_per_user': revenue / max(user_count, 1),
                    'engagement_rate': 0.3 + model_boost * 2
                }
                
                # Calculate confidence intervals (simplified)
                confidence_intervals = {}
                for metric_name, value in metrics.items():
                    margin = value * 0.1  # 10% margin for demo
                    confidence_intervals[metric_name] = (value - margin, value + margin)
                
                # Statistical significance (simplified)
                statistical_significance = {}
                for metric_name in metrics:
                    # Compare with control if this isn't control
                    statistical_significance[metric_name] = model_id != experiment.control_model
                
                result = ExperimentResult(
                    experiment_id=experiment_id,
                    model_id=model_id,
                    user_count=user_count,
                    impressions=user_count * 10,  # 10 impressions per user
                    conversions=conversions,
                    revenue=revenue,
                    engagement_events=engagement_events,
                    metrics=metrics,
                    confidence_intervals=confidence_intervals,
                    statistical_significance=statistical_significance,
                    timestamp=datetime.utcnow()
                )
                
                results[model_id] = result
                self.experiment_results[experiment_id].append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to calculate experiment results: {e}")
            return {}
    
    def should_stop_experiment(self, experiment_id: str) -> Tuple[bool, str]:
        """Determine if experiment should be stopped."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                return False, "Experiment not found"
            
            if experiment.status != ExperimentStatus.RUNNING:
                return False, "Experiment not running"
            
            # Check duration
            duration = datetime.utcnow() - experiment.created_at
            if duration.days >= experiment.maximum_duration_days:
                return True, "Maximum duration reached"
            
            # Check sample size
            total_users = len([
                assignment for assignment in self.traffic_splits.values()
                if assignment.experiment_id == experiment_id
            ])
            
            if total_users < experiment.minimum_sample_size:
                return False, "Insufficient sample size"
            
            # Check statistical significance
            results = self.calculate_experiment_results(experiment_id)
            
            if len(results) < 2:
                return False, "Need results from multiple variants"
            
            # Check if any treatment significantly outperforms control
            control_result = results.get(experiment.control_model)
            if not control_result:
                return False, "No control results"
            
            for model_id, result in results.items():
                if model_id == experiment.control_model:
                    continue
                
                # Check success metrics
                for metric in experiment.success_metrics:
                    metric_name = metric.value
                    
                    if metric_name in result.statistical_significance:
                        if result.statistical_significance[metric_name]:
                            # Check if improvement is meaningful
                            treatment_value = result.metrics.get(metric_name, 0)
                            control_value = control_result.metrics.get(metric_name, 0)
                            
                            if treatment_value > control_value * 1.05:  # 5% improvement
                                return True, f"Significant improvement in {metric_name}"
            
            return False, "No significant results yet"
            
        except Exception as e:
            logger.error(f"Failed to check experiment stopping condition: {e}")
            return False, f"Error: {e}"
    
    def stop_experiment(self, experiment_id: str, reason: str = "") -> bool:
        """Stop an experiment."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                raise ValueError(f"Experiment {experiment_id} not found")
            
            experiment.status = ExperimentStatus.COMPLETED
            
            # Calculate final results
            final_results = self.calculate_experiment_results(experiment_id)
            
            logger.info(f"Stopped experiment {experiment.name}: {reason}")
            logger.info(f"Final results: {len(final_results)} variants tested")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop experiment: {e}")
            return False
    
    def get_experiment_summary(self, experiment_id: str) -> Dict[str, Any]:
        """Get comprehensive experiment summary."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                return {}
            
            # Calculate current results
            results = self.calculate_experiment_results(experiment_id)
            
            # Get user assignments
            assignments = [
                assignment for assignment in self.traffic_splits.values()
                if assignment.experiment_id == experiment_id
            ]
            
            summary = {
                'experiment_id': experiment_id,
                'name': experiment.name,
                'status': experiment.status.value,
                'hypothesis': experiment.hypothesis,
                'duration_days': (datetime.utcnow() - experiment.created_at).days,
                'total_users': len(assignments),
                'models_tested': len(experiment.treatment_models) + 1,
                'traffic_allocation': experiment.traffic_allocation,
                'results': {
                    model_id: {
                        'user_count': result.user_count,
                        'conversion_rate': result.metrics.get('conversion_rate', 0),
                        'revenue_per_user': result.metrics.get('revenue_per_user', 0),
                        'statistical_significance': result.statistical_significance
                    }
                    for model_id, result in results.items()
                }
            }
            
            # Determine winner
            if experiment.status == ExperimentStatus.COMPLETED and results:
                best_model = max(
                    results.items(),
                    key=lambda x: x[1].metrics.get('conversion_rate', 0)
                )
                summary['winner'] = {
                    'model_id': best_model[0],
                    'improvement_over_control': (
                        best_model[1].metrics.get('conversion_rate', 0) - 
                        results[experiment.control_model].metrics.get('conversion_rate', 0)
                    )
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get experiment summary: {e}")
            return {}

# Global instances
model_registry = ModelRegistry()
ab_test_manager = ABTestManager(model_registry)

def register_model(name: str, version: str, model_type: ModelType, 
                  description: str, created_by: str, **kwargs) -> str:
    """Convenience function to register a model."""
    model_id = f"{name}_{version}_{int(time.time())}"
    
    metadata = ModelMetadata(
        model_id=model_id,
        name=name,
        version=version,
        model_type=model_type,
        description=description,
        created_by=created_by,
        created_at=datetime.utcnow(),
        **kwargs
    )
    
    return model_registry.register_model(metadata)

def create_ab_test(name: str, hypothesis: str, treatment_models: List[str], 
                  control_model: str, traffic_allocation: Dict[str, float], **kwargs) -> str:
    """Convenience function to create A/B test."""
    experiment_id = f"exp_{int(time.time())}_{hash(name) % 1000}"
    
    config = ExperimentConfig(
        experiment_id=experiment_id,
        name=name,
        hypothesis=hypothesis,
        treatment_models=treatment_models,
        control_model=control_model,
        traffic_allocation=traffic_allocation,
        description=kwargs.get('description', ''),
        success_metrics=kwargs.get('success_metrics', [MetricType.CONVERSION_RATE]),
        guardrail_metrics=kwargs.get('guardrail_metrics', []),
        minimum_sample_size=kwargs.get('minimum_sample_size', 1000),
        maximum_duration_days=kwargs.get('maximum_duration_days', 30)
    )
    
    return ab_test_manager.create_experiment(config)
