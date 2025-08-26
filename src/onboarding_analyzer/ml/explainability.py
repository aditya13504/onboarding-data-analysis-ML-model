"""
Recommendation Explainability and Transparency System

Provides interpretable AI capabilities for understanding recommendation decisions
through SHAP values, feature importance analysis, and explanation generation.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pickle
from pathlib import Path

# SHAP for model explanations
try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False
    logging.warning("SHAP not available. Install with: pip install shap")

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
explanation_requests = Counter('recommendation_explanations_total', 'Total explanation requests', ['explanation_type', 'status'])
explanation_latency = Histogram('recommendation_explanation_duration_seconds', 'Explanation generation time')
explanation_cache_hits = Counter('recommendation_explanation_cache_hits_total', 'Explanation cache hits')
active_explainers = Gauge('recommendation_active_explainers', 'Number of active explainer models')

logger = logging.getLogger(__name__)

@dataclass
class FeatureImportance:
    """Feature importance for recommendation explanation."""
    feature_name: str
    importance_score: float
    feature_value: Any
    contribution: float
    description: str

@dataclass
class RecommendationExplanation:
    """Complete explanation for a recommendation."""
    item_id: str
    user_id: str
    predicted_score: float
    confidence_score: float
    feature_importances: List[FeatureImportance]
    top_factors: List[str]
    explanation_text: str
    counterfactual_analysis: Optional[Dict[str, Any]]
    timestamp: datetime
    explanation_method: str

@dataclass
class ExplanationConfig:
    """Configuration for explanation generation."""
    method: str = "shap"  # shap, lime, permutation
    top_k_features: int = 10
    include_counterfactuals: bool = True
    cache_explanations: bool = True
    cache_ttl_hours: int = 24
    explanation_detail_level: str = "medium"  # low, medium, high
    include_confidence: bool = True

class SHAPExplainer:
    """SHAP-based explainer for recommendation models."""
    
    def __init__(self, model, feature_names: List[str], background_data: Optional[np.ndarray] = None):
        self.model = model
        self.feature_names = feature_names
        self.background_data = background_data
        self.explainer = None
        self._initialize_explainer()
    
    def _initialize_explainer(self):
        """Initialize SHAP explainer based on model type."""
        if not SHAP_AVAILABLE:
            raise ImportError("SHAP not available")
        
        try:
            # Try TreeExplainer for tree-based models
            self.explainer = shap.TreeExplainer(self.model)
            logger.info("Initialized SHAP TreeExplainer")
        except Exception:
            try:
                # Fall back to KernelExplainer for other models
                if self.background_data is not None:
                    self.explainer = shap.KernelExplainer(
                        self.model.predict, 
                        self.background_data
                    )
                    logger.info("Initialized SHAP KernelExplainer")
                else:
                    logger.warning("No background data provided for KernelExplainer")
            except Exception as e:
                logger.error(f"Failed to initialize SHAP explainer: {e}")
                raise
    
    def explain(self, features: np.ndarray, user_id: str, item_id: str) -> Dict[str, Any]:
        """Generate SHAP explanation for features."""
        if self.explainer is None:
            raise ValueError("SHAP explainer not initialized")
        
        try:
            # Get SHAP values
            if hasattr(self.explainer, 'shap_values'):
                shap_values = self.explainer.shap_values(features)
            else:
                shap_values = self.explainer(features)
            
            # Handle different SHAP value formats
            if isinstance(shap_values, list):
                # Multi-class case, take first class
                shap_values = shap_values[0]
            elif hasattr(shap_values, 'values'):
                # SHAP Explanation object
                shap_values = shap_values.values
            
            # Create feature importances
            feature_importances = []
            for i, (feature_name, shap_value, feature_value) in enumerate(
                zip(self.feature_names, shap_values.flatten(), features.flatten())
            ):
                importance = FeatureImportance(
                    feature_name=feature_name,
                    importance_score=abs(shap_value),
                    feature_value=feature_value,
                    contribution=shap_value,
                    description=f"Feature {feature_name} contributed {shap_value:.4f} to the prediction"
                )
                feature_importances.append(importance)
            
            # Sort by importance
            feature_importances.sort(key=lambda x: x.importance_score, reverse=True)
            
            return {
                'feature_importances': feature_importances,
                'base_value': getattr(self.explainer, 'expected_value', 0.0),
                'shap_values': shap_values.tolist(),
                'method': 'shap'
            }
            
        except Exception as e:
            logger.error(f"SHAP explanation failed: {e}")
            raise

class PermutationExplainer:
    """Permutation-based feature importance explainer."""
    
    def __init__(self, model, feature_names: List[str], 
                 scoring_function=None, n_iterations: int = 10):
        self.model = model
        self.feature_names = feature_names
        self.scoring_function = scoring_function or (lambda y_true, y_pred: -np.mean((y_true - y_pred) ** 2))
        self.n_iterations = n_iterations
    
    def explain(self, features: np.ndarray, user_id: str, item_id: str) -> Dict[str, Any]:
        """Generate permutation-based explanation."""
        try:
            # Get baseline prediction
            baseline_pred = self.model.predict(features.reshape(1, -1))[0]
            
            feature_importances = []
            
            for i, feature_name in enumerate(self.feature_names):
                importance_scores = []
                
                # Permute feature multiple times
                for _ in range(self.n_iterations):
                    # Create permuted features
                    permuted_features = features.copy()
                    
                    # Randomly permute this feature
                    if len(features.shape) > 1:
                        # Multiple samples
                        np.random.shuffle(permuted_features[:, i])
                    else:
                        # Single sample - use random value from reasonable range
                        feature_std = np.std(features) if np.std(features) > 0 else 1.0
                        permuted_features[i] = np.random.normal(features[i], feature_std)
                    
                    # Get prediction with permuted feature
                    permuted_pred = self.model.predict(permuted_features.reshape(1, -1))[0]
                    
                    # Calculate importance as prediction change
                    importance = abs(baseline_pred - permuted_pred)
                    importance_scores.append(importance)
                
                # Average importance across iterations
                avg_importance = np.mean(importance_scores)
                
                importance = FeatureImportance(
                    feature_name=feature_name,
                    importance_score=avg_importance,
                    feature_value=features[i],
                    contribution=avg_importance * (1 if baseline_pred > 0.5 else -1),
                    description=f"Permuting {feature_name} changes prediction by {avg_importance:.4f}"
                )
                feature_importances.append(importance)
            
            # Sort by importance
            feature_importances.sort(key=lambda x: x.importance_score, reverse=True)
            
            return {
                'feature_importances': feature_importances,
                'base_value': baseline_pred,
                'permutation_scores': [fi.importance_score for fi in feature_importances],
                'method': 'permutation'
            }
            
        except Exception as e:
            logger.error(f"Permutation explanation failed: {e}")
            raise

class CounterfactualAnalyzer:
    """Generates counterfactual explanations for recommendations."""
    
    def __init__(self, model, feature_names: List[str], feature_ranges: Dict[str, Tuple[float, float]]):
        self.model = model
        self.feature_names = feature_names
        self.feature_ranges = feature_ranges
    
    def generate_counterfactuals(self, features: np.ndarray, target_change: float = 0.1,
                               max_iterations: int = 100) -> Dict[str, Any]:
        """Generate counterfactual explanations."""
        try:
            original_pred = self.model.predict(features.reshape(1, -1))[0]
            target_pred = original_pred + target_change
            
            counterfactuals = []
            
            # Try modifying each feature individually
            for i, feature_name in enumerate(self.feature_names):
                if feature_name not in self.feature_ranges:
                    continue
                
                min_val, max_val = self.feature_ranges[feature_name]
                best_cf = None
                best_distance = float('inf')
                
                # Search for counterfactual values
                search_values = np.linspace(min_val, max_val, 20)
                
                for new_value in search_values:
                    if abs(new_value - features[i]) < 1e-6:
                        continue  # Skip original value
                    
                    # Create modified features
                    cf_features = features.copy()
                    cf_features[i] = new_value
                    
                    # Get prediction
                    cf_pred = self.model.predict(cf_features.reshape(1, -1))[0]
                    
                    # Check if we reached target
                    if (target_change > 0 and cf_pred >= target_pred) or \
                       (target_change < 0 and cf_pred <= target_pred):
                        
                        # Calculate distance (feature change)
                        distance = abs(new_value - features[i])
                        
                        if distance < best_distance:
                            best_distance = distance
                            best_cf = {
                                'feature_name': feature_name,
                                'original_value': features[i],
                                'counterfactual_value': new_value,
                                'feature_change': new_value - features[i],
                                'prediction_change': cf_pred - original_pred,
                                'distance': distance
                            }
                
                if best_cf:
                    counterfactuals.append(best_cf)
            
            return {
                'counterfactuals': counterfactuals,
                'original_prediction': original_pred,
                'target_prediction': target_pred,
                'target_change': target_change
            }
            
        except Exception as e:
            logger.error(f"Counterfactual generation failed: {e}")
            return {'counterfactuals': [], 'error': str(e)}

class ExplanationCache:
    """Cache for explanation results."""
    
    def __init__(self, max_size: int = 10000, ttl_hours: int = 24):
        self.max_size = max_size
        self.ttl_hours = ttl_hours
        self.cache: Dict[str, Dict] = {}
        self.access_times: Dict[str, datetime] = {}
    
    def _generate_key(self, user_id: str, item_id: str, features_hash: str) -> str:
        """Generate cache key."""
        return f"{user_id}:{item_id}:{features_hash}"
    
    def _cleanup_expired(self):
        """Remove expired entries."""
        current_time = datetime.now()
        expired_keys = []
        
        for key, access_time in self.access_times.items():
            if current_time - access_time > timedelta(hours=self.ttl_hours):
                expired_keys.append(key)
        
        for key in expired_keys:
            self.cache.pop(key, None)
            self.access_times.pop(key, None)
    
    def get(self, user_id: str, item_id: str, features: np.ndarray) -> Optional[Dict]:
        """Get explanation from cache."""
        features_hash = str(hash(features.tobytes()))
        key = self._generate_key(user_id, item_id, features_hash)
        
        self._cleanup_expired()
        
        if key in self.cache:
            self.access_times[key] = datetime.now()
            explanation_cache_hits.inc()
            return self.cache[key]
        
        return None
    
    def put(self, user_id: str, item_id: str, features: np.ndarray, explanation: Dict):
        """Store explanation in cache."""
        features_hash = str(hash(features.tobytes()))
        key = self._generate_key(user_id, item_id, features_hash)
        
        # Ensure cache size limit
        if len(self.cache) >= self.max_size:
            # Remove oldest entry
            oldest_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
            self.cache.pop(oldest_key, None)
            self.access_times.pop(oldest_key, None)
        
        self.cache[key] = explanation
        self.access_times[key] = datetime.now()

class RecommendationExplainer:
    """Main explainer class for recommendation systems."""
    
    def __init__(self, model, feature_names: List[str], 
                 config: ExplanationConfig = None):
        self.model = model
        self.feature_names = feature_names
        self.config = config or ExplanationConfig()
        
        # Initialize explainers
        self.shap_explainer = None
        self.permutation_explainer = None
        self.counterfactual_analyzer = None
        
        # Initialize cache
        self.cache = ExplanationCache(
            ttl_hours=self.config.cache_ttl_hours
        ) if self.config.cache_explanations else None
        
        # Thread pool for async operations
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        active_explainers.set(1)
        logger.info("RecommendationExplainer initialized")
    
    def initialize_shap_explainer(self, background_data: Optional[np.ndarray] = None):
        """Initialize SHAP explainer."""
        try:
            self.shap_explainer = SHAPExplainer(
                self.model, self.feature_names, background_data
            )
            logger.info("SHAP explainer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize SHAP explainer: {e}")
    
    def initialize_permutation_explainer(self, n_iterations: int = 10):
        """Initialize permutation explainer."""
        self.permutation_explainer = PermutationExplainer(
            self.model, self.feature_names, n_iterations=n_iterations
        )
        logger.info("Permutation explainer initialized")
    
    def initialize_counterfactual_analyzer(self, feature_ranges: Dict[str, Tuple[float, float]]):
        """Initialize counterfactual analyzer."""
        self.counterfactual_analyzer = CounterfactualAnalyzer(
            self.model, self.feature_names, feature_ranges
        )
        logger.info("Counterfactual analyzer initialized")
    
    def _generate_explanation_text(self, feature_importances: List[FeatureImportance],
                                 prediction_score: float) -> str:
        """Generate human-readable explanation text."""
        if not feature_importances:
            return "No explanation available."
        
        # Get top factors
        top_factors = feature_importances[:3]
        
        if self.config.explanation_detail_level == "low":
            return f"Recommendation score: {prediction_score:.2f}. " \
                   f"Top factor: {top_factors[0].feature_name}"
        
        elif self.config.explanation_detail_level == "medium":
            factor_text = ", ".join([f.feature_name for f in top_factors])
            return f"Recommendation score: {prediction_score:.2f}. " \
                   f"Main contributing factors: {factor_text}"
        
        else:  # high detail
            explanations = []
            for factor in top_factors:
                contribution = "positively" if factor.contribution > 0 else "negatively"
                explanations.append(
                    f"{factor.feature_name} (value: {factor.feature_value}) "
                    f"contributes {contribution} with importance {factor.importance_score:.3f}"
                )
            
            return f"Recommendation score: {prediction_score:.2f}. " \
                   f"Detailed breakdown: {'; '.join(explanations)}"
    
    async def explain_recommendation(self, features: np.ndarray, user_id: str, 
                                   item_id: str) -> RecommendationExplanation:
        """Generate complete explanation for a recommendation."""
        with explanation_latency.time():
            explanation_requests.labels(
                explanation_type=self.config.method, 
                status='started'
            ).inc()
            
            try:
                # Check cache first
                if self.cache:
                    cached_explanation = self.cache.get(user_id, item_id, features)
                    if cached_explanation:
                        explanation_requests.labels(
                            explanation_type=self.config.method,
                            status='cache_hit'
                        ).inc()
                        return RecommendationExplanation(**cached_explanation)
                
                # Get prediction score
                prediction_score = self.model.predict(features.reshape(1, -1))[0]
                
                # Generate explanation based on method
                explanation_data = await self._generate_explanation(features, user_id, item_id)
                
                # Get feature importances
                feature_importances = explanation_data.get('feature_importances', [])
                
                # Limit to top K features
                top_features = feature_importances[:self.config.top_k_features]
                
                # Generate counterfactuals if enabled
                counterfactual_analysis = None
                if (self.config.include_counterfactuals and 
                    self.counterfactual_analyzer):
                    cf_result = await asyncio.get_event_loop().run_in_executor(
                        self.executor,
                        self.counterfactual_analyzer.generate_counterfactuals,
                        features
                    )
                    counterfactual_analysis = cf_result
                
                # Calculate confidence score
                confidence_score = self._calculate_confidence(
                    prediction_score, feature_importances
                ) if self.config.include_confidence else 0.0
                
                # Generate explanation text
                explanation_text = self._generate_explanation_text(
                    top_features, prediction_score
                )
                
                # Create explanation object
                explanation = RecommendationExplanation(
                    item_id=item_id,
                    user_id=user_id,
                    predicted_score=prediction_score,
                    confidence_score=confidence_score,
                    feature_importances=top_features,
                    top_factors=[f.feature_name for f in top_features[:5]],
                    explanation_text=explanation_text,
                    counterfactual_analysis=counterfactual_analysis,
                    timestamp=datetime.now(),
                    explanation_method=self.config.method
                )
                
                # Cache explanation
                if self.cache:
                    self.cache.put(user_id, item_id, features, asdict(explanation))
                
                explanation_requests.labels(
                    explanation_type=self.config.method,
                    status='success'
                ).inc()
                
                return explanation
                
            except Exception as e:
                logger.error(f"Explanation generation failed: {e}")
                explanation_requests.labels(
                    explanation_type=self.config.method,
                    status='error'
                ).inc()
                raise
    
    async def _generate_explanation(self, features: np.ndarray, user_id: str, 
                                  item_id: str) -> Dict[str, Any]:
        """Generate explanation using configured method."""
        if self.config.method == "shap" and self.shap_explainer:
            return await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.shap_explainer.explain,
                features, user_id, item_id
            )
        
        elif self.config.method == "permutation" and self.permutation_explainer:
            return await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.permutation_explainer.explain,
                features, user_id, item_id
            )
        
        else:
            # Fallback to permutation if configured method not available
            if not self.permutation_explainer:
                self.initialize_permutation_explainer()
            
            return await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.permutation_explainer.explain,
                features, user_id, item_id
            )
    
    def _calculate_confidence(self, prediction_score: float, 
                            feature_importances: List[FeatureImportance]) -> float:
        """Calculate confidence score for explanation."""
        if not feature_importances:
            return 0.0
        
        # Base confidence on prediction certainty and feature importance distribution
        prediction_certainty = min(prediction_score, 1 - prediction_score) * 2
        
        # Feature importance entropy (lower entropy = higher confidence)
        importances = [fi.importance_score for fi in feature_importances]
        if sum(importances) > 0:
            normalized_importances = [imp / sum(importances) for imp in importances]
            entropy = -sum(p * np.log(p + 1e-10) for p in normalized_importances if p > 0)
            max_entropy = np.log(len(normalized_importances))
            importance_confidence = 1 - (entropy / max_entropy) if max_entropy > 0 else 0
        else:
            importance_confidence = 0
        
        # Combine confidences
        confidence = (prediction_certainty + importance_confidence) / 2
        return min(max(confidence, 0.0), 1.0)
    
    async def explain_batch(self, batch_features: List[np.ndarray], 
                          user_ids: List[str], item_ids: List[str]) -> List[RecommendationExplanation]:
        """Generate explanations for a batch of recommendations."""
        tasks = []
        for features, user_id, item_id in zip(batch_features, user_ids, item_ids):
            task = self.explain_recommendation(features, user_id, item_id)
            tasks.append(task)
        
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    def save_explanation(self, explanation: RecommendationExplanation, filepath: str):
        """Save explanation to file."""
        explanation_dict = asdict(explanation)
        # Convert datetime to string for JSON serialization
        explanation_dict['timestamp'] = explanation.timestamp.isoformat()
        
        with open(filepath, 'w') as f:
            json.dump(explanation_dict, f, indent=2, default=str)
    
    def load_explanation(self, filepath: str) -> RecommendationExplanation:
        """Load explanation from file."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Convert timestamp back to datetime
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        
        # Convert feature importances back to objects
        data['feature_importances'] = [
            FeatureImportance(**fi) for fi in data['feature_importances']
        ]
        
        return RecommendationExplanation(**data)
    
    def get_global_feature_importance(self, sample_features: List[np.ndarray],
                                    sample_user_ids: List[str],
                                    sample_item_ids: List[str]) -> Dict[str, float]:
        """Calculate global feature importance across sample data."""
        feature_importance_sums = {name: 0.0 for name in self.feature_names}
        total_samples = len(sample_features)
        
        for features, user_id, item_id in zip(sample_features, sample_user_ids, sample_item_ids):
            try:
                # Get explanation
                if self.config.method == "shap" and self.shap_explainer:
                    explanation_data = self.shap_explainer.explain(features, user_id, item_id)
                else:
                    explanation_data = self.permutation_explainer.explain(features, user_id, item_id)
                
                # Add feature importances
                for fi in explanation_data['feature_importances']:
                    feature_importance_sums[fi.feature_name] += fi.importance_score
                    
            except Exception as e:
                logger.error(f"Failed to get explanation for global importance: {e}")
                total_samples -= 1
        
        # Average the importances
        if total_samples > 0:
            global_importance = {
                name: importance / total_samples 
                for name, importance in feature_importance_sums.items()
            }
        else:
            global_importance = feature_importance_sums
        
        return dict(sorted(global_importance.items(), key=lambda x: x[1], reverse=True))
