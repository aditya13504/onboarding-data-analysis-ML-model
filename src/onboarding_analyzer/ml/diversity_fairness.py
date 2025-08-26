"""
Recommendation Diversity and Fairness System

Ensures recommendation diversity, fairness across user groups, and bias detection
to provide equitable and varied recommendation experiences.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pickle
from pathlib import Path
from collections import defaultdict, Counter
import random
from itertools import combinations

# Statistical and ML libraries
from scipy.stats import chi2_contingency, entropy
from sklearn.preprocessing import LabelEncoder
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# Prometheus metrics
from prometheus_client import Counter as PrometheusCounter, Histogram, Gauge

# Initialize metrics
diversity_computations = PrometheusCounter('recommendation_diversity_computations_total', 'Total diversity computations')
fairness_violations = PrometheusCounter('recommendation_fairness_violations_total', 'Fairness violations detected', ['violation_type'])
bias_detections = PrometheusCounter('recommendation_bias_detections_total', 'Bias detections', ['bias_type'])
diversity_scores = Histogram('recommendation_diversity_scores', 'Diversity score distribution')
fairness_scores = Histogram('recommendation_fairness_scores', 'Fairness score distribution')

logger = logging.getLogger(__name__)

@dataclass
class DiversityMetrics:
    """Diversity metrics for recommendation sets."""
    intra_list_diversity: float  # Diversity within recommendation list
    coverage: float  # Catalog coverage
    novelty: float  # Average novelty of recommendations
    serendipity: float  # Unexpected but relevant recommendations
    category_distribution: Dict[str, float]  # Category distribution
    temporal_diversity: float  # Diversity across time periods

@dataclass
class FairnessMetrics:
    """Fairness metrics across user groups."""
    demographic_parity: Dict[str, float]  # Equal recommendation rates across groups
    equalized_odds: Dict[str, float]  # Equal accuracy across groups
    calibration: Dict[str, float]  # Prediction calibration across groups
    individual_fairness: float  # Similar users get similar recommendations
    group_fairness: Dict[str, float]  # Group-level fairness measures

@dataclass
class BiasDetection:
    """Bias detection results."""
    bias_type: str
    affected_groups: List[str]
    bias_magnitude: float
    confidence: float
    statistical_significance: float
    description: str
    mitigation_suggestions: List[str]

@dataclass
class DiversityConfig:
    """Configuration for diversity and fairness."""
    # Diversity parameters
    diversity_lambda: float = 0.5  # Trade-off between relevance and diversity
    min_category_representation: float = 0.1  # Minimum category representation
    novelty_time_window_days: int = 30  # Time window for novelty calculation
    serendipity_threshold: float = 0.3  # Threshold for serendipitous recommendations
    
    # Fairness parameters
    protected_attributes: List[str] = None  # Protected demographic attributes
    fairness_threshold: float = 0.1  # Maximum allowed fairness violation
    bias_detection_threshold: float = 0.2  # Bias detection sensitivity
    
    # General parameters
    max_recommendations: int = 20
    enable_bias_mitigation: bool = True
    enable_diversity_boost: bool = True

class DiversityCalculator:
    """Calculator for various diversity metrics."""
    
    def __init__(self, item_features: Dict[str, Dict[str, Any]]):
        self.item_features = item_features
        self.category_popularity = self._calculate_category_popularity()
    
    def _calculate_category_popularity(self) -> Dict[str, float]:
        """Calculate popularity of each category."""
        category_counts = Counter()
        for item_id, features in self.item_features.items():
            category = features.get('category', 'unknown')
            category_counts[category] += 1
        
        total_items = sum(category_counts.values())
        return {
            category: count / total_items 
            for category, count in category_counts.items()
        }
    
    def calculate_intra_list_diversity(self, recommendation_list: List[str]) -> float:
        """Calculate diversity within a recommendation list."""
        if len(recommendation_list) < 2:
            return 0.0
        
        try:
            # Calculate pairwise dissimilarity
            dissimilarities = []
            
            for i, item1 in enumerate(recommendation_list):
                for j, item2 in enumerate(recommendation_list):
                    if i < j:  # Avoid duplicates
                        dissim = self._calculate_item_dissimilarity(item1, item2)
                        dissimilarities.append(dissim)
            
            # Average dissimilarity as diversity measure
            diversity = np.mean(dissimilarities) if dissimilarities else 0.0
            return min(max(diversity, 0.0), 1.0)
            
        except Exception as e:
            logger.error(f"Intra-list diversity calculation failed: {e}")
            return 0.0
    
    def _calculate_item_dissimilarity(self, item1: str, item2: str) -> float:
        """Calculate dissimilarity between two items."""
        if item1 not in self.item_features or item2 not in self.item_features:
            return 1.0  # Maximum dissimilarity if features unknown
        
        features1 = self.item_features[item1]
        features2 = self.item_features[item2]
        
        dissimilarity = 0.0
        feature_count = 0
        
        # Category dissimilarity
        if 'category' in features1 and 'category' in features2:
            dissimilarity += 1.0 if features1['category'] != features2['category'] else 0.0
            feature_count += 1
        
        # Tag dissimilarity (Jaccard distance)
        if 'tags' in features1 and 'tags' in features2:
            tags1 = set(features1['tags'])
            tags2 = set(features2['tags'])
            
            if tags1 or tags2:
                jaccard_sim = len(tags1.intersection(tags2)) / len(tags1.union(tags2))
                dissimilarity += 1.0 - jaccard_sim
                feature_count += 1
        
        # Numerical feature dissimilarity
        numerical_features = ['price', 'rating', 'popularity_score']
        for feature in numerical_features:
            if feature in features1 and feature in features2:
                val1, val2 = features1[feature], features2[feature]
                if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                    # Normalized absolute difference
                    max_val = max(abs(val1), abs(val2), 1.0)  # Avoid division by zero
                    dissimilarity += abs(val1 - val2) / max_val
                    feature_count += 1
        
        return dissimilarity / feature_count if feature_count > 0 else 1.0
    
    def calculate_coverage(self, recommendation_list: List[str], 
                         total_catalog_size: int) -> float:
        """Calculate catalog coverage."""
        unique_items = len(set(recommendation_list))
        return unique_items / total_catalog_size if total_catalog_size > 0 else 0.0
    
    def calculate_novelty(self, recommendation_list: List[str],
                         user_history: List[str],
                         global_popularity: Dict[str, float]) -> float:
        """Calculate novelty of recommendations."""
        if not recommendation_list:
            return 0.0
        
        novelty_scores = []
        
        for item in recommendation_list:
            # Novelty based on user history
            user_novelty = 1.0 if item not in user_history else 0.0
            
            # Novelty based on global popularity (less popular = more novel)
            popularity = global_popularity.get(item, 0.0)
            popularity_novelty = 1.0 - popularity
            
            # Combined novelty
            combined_novelty = 0.6 * user_novelty + 0.4 * popularity_novelty
            novelty_scores.append(combined_novelty)
        
        return np.mean(novelty_scores)
    
    def calculate_serendipity(self, recommendation_list: List[str],
                            user_profile: Dict[str, Any],
                            item_relevance_scores: Dict[str, float]) -> float:
        """Calculate serendipity (unexpected but relevant)."""
        if not recommendation_list:
            return 0.0
        
        serendipity_scores = []
        user_preferences = user_profile.get('category_preferences', {})
        
        for item in recommendation_list:
            if item not in self.item_features:
                continue
            
            item_category = self.item_features[item].get('category', 'unknown')
            relevance = item_relevance_scores.get(item, 0.0)
            
            # Expected probability based on user preferences
            expected_prob = user_preferences.get(item_category, 0.1)
            
            # Serendipity = relevance * unexpectedness
            unexpectedness = 1.0 - expected_prob
            serendipity = relevance * unexpectedness
            
            serendipity_scores.append(serendipity)
        
        return np.mean(serendipity_scores)
    
    def calculate_category_distribution(self, recommendation_list: List[str]) -> Dict[str, float]:
        """Calculate category distribution in recommendations."""
        category_counts = Counter()
        
        for item in recommendation_list:
            if item in self.item_features:
                category = self.item_features[item].get('category', 'unknown')
                category_counts[category] += 1
        
        total_recs = len(recommendation_list)
        return {
            category: count / total_recs 
            for category, count in category_counts.items()
        } if total_recs > 0 else {}
    
    def calculate_temporal_diversity(self, recommendation_lists: List[List[str]]) -> float:
        """Calculate diversity across multiple recommendation lists over time."""
        if len(recommendation_lists) < 2:
            return 0.0
        
        # Calculate overlap between consecutive recommendation lists
        overlaps = []
        
        for i in range(len(recommendation_lists) - 1):
            list1 = set(recommendation_lists[i])
            list2 = set(recommendation_lists[i + 1])
            
            if list1 or list2:
                overlap = len(list1.intersection(list2)) / len(list1.union(list2))
                overlaps.append(overlap)
        
        # Temporal diversity is inverse of average overlap
        avg_overlap = np.mean(overlaps) if overlaps else 0.0
        return 1.0 - avg_overlap

class FairnessAnalyzer:
    """Analyzer for fairness across different user groups."""
    
    def __init__(self, protected_attributes: List[str]):
        self.protected_attributes = protected_attributes
        self.group_statistics = {}
    
    def analyze_demographic_parity(self, recommendations: Dict[str, List[str]],
                                 user_demographics: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
        """Analyze demographic parity across user groups."""
        group_recommendation_rates = defaultdict(list)
        
        # Group users by protected attributes
        for user_id, recs in recommendations.items():
            if user_id not in user_demographics:
                continue
            
            user_demo = user_demographics[user_id]
            
            for attr in self.protected_attributes:
                if attr in user_demo:
                    group_value = user_demo[attr]
                    group_key = f"{attr}_{group_value}"
                    
                    # Calculate recommendation rate (number of recommendations)
                    rec_rate = len(recs)
                    group_recommendation_rates[group_key].append(rec_rate)
        
        # Calculate parity scores
        parity_scores = {}
        
        for attr in self.protected_attributes:
            attr_groups = {k: v for k, v in group_recommendation_rates.items() 
                          if k.startswith(f"{attr}_")}
            
            if len(attr_groups) < 2:
                parity_scores[attr] = 1.0  # Perfect parity if only one group
                continue
            
            # Calculate mean recommendation rates for each group
            group_means = {group: np.mean(rates) for group, rates in attr_groups.items()}
            
            # Parity score as inverse of coefficient of variation
            means = list(group_means.values())
            if np.mean(means) > 0:
                cv = np.std(means) / np.mean(means)
                parity_scores[attr] = max(0.0, 1.0 - cv)
            else:
                parity_scores[attr] = 1.0
        
        return parity_scores
    
    def analyze_equalized_odds(self, recommendations: Dict[str, List[str]],
                             true_preferences: Dict[str, List[str]],
                             user_demographics: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
        """Analyze equalized odds (equal accuracy across groups)."""
        group_accuracies = defaultdict(list)
        
        for user_id, recs in recommendations.items():
            if user_id not in user_demographics or user_id not in true_preferences:
                continue
            
            user_demo = user_demographics[user_id]
            true_prefs = set(true_preferences[user_id])
            rec_set = set(recs)
            
            # Calculate accuracy (precision)
            if recs:
                accuracy = len(rec_set.intersection(true_prefs)) / len(recs)
            else:
                accuracy = 0.0
            
            # Group by protected attributes
            for attr in self.protected_attributes:
                if attr in user_demo:
                    group_value = user_demo[attr]
                    group_key = f"{attr}_{group_value}"
                    group_accuracies[group_key].append(accuracy)
        
        # Calculate equalized odds scores
        odds_scores = {}
        
        for attr in self.protected_attributes:
            attr_groups = {k: v for k, v in group_accuracies.items() 
                          if k.startswith(f"{attr}_")}
            
            if len(attr_groups) < 2:
                odds_scores[attr] = 1.0
                continue
            
            # Calculate accuracy differences
            group_means = {group: np.mean(accs) for group, accs in attr_groups.items()}
            
            # Equalized odds as inverse of accuracy difference range
            mean_values = list(group_means.values())
            accuracy_range = max(mean_values) - min(mean_values)
            odds_scores[attr] = max(0.0, 1.0 - accuracy_range)
        
        return odds_scores
    
    def analyze_individual_fairness(self, recommendations: Dict[str, List[str]],
                                   user_similarities: Dict[Tuple[str, str], float],
                                   similarity_threshold: float = 0.8) -> float:
        """Analyze individual fairness (similar users get similar recommendations)."""
        fairness_violations = []
        
        for (user1, user2), similarity in user_similarities.items():
            if similarity >= similarity_threshold:
                if user1 in recommendations and user2 in recommendations:
                    recs1 = set(recommendations[user1])
                    recs2 = set(recommendations[user2])
                    
                    # Calculate recommendation similarity (Jaccard index)
                    if recs1 or recs2:
                        rec_similarity = len(recs1.intersection(recs2)) / len(recs1.union(recs2))
                    else:
                        rec_similarity = 1.0
                    
                    # Fairness violation if recommendation similarity is much lower than user similarity
                    violation = max(0.0, similarity - rec_similarity)
                    fairness_violations.append(violation)
        
        # Individual fairness as inverse of average violation
        if fairness_violations:
            avg_violation = np.mean(fairness_violations)
            return max(0.0, 1.0 - avg_violation)
        else:
            return 1.0  # Perfect fairness if no violations

class BiasDetector:
    """Detector for various types of bias in recommendations."""
    
    def __init__(self, significance_threshold: float = 0.05):
        self.significance_threshold = significance_threshold
    
    def detect_popularity_bias(self, recommendations: Dict[str, List[str]],
                             item_popularity: Dict[str, float]) -> BiasDetection:
        """Detect bias towards popular items."""
        try:
            # Calculate average popularity of recommended items
            all_recommendations = []
            for recs in recommendations.values():
                all_recommendations.extend(recs)
            
            rec_popularities = [item_popularity.get(item, 0.0) for item in all_recommendations]
            avg_rec_popularity = np.mean(rec_popularities) if rec_popularities else 0.0
            
            # Compare with overall catalog popularity
            catalog_popularities = list(item_popularity.values())
            avg_catalog_popularity = np.mean(catalog_popularities) if catalog_popularities else 0.0
            
            # Calculate bias magnitude
            bias_magnitude = avg_rec_popularity - avg_catalog_popularity
            
            # Statistical significance test
            from scipy.stats import ttest_ind
            if len(rec_popularities) > 1 and len(catalog_popularities) > 1:
                _, p_value = ttest_ind(rec_popularities, catalog_popularities)
                significant = p_value < self.significance_threshold
            else:
                p_value = 1.0
                significant = False
            
            return BiasDetection(
                bias_type="popularity_bias",
                affected_groups=["all_users"],
                bias_magnitude=abs(bias_magnitude),
                confidence=1.0 - p_value,
                statistical_significance=p_value,
                description=f"Recommendations favor {'popular' if bias_magnitude > 0 else 'unpopular'} items "
                           f"by {abs(bias_magnitude):.3f} points",
                mitigation_suggestions=[
                    "Implement diversity boosting",
                    "Add novelty component to ranking",
                    "Use popularity-aware negative sampling"
                ]
            )
            
        except Exception as e:
            logger.error(f"Popularity bias detection failed: {e}")
            return BiasDetection(
                bias_type="popularity_bias",
                affected_groups=[],
                bias_magnitude=0.0,
                confidence=0.0,
                statistical_significance=1.0,
                description="Bias detection failed",
                mitigation_suggestions=[]
            )
    
    def detect_demographic_bias(self, recommendations: Dict[str, List[str]],
                               user_demographics: Dict[str, Dict[str, Any]],
                               protected_attribute: str) -> BiasDetection:
        """Detect bias against specific demographic groups."""
        try:
            # Group recommendations by demographic attribute
            group_recommendations = defaultdict(list)
            
            for user_id, recs in recommendations.items():
                if user_id in user_demographics and protected_attribute in user_demographics[user_id]:
                    group_value = user_demographics[user_id][protected_attribute]
                    group_recommendations[group_value].extend(recs)
            
            if len(group_recommendations) < 2:
                return BiasDetection(
                    bias_type="demographic_bias",
                    affected_groups=[],
                    bias_magnitude=0.0,
                    confidence=0.0,
                    statistical_significance=1.0,
                    description="Insufficient groups for bias detection",
                    mitigation_suggestions=[]
                )
            
            # Calculate recommendation rates
            group_rates = {}
            for group, recs in group_recommendations.items():
                group_users = sum(1 for uid, demo in user_demographics.items() 
                                if demo.get(protected_attribute) == group)
                group_rates[group] = len(recs) / group_users if group_users > 0 else 0.0
            
            # Find maximum bias
            rates = list(group_rates.values())
            bias_magnitude = max(rates) - min(rates) if rates else 0.0
            
            # Chi-square test for independence
            observed_counts = [len(recs) for recs in group_recommendations.values()]
            expected_counts = [np.mean(observed_counts)] * len(observed_counts)
            
            if sum(observed_counts) > 0:
                chi2, p_value = chi2_contingency([observed_counts, expected_counts])[:2]
            else:
                p_value = 1.0
            
            # Identify affected groups
            min_rate = min(group_rates.values()) if group_rates else 0.0
            affected_groups = [group for group, rate in group_rates.items() 
                             if rate <= min_rate + 0.1]
            
            return BiasDetection(
                bias_type="demographic_bias",
                affected_groups=affected_groups,
                bias_magnitude=bias_magnitude,
                confidence=1.0 - p_value,
                statistical_significance=p_value,
                description=f"Demographic bias detected for attribute '{protected_attribute}'. "
                           f"Maximum rate difference: {bias_magnitude:.3f}",
                mitigation_suggestions=[
                    "Implement demographic parity constraints",
                    "Use fairness-aware ranking algorithms",
                    "Balance training data across groups",
                    "Apply post-processing fairness corrections"
                ]
            )
            
        except Exception as e:
            logger.error(f"Demographic bias detection failed: {e}")
            return BiasDetection(
                bias_type="demographic_bias",
                affected_groups=[],
                bias_magnitude=0.0,
                confidence=0.0,
                statistical_significance=1.0,
                description="Bias detection failed",
                mitigation_suggestions=[]
            )
    
    def detect_filter_bubble_bias(self, recommendations: Dict[str, List[str]],
                                user_profiles: Dict[str, Dict[str, Any]],
                                item_features: Dict[str, Dict[str, Any]]) -> BiasDetection:
        """Detect filter bubble bias (lack of diversity in recommendations)."""
        try:
            diversity_scores = []
            
            for user_id, recs in recommendations.items():
                if not recs:
                    continue
                
                # Calculate diversity within user's recommendations
                categories = []
                for item in recs:
                    if item in item_features:
                        category = item_features[item].get('category', 'unknown')
                        categories.append(category)
                
                if categories:
                    # Calculate entropy as diversity measure
                    category_counts = Counter(categories)
                    total_items = len(categories)
                    probabilities = [count / total_items for count in category_counts.values()]
                    diversity = entropy(probabilities)
                    diversity_scores.append(diversity)
            
            avg_diversity = np.mean(diversity_scores) if diversity_scores else 0.0
            
            # Maximum possible diversity (uniform distribution)
            max_possible_diversity = np.log(len(set(
                item_features[item].get('category', 'unknown') 
                for item in item_features.keys()
            )))
            
            # Bias magnitude as deviation from maximum diversity
            bias_magnitude = max(0.0, max_possible_diversity - avg_diversity) / max_possible_diversity \
                           if max_possible_diversity > 0 else 0.0
            
            return BiasDetection(
                bias_type="filter_bubble_bias",
                affected_groups=["all_users"],
                bias_magnitude=bias_magnitude,
                confidence=0.8,  # Medium confidence for heuristic measure
                statistical_significance=0.1,  # Not statistical test
                description=f"Filter bubble detected. Average diversity: {avg_diversity:.3f}, "
                           f"Maximum possible: {max_possible_diversity:.3f}",
                mitigation_suggestions=[
                    "Increase recommendation diversity",
                    "Implement exploration components",
                    "Use multi-objective optimization",
                    "Add serendipity boosting"
                ]
            )
            
        except Exception as e:
            logger.error(f"Filter bubble bias detection failed: {e}")
            return BiasDetection(
                bias_type="filter_bubble_bias",
                affected_groups=[],
                bias_magnitude=0.0,
                confidence=0.0,
                statistical_significance=1.0,
                description="Bias detection failed",
                mitigation_suggestions=[]
            )

class DiversityFairnessManager:
    """Main manager for diversity and fairness in recommendations."""
    
    def __init__(self, config: DiversityConfig = None):
        self.config = config or DiversityConfig()
        
        # Initialize components
        self.diversity_calculator = None
        self.fairness_analyzer = None
        self.bias_detector = None
        
        # Data storage
        self.item_features: Dict[str, Dict[str, Any]] = {}
        self.user_demographics: Dict[str, Dict[str, Any]] = {}
        self.historical_recommendations: Dict[str, List[List[str]]] = defaultdict(list)
        
        # Metrics cache
        self.diversity_cache: Dict[str, DiversityMetrics] = {}
        self.fairness_cache: Dict[str, FairnessMetrics] = {}
        
        logger.info("DiversityFairnessManager initialized")
    
    def initialize(self, item_features: Dict[str, Dict[str, Any]],
                  user_demographics: Dict[str, Dict[str, Any]]):
        """Initialize with item and user data."""
        self.item_features = item_features
        self.user_demographics = user_demographics
        
        # Initialize components
        self.diversity_calculator = DiversityCalculator(item_features)
        
        if self.config.protected_attributes:
            self.fairness_analyzer = FairnessAnalyzer(self.config.protected_attributes)
        
        self.bias_detector = BiasDetector()
        
        logger.info(f"Initialized with {len(item_features)} items and {len(user_demographics)} users")
    
    async def enhance_recommendations_for_diversity(self, 
                                                  original_recommendations: Dict[str, List[Tuple[str, float]]],
                                                  user_profiles: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
        """Enhance recommendations to improve diversity."""
        enhanced_recommendations = {}
        
        for user_id, scored_items in original_recommendations.items():
            try:
                # Apply diversity enhancement
                enhanced_list = await self._apply_diversity_enhancement(
                    user_id, scored_items, user_profiles.get(user_id, {})
                )
                enhanced_recommendations[user_id] = enhanced_list
                
            except Exception as e:
                logger.error(f"Diversity enhancement failed for user {user_id}: {e}")
                # Fallback to original recommendations
                enhanced_recommendations[user_id] = [item for item, _ in scored_items]
        
        return enhanced_recommendations
    
    async def _apply_diversity_enhancement(self, user_id: str, 
                                         scored_items: List[Tuple[str, float]],
                                         user_profile: Dict[str, Any]) -> List[str]:
        """Apply diversity enhancement to a single user's recommendations."""
        if not self.config.enable_diversity_boost:
            return [item for item, _ in scored_items[:self.config.max_recommendations]]
        
        try:
            # Sort by original relevance score
            sorted_items = sorted(scored_items, key=lambda x: x[1], reverse=True)
            
            # Start with highest-relevance item
            selected_items = [sorted_items[0][0]] if sorted_items else []
            candidate_items = sorted_items[1:]
            
            # Greedy selection for diversity
            while (len(selected_items) < self.config.max_recommendations and 
                   candidate_items):
                
                best_item = None
                best_score = -1
                
                for item, relevance in candidate_items:
                    # Calculate diversity score
                    diversity_score = self._calculate_marginal_diversity(
                        item, selected_items
                    )
                    
                    # Combined score: relevance + diversity
                    combined_score = (
                        (1 - self.config.diversity_lambda) * relevance +
                        self.config.diversity_lambda * diversity_score
                    )
                    
                    if combined_score > best_score:
                        best_score = combined_score
                        best_item = item
                
                if best_item:
                    selected_items.append(best_item)
                    candidate_items = [(i, s) for i, s in candidate_items if i != best_item]
                else:
                    break
            
            # Ensure minimum category representation
            selected_items = self._ensure_category_diversity(selected_items, candidate_items)
            
            return selected_items[:self.config.max_recommendations]
            
        except Exception as e:
            logger.error(f"Diversity enhancement failed: {e}")
            return [item for item, _ in scored_items[:self.config.max_recommendations]]
    
    def _calculate_marginal_diversity(self, candidate_item: str, 
                                    selected_items: List[str]) -> float:
        """Calculate marginal diversity contribution of adding candidate item."""
        if not selected_items:
            return 1.0
        
        # Calculate average dissimilarity with selected items
        dissimilarities = []
        for selected_item in selected_items:
            dissim = self.diversity_calculator._calculate_item_dissimilarity(
                candidate_item, selected_item
            )
            dissimilarities.append(dissim)
        
        return np.mean(dissimilarities) if dissimilarities else 1.0
    
    def _ensure_category_diversity(self, selected_items: List[str],
                                 candidate_items: List[Tuple[str, float]]) -> List[str]:
        """Ensure minimum representation of different categories."""
        if not self.config.min_category_representation:
            return selected_items
        
        # Count categories in selected items
        selected_categories = Counter()
        for item in selected_items:
            if item in self.item_features:
                category = self.item_features[item].get('category', 'unknown')
                selected_categories[category] += 1
        
        total_selected = len(selected_items)
        min_per_category = int(total_selected * self.config.min_category_representation)
        
        # Add items from underrepresented categories
        for category, count in selected_categories.items():
            if count < min_per_category:
                needed = min_per_category - count
                
                # Find candidate items from this category
                category_candidates = [
                    (item, score) for item, score in candidate_items
                    if (item in self.item_features and 
                        self.item_features[item].get('category') == category)
                ]
                
                # Add top candidates from this category
                category_candidates.sort(key=lambda x: x[1], reverse=True)
                for item, _ in category_candidates[:needed]:
                    if len(selected_items) < self.config.max_recommendations:
                        selected_items.append(item)
        
        return selected_items
    
    async def calculate_diversity_metrics(self, recommendations: Dict[str, List[str]],
                                        user_profiles: Dict[str, Dict[str, Any]],
                                        global_popularity: Dict[str, float]) -> DiversityMetrics:
        """Calculate comprehensive diversity metrics."""
        diversity_computations.inc()
        
        try:
            all_recommendations = []
            for recs in recommendations.values():
                all_recommendations.extend(recs)
            
            # Calculate metrics
            intra_diversity_scores = []
            novelty_scores = []
            serendipity_scores = []
            category_distributions = []
            
            for user_id, recs in recommendations.items():
                if not recs:
                    continue
                
                # Intra-list diversity
                intra_div = self.diversity_calculator.calculate_intra_list_diversity(recs)
                intra_diversity_scores.append(intra_div)
                
                # Novelty
                user_profile = user_profiles.get(user_id, {})
                user_history = user_profile.get('interaction_history', [])
                novelty = self.diversity_calculator.calculate_novelty(
                    recs, user_history, global_popularity
                )
                novelty_scores.append(novelty)
                
                # Serendipity
                # Dummy relevance scores for serendipity calculation
                relevance_scores = {item: 0.8 for item in recs}  # Assume high relevance
                serendipity = self.diversity_calculator.calculate_serendipity(
                    recs, user_profile, relevance_scores
                )
                serendipity_scores.append(serendipity)
                
                # Category distribution
                cat_dist = self.diversity_calculator.calculate_category_distribution(recs)
                category_distributions.append(cat_dist)
            
            # Aggregate metrics
            avg_intra_diversity = np.mean(intra_diversity_scores) if intra_diversity_scores else 0.0
            
            # Coverage
            unique_items = len(set(all_recommendations))
            total_catalog = len(self.item_features)
            coverage = unique_items / total_catalog if total_catalog > 0 else 0.0
            
            avg_novelty = np.mean(novelty_scores) if novelty_scores else 0.0
            avg_serendipity = np.mean(serendipity_scores) if serendipity_scores else 0.0
            
            # Aggregate category distribution
            all_categories = set()
            for cat_dist in category_distributions:
                all_categories.update(cat_dist.keys())
            
            aggregated_category_dist = {}
            for category in all_categories:
                category_scores = [
                    dist.get(category, 0.0) for dist in category_distributions
                ]
                aggregated_category_dist[category] = np.mean(category_scores)
            
            # Temporal diversity (if historical data available)
            temporal_diversity = 0.0
            if self.historical_recommendations:
                temporal_scores = []
                for user_id in recommendations.keys():
                    if user_id in self.historical_recommendations:
                        user_history = self.historical_recommendations[user_id]
                        current_recs = recommendations[user_id]
                        temp_div = self.diversity_calculator.calculate_temporal_diversity(
                            user_history + [current_recs]
                        )
                        temporal_scores.append(temp_div)
                
                temporal_diversity = np.mean(temporal_scores) if temporal_scores else 0.0
            
            diversity_metrics = DiversityMetrics(
                intra_list_diversity=avg_intra_diversity,
                coverage=coverage,
                novelty=avg_novelty,
                serendipity=avg_serendipity,
                category_distribution=aggregated_category_dist,
                temporal_diversity=temporal_diversity
            )
            
            # Record metrics
            diversity_scores.observe(avg_intra_diversity)
            
            return diversity_metrics
            
        except Exception as e:
            logger.error(f"Diversity metrics calculation failed: {e}")
            return DiversityMetrics(
                intra_list_diversity=0.0,
                coverage=0.0,
                novelty=0.0,
                serendipity=0.0,
                category_distribution={},
                temporal_diversity=0.0
            )
    
    async def calculate_fairness_metrics(self, recommendations: Dict[str, List[str]],
                                       true_preferences: Dict[str, List[str]]) -> FairnessMetrics:
        """Calculate comprehensive fairness metrics."""
        if not self.fairness_analyzer:
            return FairnessMetrics(
                demographic_parity={},
                equalized_odds={},
                calibration={},
                individual_fairness=0.0,
                group_fairness={}
            )
        
        try:
            # Demographic parity
            demographic_parity = self.fairness_analyzer.analyze_demographic_parity(
                recommendations, self.user_demographics
            )
            
            # Equalized odds
            equalized_odds = self.fairness_analyzer.analyze_equalized_odds(
                recommendations, true_preferences, self.user_demographics
            )
            
            # Individual fairness (simplified calculation)
            # TODO: Implement proper user similarity calculation
            individual_fairness = 0.8  # Placeholder
            
            # Record metrics
            for attr, score in demographic_parity.items():
                fairness_scores.observe(score)
            
            return FairnessMetrics(
                demographic_parity=demographic_parity,
                equalized_odds=equalized_odds,
                calibration={},  # TODO: Implement calibration analysis
                individual_fairness=individual_fairness,
                group_fairness={}  # TODO: Implement group fairness analysis
            )
            
        except Exception as e:
            logger.error(f"Fairness metrics calculation failed: {e}")
            return FairnessMetrics(
                demographic_parity={},
                equalized_odds={},
                calibration={},
                individual_fairness=0.0,
                group_fairness={}
            )
    
    async def detect_biases(self, recommendations: Dict[str, List[str]],
                          item_popularity: Dict[str, float]) -> List[BiasDetection]:
        """Detect various types of bias in recommendations."""
        if not self.bias_detector:
            return []
        
        detected_biases = []
        
        try:
            # Popularity bias
            popularity_bias = self.bias_detector.detect_popularity_bias(
                recommendations, item_popularity
            )
            if popularity_bias.bias_magnitude > self.config.bias_detection_threshold:
                detected_biases.append(popularity_bias)
                bias_detections.labels(bias_type='popularity').inc()
            
            # Demographic bias
            if self.config.protected_attributes:
                for attr in self.config.protected_attributes:
                    demographic_bias = self.bias_detector.detect_demographic_bias(
                        recommendations, self.user_demographics, attr
                    )
                    if demographic_bias.bias_magnitude > self.config.bias_detection_threshold:
                        detected_biases.append(demographic_bias)
                        bias_detections.labels(bias_type='demographic').inc()
            
            # Filter bubble bias
            filter_bubble_bias = self.bias_detector.detect_filter_bubble_bias(
                recommendations, self.user_demographics, self.item_features
            )
            if filter_bubble_bias.bias_magnitude > self.config.bias_detection_threshold:
                detected_biases.append(filter_bubble_bias)
                bias_detections.labels(bias_type='filter_bubble').inc()
            
            return detected_biases
            
        except Exception as e:
            logger.error(f"Bias detection failed: {e}")
            return []
    
    def update_historical_recommendations(self, user_id: str, recommendations: List[str]):
        """Update historical recommendations for temporal analysis."""
        self.historical_recommendations[user_id].append(recommendations)
        
        # Keep only recent history (last 10 recommendation sets)
        if len(self.historical_recommendations[user_id]) > 10:
            self.historical_recommendations[user_id] = self.historical_recommendations[user_id][-10:]
    
    def get_diversity_fairness_report(self, recommendations: Dict[str, List[str]],
                                    user_profiles: Dict[str, Dict[str, Any]],
                                    true_preferences: Dict[str, List[str]],
                                    item_popularity: Dict[str, float]) -> Dict[str, Any]:
        """Generate comprehensive diversity and fairness report."""
        # This would be called asynchronously in practice
        import asyncio
        
        async def generate_report():
            diversity_metrics = await self.calculate_diversity_metrics(
                recommendations, user_profiles, item_popularity
            )
            
            fairness_metrics = await self.calculate_fairness_metrics(
                recommendations, true_preferences
            )
            
            detected_biases = await self.detect_biases(
                recommendations, item_popularity
            )
            
            return {
                'diversity_metrics': asdict(diversity_metrics),
                'fairness_metrics': asdict(fairness_metrics),
                'detected_biases': [asdict(bias) for bias in detected_biases],
                'recommendations_analyzed': len(recommendations),
                'total_recommended_items': sum(len(recs) for recs in recommendations.values()),
                'analysis_timestamp': datetime.now().isoformat()
            }
        
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            report = loop.run_until_complete(generate_report())
            return report
        finally:
            loop.close()
