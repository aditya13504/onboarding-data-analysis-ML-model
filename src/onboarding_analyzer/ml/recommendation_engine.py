"""Advanced recommendation algorithms with multiple strategies and ensemble methods.

Provides production-grade recommendation engine with collaborative filtering, content-based,
and hybrid approaches.
"""
from __future__ import annotations
import json
import logging
import asyncio
import time
import pickle
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.neighbors import NearestNeighbors
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.ml.feature_engineering import feature_pipeline

logger = logging.getLogger(__name__)

class RecommendationStrategy(Enum):
    COLLABORATIVE_FILTERING = "collaborative_filtering"
    CONTENT_BASED = "content_based"
    MATRIX_FACTORIZATION = "matrix_factorization"
    DEEP_LEARNING = "deep_learning"
    HYBRID = "hybrid"
    POPULARITY_BASED = "popularity_based"
    KNOWLEDGE_BASED = "knowledge_based"

class SimilarityMetric(Enum):
    COSINE = "cosine"
    EUCLIDEAN = "euclidean"
    PEARSON = "pearson"
    JACCARD = "jaccard"
    MANHATTAN = "manhattan"

class RecommendationType(Enum):
    USER_BASED = "user_based"          # Recommend items to users
    ITEM_BASED = "item_based"          # Recommend similar items
    SESSION_BASED = "session_based"    # Recommend next actions in session
    CONTENT_BASED = "content_based"    # Recommend based on content similarity

@dataclass
class Recommendation:
    item_id: str
    score: float
    confidence: float
    strategy: RecommendationStrategy
    explanation: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass
class RecommendationRequest:
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    item_id: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    num_recommendations: int = 10
    strategy: Optional[RecommendationStrategy] = None
    exclude_items: Set[str] = field(default_factory=set)
    include_explanations: bool = True

@dataclass
class UserProfile:
    user_id: str
    preferences: Dict[str, float]
    interaction_history: List[Dict[str, Any]]
    demographic_features: Dict[str, Any]
    behavioral_features: Dict[str, float]
    created_at: datetime
    updated_at: datetime

@dataclass
class ItemProfile:
    item_id: str
    features: Dict[str, Any]
    content_vector: Optional[np.ndarray] = None
    popularity_score: float = 0.0
    category: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)

# Metrics
RECOMMENDATION_REQUESTS = Counter('recommendation_requests_total', 'Recommendation requests', ['strategy', 'type'])
RECOMMENDATION_LATENCY = Histogram('recommendation_latency_seconds', 'Recommendation latency', ['strategy'])
RECOMMENDATION_QUALITY = Gauge('recommendation_quality_score', 'Recommendation quality', ['strategy'])
MODEL_TRAINING_TIME = Histogram('model_training_seconds', 'Model training time', ['algorithm'])
CACHE_HIT_RATE = Gauge('recommendation_cache_hit_rate', 'Cache hit rate', ['strategy'])

class CollaborativeFilteringEngine:
    """User-item collaborative filtering with multiple similarity metrics."""
    
    def __init__(self, similarity_metric: SimilarityMetric = SimilarityMetric.COSINE):
        self.similarity_metric = similarity_metric
        self.user_item_matrix: Optional[pd.DataFrame] = None
        self.user_similarity_matrix: Optional[np.ndarray] = None
        self.item_similarity_matrix: Optional[np.ndarray] = None
        self.trained = False
        
    def train(self, interactions: List[Dict[str, Any]]):
        """Train collaborative filtering model."""
        start_time = time.time()
        
        try:
            # Create user-item interaction matrix
            df = pd.DataFrame(interactions)
            
            # Pivot to create user-item matrix
            self.user_item_matrix = df.pivot_table(
                index='user_id',
                columns='item_id',
                values='rating',
                fill_value=0
            )
            
            # Compute similarity matrices
            if self.similarity_metric == SimilarityMetric.COSINE:
                self.user_similarity_matrix = cosine_similarity(self.user_item_matrix)
                self.item_similarity_matrix = cosine_similarity(self.user_item_matrix.T)
            elif self.similarity_metric == SimilarityMetric.PEARSON:
                self.user_similarity_matrix = np.corrcoef(self.user_item_matrix)
                self.item_similarity_matrix = np.corrcoef(self.user_item_matrix.T)
            
            self.trained = True
            
            training_time = time.time() - start_time
            MODEL_TRAINING_TIME.labels(algorithm='collaborative_filtering').observe(training_time)
            
            logger.info(f"Collaborative filtering model trained in {training_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Collaborative filtering training failed: {e}")
            raise
    
    def recommend_user_based(self, user_id: str, num_recommendations: int = 10) -> List[Recommendation]:
        """Generate user-based collaborative filtering recommendations."""
        if not self.trained or user_id not in self.user_item_matrix.index:
            return []
        
        user_idx = self.user_item_matrix.index.get_loc(user_id)
        user_similarities = self.user_similarity_matrix[user_idx]
        
        # Find similar users
        similar_users = np.argsort(user_similarities)[::-1][1:51]  # Top 50 similar users
        
        # Get recommendations based on similar users' preferences
        recommendations = []
        user_ratings = self.user_item_matrix.iloc[user_idx]
        
        for item_id in self.user_item_matrix.columns:
            if user_ratings[item_id] > 0:  # Skip items user already interacted with
                continue
            
            # Calculate predicted rating
            numerator = 0
            denominator = 0
            
            for similar_user_idx in similar_users:
                similarity = user_similarities[similar_user_idx]
                rating = self.user_item_matrix.iloc[similar_user_idx][item_id]
                
                if rating > 0 and similarity > 0:
                    numerator += similarity * rating
                    denominator += similarity
            
            if denominator > 0:
                predicted_rating = numerator / denominator
                confidence = min(denominator / 10.0, 1.0)  # Normalize confidence
                
                recommendations.append(Recommendation(
                    item_id=item_id,
                    score=predicted_rating,
                    confidence=confidence,
                    strategy=RecommendationStrategy.COLLABORATIVE_FILTERING,
                    explanation=f"Recommended based on similar users' preferences"
                ))
        
        # Sort by score and return top recommendations
        recommendations.sort(key=lambda x: x.score, reverse=True)
        return recommendations[:num_recommendations]
    
    def recommend_item_based(self, user_id: str, num_recommendations: int = 10) -> List[Recommendation]:
        """Generate item-based collaborative filtering recommendations."""
        if not self.trained or user_id not in self.user_item_matrix.index:
            return []
        
        user_ratings = self.user_item_matrix.loc[user_id]
        user_items = user_ratings[user_ratings > 0].index.tolist()
        
        recommendations = defaultdict(float)
        item_weights = defaultdict(float)
        
        for item_id in user_items:
            if item_id not in self.user_item_matrix.columns:
                continue
                
            item_idx = self.user_item_matrix.columns.get_loc(item_id)
            item_similarities = self.item_similarity_matrix[item_idx]
            
            for idx, similarity in enumerate(item_similarities):
                target_item = self.user_item_matrix.columns[idx]
                
                if target_item != item_id and user_ratings[target_item] == 0:
                    recommendations[target_item] += similarity * user_ratings[item_id]
                    item_weights[target_item] += abs(similarity)
        
        # Normalize recommendations
        final_recommendations = []
        for item_id, score in recommendations.items():
            if item_weights[item_id] > 0:
                normalized_score = score / item_weights[item_id]
                confidence = min(item_weights[item_id] / 5.0, 1.0)
                
                final_recommendations.append(Recommendation(
                    item_id=item_id,
                    score=normalized_score,
                    confidence=confidence,
                    strategy=RecommendationStrategy.COLLABORATIVE_FILTERING,
                    explanation=f"Recommended based on similar items you liked"
                ))
        
        final_recommendations.sort(key=lambda x: x.score, reverse=True)
        return final_recommendations[:num_recommendations]

class ContentBasedEngine:
    """Content-based recommendation using item features and user preferences."""
    
    def __init__(self):
        self.item_profiles: Dict[str, ItemProfile] = {}
        self.user_profiles: Dict[str, UserProfile] = {}
        self.content_vectorizer: Optional[TfidfVectorizer] = None
        self.content_matrix: Optional[np.ndarray] = None
        self.trained = False
    
    def add_item_profile(self, item_profile: ItemProfile):
        """Add item profile to the engine."""
        self.item_profiles[item_profile.item_id] = item_profile
    
    def add_user_profile(self, user_profile: UserProfile):
        """Add user profile to the engine."""
        self.user_profiles[user_profile.user_id] = user_profile
    
    def train(self, items: List[Dict[str, Any]]):
        """Train content-based model."""
        start_time = time.time()
        
        try:
            # Create item profiles
            for item_data in items:
                item_profile = ItemProfile(
                    item_id=item_data['item_id'],
                    features=item_data.get('features', {}),
                    category=item_data.get('category'),
                    tags=item_data.get('tags', [])
                )
                self.add_item_profile(item_profile)
            
            # Build content vectors
            self._build_content_vectors()
            
            self.trained = True
            
            training_time = time.time() - start_time
            MODEL_TRAINING_TIME.labels(algorithm='content_based').observe(training_time)
            
            logger.info(f"Content-based model trained in {training_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Content-based training failed: {e}")
            raise
    
    def _build_content_vectors(self):
        """Build content vectors for items."""
        # Combine textual features
        item_texts = []
        item_ids = []
        
        for item_id, profile in self.item_profiles.items():
            text_features = []
            
            # Add category
            if profile.category:
                text_features.append(profile.category)
            
            # Add tags
            text_features.extend(profile.tags)
            
            # Add other textual features
            for key, value in profile.features.items():
                if isinstance(value, str):
                    text_features.append(value)
            
            item_texts.append(' '.join(text_features))
            item_ids.append(item_id)
        
        # Vectorize content
        if item_texts:
            self.content_vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words='english',
                ngram_range=(1, 2)
            )
            self.content_matrix = self.content_vectorizer.fit_transform(item_texts)
            
            # Store content vectors in item profiles
            for idx, item_id in enumerate(item_ids):
                self.item_profiles[item_id].content_vector = self.content_matrix[idx].toarray().flatten()
    
    def build_user_profile(self, user_id: str, interactions: List[Dict[str, Any]]) -> UserProfile:
        """Build user profile from interaction history."""
        preferences = defaultdict(float)
        interaction_count = 0
        
        for interaction in interactions:
            if interaction.get('user_id') == user_id:
                item_id = interaction.get('item_id')
                rating = interaction.get('rating', 1.0)
                
                item_profile = self.item_profiles.get(item_id)
                if item_profile:
                    # Aggregate preferences by category
                    if item_profile.category:
                        preferences[f"category_{item_profile.category}"] += rating
                    
                    # Aggregate preferences by tags
                    for tag in item_profile.tags:
                        preferences[f"tag_{tag}"] += rating
                    
                    interaction_count += 1
        
        # Normalize preferences
        if interaction_count > 0:
            for key in preferences:
                preferences[key] /= interaction_count
        
        user_profile = UserProfile(
            user_id=user_id,
            preferences=dict(preferences),
            interaction_history=interactions,
            demographic_features={},
            behavioral_features={},
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        self.add_user_profile(user_profile)
        return user_profile
    
    def recommend(self, user_id: str, num_recommendations: int = 10) -> List[Recommendation]:
        """Generate content-based recommendations."""
        if not self.trained or user_id not in self.user_profiles:
            return []
        
        user_profile = self.user_profiles[user_id]
        recommendations = []
        
        # Get user's interacted items
        interacted_items = {
            interaction['item_id'] 
            for interaction in user_profile.interaction_history
            if interaction.get('user_id') == user_id
        }
        
        # Calculate scores for all items
        for item_id, item_profile in self.item_profiles.items():
            if item_id in interacted_items:
                continue
            
            score = self._calculate_content_score(user_profile, item_profile)
            
            if score > 0:
                recommendations.append(Recommendation(
                    item_id=item_id,
                    score=score,
                    confidence=0.8,  # Content-based usually has good confidence
                    strategy=RecommendationStrategy.CONTENT_BASED,
                    explanation=self._generate_content_explanation(user_profile, item_profile)
                ))
        
        recommendations.sort(key=lambda x: x.score, reverse=True)
        return recommendations[:num_recommendations]
    
    def _calculate_content_score(self, user_profile: UserProfile, item_profile: ItemProfile) -> float:
        """Calculate content-based score between user and item."""
        score = 0.0
        
        # Category preference
        if item_profile.category:
            category_key = f"category_{item_profile.category}"
            score += user_profile.preferences.get(category_key, 0.0)
        
        # Tag preferences
        for tag in item_profile.tags:
            tag_key = f"tag_{tag}"
            score += user_profile.preferences.get(tag_key, 0.0)
        
        return score
    
    def _generate_content_explanation(self, user_profile: UserProfile, item_profile: ItemProfile) -> str:
        """Generate explanation for content-based recommendation."""
        explanations = []
        
        if item_profile.category:
            category_key = f"category_{item_profile.category}"
            if user_profile.preferences.get(category_key, 0) > 0:
                explanations.append(f"you like {item_profile.category}")
        
        high_pref_tags = [
            tag for tag in item_profile.tags
            if user_profile.preferences.get(f"tag_{tag}", 0) > 0.5
        ]
        
        if high_pref_tags:
            explanations.append(f"you're interested in {', '.join(high_pref_tags[:3])}")
        
        if explanations:
            return f"Recommended because {' and '.join(explanations)}"
        else:
            return "Recommended based on your content preferences"

class MatrixFactorizationEngine:
    """Matrix factorization using SVD for dimensionality reduction."""
    
    def __init__(self, n_components: int = 50):
        self.n_components = n_components
        self.svd_model: Optional[TruncatedSVD] = None
        self.user_factors: Optional[np.ndarray] = None
        self.item_factors: Optional[np.ndarray] = None
        self.user_mapping: Dict[str, int] = {}
        self.item_mapping: Dict[str, int] = {}
        self.trained = False
    
    def train(self, interactions: List[Dict[str, Any]]):
        """Train matrix factorization model."""
        start_time = time.time()
        
        try:
            # Create user-item matrix
            df = pd.DataFrame(interactions)
            user_item_matrix = df.pivot_table(
                index='user_id',
                columns='item_id',
                values='rating',
                fill_value=0
            )
            
            # Create mappings
            self.user_mapping = {user: idx for idx, user in enumerate(user_item_matrix.index)}
            self.item_mapping = {item: idx for idx, item in enumerate(user_item_matrix.columns)}
            
            # Apply SVD
            self.svd_model = TruncatedSVD(n_components=self.n_components, random_state=42)
            user_item_reduced = self.svd_model.fit_transform(user_item_matrix)
            
            # Extract factors
            self.user_factors = user_item_reduced
            self.item_factors = self.svd_model.components_.T
            
            self.trained = True
            
            training_time = time.time() - start_time
            MODEL_TRAINING_TIME.labels(algorithm='matrix_factorization').observe(training_time)
            
            logger.info(f"Matrix factorization model trained in {training_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Matrix factorization training failed: {e}")
            raise
    
    def recommend(self, user_id: str, num_recommendations: int = 10, 
                 exclude_items: Set[str] = None) -> List[Recommendation]:
        """Generate matrix factorization recommendations."""
        if not self.trained or user_id not in self.user_mapping:
            return []
        
        if exclude_items is None:
            exclude_items = set()
        
        user_idx = self.user_mapping[user_id]
        user_vector = self.user_factors[user_idx]
        
        # Calculate scores for all items
        item_scores = np.dot(self.item_factors, user_vector)
        
        # Create recommendations
        recommendations = []
        for item_id, item_idx in self.item_mapping.items():
            if item_id not in exclude_items:
                score = item_scores[item_idx]
                
                recommendations.append(Recommendation(
                    item_id=item_id,
                    score=float(score),
                    confidence=0.7,
                    strategy=RecommendationStrategy.MATRIX_FACTORIZATION,
                    explanation="Recommended based on latent factor analysis"
                ))
        
        recommendations.sort(key=lambda x: x.score, reverse=True)
        return recommendations[:num_recommendations]

class HybridRecommendationEngine:
    """Hybrid recommendation engine combining multiple strategies."""
    
    def __init__(self):
        self.collaborative_engine = CollaborativeFilteringEngine()
        self.content_engine = ContentBasedEngine()
        self.matrix_factorization_engine = MatrixFactorizationEngine()
        self.strategy_weights = {
            RecommendationStrategy.COLLABORATIVE_FILTERING: 0.4,
            RecommendationStrategy.CONTENT_BASED: 0.3,
            RecommendationStrategy.MATRIX_FACTORIZATION: 0.3
        }
        self.recommendation_cache: Dict[str, Tuple[List[Recommendation], datetime]] = {}
        self.cache_ttl = timedelta(minutes=15)
    
    def train(self, interactions: List[Dict[str, Any]], items: List[Dict[str, Any]]):
        """Train all recommendation engines."""
        start_time = time.time()
        
        try:
            # Train collaborative filtering
            self.collaborative_engine.train(interactions)
            
            # Train content-based
            self.content_engine.train(items)
            
            # Build user profiles for content-based
            users = set(interaction['user_id'] for interaction in interactions)
            for user_id in users:
                user_interactions = [
                    interaction for interaction in interactions
                    if interaction['user_id'] == user_id
                ]
                self.content_engine.build_user_profile(user_id, user_interactions)
            
            # Train matrix factorization
            self.matrix_factorization_engine.train(interactions)
            
            training_time = time.time() - start_time
            logger.info(f"Hybrid recommendation engine trained in {training_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Hybrid engine training failed: {e}")
            raise
    
    async def recommend(self, request: RecommendationRequest) -> List[Recommendation]:
        """Generate hybrid recommendations."""
        start_time = time.time()
        
        try:
            RECOMMENDATION_REQUESTS.labels(
                strategy='hybrid',
                type='user_based' if request.user_id else 'item_based'
            ).inc()
            
            # Check cache
            cache_key = self._generate_cache_key(request)
            cached_result = self._get_cached_recommendations(cache_key)
            if cached_result:
                CACHE_HIT_RATE.labels(strategy='hybrid').set(1.0)
                return cached_result
            
            CACHE_HIT_RATE.labels(strategy='hybrid').set(0.0)
            
            # Get recommendations from each strategy
            all_recommendations = []
            
            if request.user_id:
                # Collaborative filtering
                cf_recs = self.collaborative_engine.recommend_user_based(
                    request.user_id, request.num_recommendations * 2
                )
                all_recommendations.extend(cf_recs)
                
                # Content-based
                content_recs = self.content_engine.recommend(
                    request.user_id, request.num_recommendations * 2
                )
                all_recommendations.extend(content_recs)
                
                # Matrix factorization
                mf_recs = self.matrix_factorization_engine.recommend(
                    request.user_id, request.num_recommendations * 2, request.exclude_items
                )
                all_recommendations.extend(mf_recs)
            
            # Combine recommendations using weighted scoring
            combined_recommendations = self._combine_recommendations(all_recommendations)
            
            # Filter and sort
            final_recommendations = [
                rec for rec in combined_recommendations
                if rec.item_id not in request.exclude_items
            ][:request.num_recommendations]
            
            # Cache results
            self._cache_recommendations(cache_key, final_recommendations)
            
            latency = time.time() - start_time
            RECOMMENDATION_LATENCY.labels(strategy='hybrid').observe(latency)
            
            return final_recommendations
            
        except Exception as e:
            logger.error(f"Hybrid recommendation failed: {e}")
            return []
    
    def _combine_recommendations(self, all_recommendations: List[Recommendation]) -> List[Recommendation]:
        """Combine recommendations from multiple strategies."""
        item_scores = defaultdict(list)
        item_strategies = defaultdict(set)
        item_explanations = defaultdict(list)
        
        # Group recommendations by item
        for rec in all_recommendations:
            weight = self.strategy_weights.get(rec.strategy, 0.1)
            weighted_score = rec.score * weight * rec.confidence
            
            item_scores[rec.item_id].append(weighted_score)
            item_strategies[rec.item_id].add(rec.strategy)
            item_explanations[rec.item_id].append(rec.explanation)
        
        # Calculate final scores
        combined_recommendations = []
        for item_id, scores in item_scores.items():
            final_score = sum(scores) / len(scores)  # Average weighted score
            
            # Calculate confidence based on strategy agreement
            strategy_count = len(item_strategies[item_id])
            confidence = min(strategy_count / 3.0, 1.0)  # Higher if multiple strategies agree
            
            # Create combined explanation
            strategies_used = list(item_strategies[item_id])
            if len(strategies_used) == 1:
                explanation = item_explanations[item_id][0]
            else:
                explanation = f"Recommended by {len(strategies_used)} different algorithms"
            
            combined_recommendations.append(Recommendation(
                item_id=item_id,
                score=final_score,
                confidence=confidence,
                strategy=RecommendationStrategy.HYBRID,
                explanation=explanation,
                metadata={
                    'strategies_used': [s.value for s in strategies_used],
                    'individual_scores': scores
                }
            ))
        
        combined_recommendations.sort(key=lambda x: x.score, reverse=True)
        return combined_recommendations
    
    def _generate_cache_key(self, request: RecommendationRequest) -> str:
        """Generate cache key for request."""
        key_parts = [
            request.user_id or "",
            request.session_id or "",
            request.item_id or "",
            str(request.num_recommendations),
            str(sorted(request.exclude_items))
        ]
        return hashlib.md5("_".join(key_parts).encode()).hexdigest()
    
    def _get_cached_recommendations(self, cache_key: str) -> Optional[List[Recommendation]]:
        """Get cached recommendations if valid."""
        if cache_key in self.recommendation_cache:
            recommendations, timestamp = self.recommendation_cache[cache_key]
            if datetime.utcnow() - timestamp < self.cache_ttl:
                return recommendations
        return None
    
    def _cache_recommendations(self, cache_key: str, recommendations: List[Recommendation]):
        """Cache recommendations."""
        self.recommendation_cache[cache_key] = (recommendations, datetime.utcnow())
        
        # Clean up old cache entries
        if len(self.recommendation_cache) > 1000:
            cutoff_time = datetime.utcnow() - self.cache_ttl
            expired_keys = [
                key for key, (_, timestamp) in self.recommendation_cache.items()
                if timestamp < cutoff_time
            ]
            for key in expired_keys:
                del self.recommendation_cache[key]
    
    def get_engine_stats(self) -> Dict[str, Any]:
        """Get comprehensive engine statistics."""
        return {
            "collaborative_filtering": {
                "trained": self.collaborative_engine.trained,
                "users": len(self.collaborative_engine.user_item_matrix.index) if self.collaborative_engine.user_item_matrix is not None else 0,
                "items": len(self.collaborative_engine.user_item_matrix.columns) if self.collaborative_engine.user_item_matrix is not None else 0
            },
            "content_based": {
                "trained": self.content_engine.trained,
                "item_profiles": len(self.content_engine.item_profiles),
                "user_profiles": len(self.content_engine.user_profiles)
            },
            "matrix_factorization": {
                "trained": self.matrix_factorization_engine.trained,
                "components": self.matrix_factorization_engine.n_components,
                "users": len(self.matrix_factorization_engine.user_mapping),
                "items": len(self.matrix_factorization_engine.item_mapping)
            },
            "cache": {
                "size": len(self.recommendation_cache),
                "ttl_minutes": self.cache_ttl.total_seconds() / 60
            },
            "strategy_weights": self.strategy_weights
        }

# Global recommendation engine
recommendation_engine = HybridRecommendationEngine()

async def get_recommendations(request: RecommendationRequest) -> List[Recommendation]:
    """Convenience function to get recommendations."""
    return await recommendation_engine.recommend(request)

def train_recommendation_models(interactions: List[Dict[str, Any]], items: List[Dict[str, Any]]):
    """Convenience function to train recommendation models."""
    recommendation_engine.train(interactions, items)
