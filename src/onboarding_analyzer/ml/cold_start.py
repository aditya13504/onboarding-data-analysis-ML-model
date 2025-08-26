"""
Cold Start Problem Solutions for Recommendation Systems

Handles new users and items without historical data through various strategies
including content-based filtering, demographic-based recommendations, and hybrid approaches.
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
from collections import defaultdict
import random

# Sklearn for clustering and similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
cold_start_requests = Counter('recommendation_cold_start_total', 'Total cold start requests', ['user_type', 'strategy'])
cold_start_latency = Histogram('recommendation_cold_start_duration_seconds', 'Cold start recommendation time')
cold_start_success = Counter('recommendation_cold_start_success_total', 'Successful cold start recommendations')
active_cold_start_models = Gauge('recommendation_cold_start_models', 'Number of active cold start models')

logger = logging.getLogger(__name__)

@dataclass
class UserProfile:
    """User profile for cold start scenarios."""
    user_id: str
    demographics: Dict[str, Any]
    preferences: Dict[str, float]
    explicit_feedback: Dict[str, float]
    interaction_count: int
    signup_timestamp: datetime
    last_activity: Optional[datetime]
    profile_completeness: float

@dataclass
class ItemProfile:
    """Item profile for cold start scenarios."""
    item_id: str
    content_features: Dict[str, Any]
    category: str
    tags: List[str]
    popularity_score: float
    quality_score: float
    creation_timestamp: datetime
    interaction_count: int
    content_similarity_vector: Optional[np.ndarray]

@dataclass
class ColdStartRecommendation:
    """Cold start recommendation result."""
    item_id: str
    score: float
    confidence: float
    strategy_used: str
    explanation: str
    fallback_reason: Optional[str]
    metadata: Dict[str, Any]

@dataclass
class ColdStartConfig:
    """Configuration for cold start handling."""
    new_user_threshold_days: int = 7
    new_item_threshold_days: int = 30
    min_interactions_for_warm: int = 5
    demographic_weight: float = 0.3
    content_weight: float = 0.4
    popularity_weight: float = 0.3
    max_recommendations: int = 20
    diversity_threshold: float = 0.7
    confidence_threshold: float = 0.5

class DemographicRecommender:
    """Demographic-based recommender for new users."""
    
    def __init__(self, demographic_features: List[str]):
        self.demographic_features = demographic_features
        self.demographic_clusters = {}
        self.cluster_preferences = {}
        self.scaler = StandardScaler()
        self.kmeans = None
        self.is_trained = False
    
    def train(self, user_profiles: List[UserProfile], interaction_data: pd.DataFrame):
        """Train demographic clusters and preferences."""
        try:
            # Prepare demographic data
            demographic_data = []
            user_ids = []
            
            for profile in user_profiles:
                if profile.interaction_count >= 5:  # Only use experienced users
                    demo_vector = []
                    for feature in self.demographic_features:
                        value = profile.demographics.get(feature, 0)
                        if isinstance(value, str):
                            # Simple hash for categorical features
                            value = hash(value) % 100
                        demo_vector.append(value)
                    
                    demographic_data.append(demo_vector)
                    user_ids.append(profile.user_id)
            
            if len(demographic_data) < 10:
                logger.warning("Insufficient data for demographic clustering")
                return
            
            demographic_matrix = np.array(demographic_data)
            
            # Normalize demographics
            demographic_matrix = self.scaler.fit_transform(demographic_matrix)
            
            # Cluster users by demographics
            n_clusters = min(10, len(demographic_data) // 5)
            self.kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            cluster_labels = self.kmeans.fit_predict(demographic_matrix)
            
            # Store cluster assignments
            for user_id, cluster in zip(user_ids, cluster_labels):
                self.demographic_clusters[user_id] = cluster
            
            # Calculate cluster preferences
            self.cluster_preferences = defaultdict(lambda: defaultdict(float))
            cluster_counts = defaultdict(int)
            
            for user_id, cluster in self.demographic_clusters.items():
                user_interactions = interaction_data[
                    interaction_data['user_id'] == user_id
                ]
                
                for _, interaction in user_interactions.iterrows():
                    item_id = interaction['item_id']
                    rating = interaction.get('rating', 1.0)
                    
                    self.cluster_preferences[cluster][item_id] += rating
                    cluster_counts[cluster] += 1
            
            # Normalize cluster preferences
            for cluster in self.cluster_preferences:
                if cluster_counts[cluster] > 0:
                    for item_id in self.cluster_preferences[cluster]:
                        self.cluster_preferences[cluster][item_id] /= cluster_counts[cluster]
            
            self.is_trained = True
            logger.info(f"Demographic recommender trained with {n_clusters} clusters")
            
        except Exception as e:
            logger.error(f"Demographic training failed: {e}")
            raise
    
    def predict_cluster(self, user_profile: UserProfile) -> int:
        """Predict demographic cluster for new user."""
        if not self.is_trained or self.kmeans is None:
            return 0
        
        demo_vector = []
        for feature in self.demographic_features:
            value = user_profile.demographics.get(feature, 0)
            if isinstance(value, str):
                value = hash(value) % 100
            demo_vector.append(value)
        
        demo_vector = np.array(demo_vector).reshape(1, -1)
        demo_vector = self.scaler.transform(demo_vector)
        
        return self.kmeans.predict(demo_vector)[0]
    
    def recommend(self, user_profile: UserProfile, n_recommendations: int = 10) -> List[Tuple[str, float]]:
        """Generate recommendations based on demographic cluster."""
        if not self.is_trained:
            return []
        
        try:
            cluster = self.predict_cluster(user_profile)
            cluster_prefs = self.cluster_preferences.get(cluster, {})
            
            # Sort items by cluster preference
            recommendations = sorted(
                cluster_prefs.items(),
                key=lambda x: x[1],
                reverse=True
            )[:n_recommendations]
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Demographic recommendation failed: {e}")
            return []

class ContentBasedColdStart:
    """Content-based recommender for cold start scenarios."""
    
    def __init__(self, content_features: List[str]):
        self.content_features = content_features
        self.item_vectors = {}
        self.tfidf_vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.category_similarities = {}
        self.is_trained = False
    
    def train(self, item_profiles: List[ItemProfile]):
        """Train content-based models."""
        try:
            # Prepare content vectors
            item_texts = []
            item_ids = []
            
            for profile in item_profiles:
                # Combine text features
                text_parts = []
                
                # Add category
                if profile.category:
                    text_parts.append(profile.category)
                
                # Add tags
                if profile.tags:
                    text_parts.extend(profile.tags)
                
                # Add other text features
                for feature in self.content_features:
                    value = profile.content_features.get(feature, "")
                    if isinstance(value, str) and value:
                        text_parts.append(value)
                
                item_text = " ".join(text_parts)
                item_texts.append(item_text)
                item_ids.append(profile.item_id)
            
            if not item_texts:
                logger.warning("No content data for training")
                return
            
            # Create TF-IDF vectors
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(item_texts)
            
            # Store item vectors
            for i, item_id in enumerate(item_ids):
                self.item_vectors[item_id] = tfidf_matrix[i].toarray().flatten()
            
            # Calculate category similarities
            categories = {}
            for profile in item_profiles:
                if profile.category not in categories:
                    categories[profile.category] = []
                categories[profile.category].append(profile.item_id)
            
            # Compute average similarity within categories
            for category, item_list in categories.items():
                if len(item_list) > 1:
                    category_vectors = [
                        self.item_vectors[item_id] 
                        for item_id in item_list 
                        if item_id in self.item_vectors
                    ]
                    
                    if category_vectors:
                        avg_similarity = np.mean([
                            cosine_similarity([v1], [v2])[0][0]
                            for i, v1 in enumerate(category_vectors)
                            for j, v2 in enumerate(category_vectors)
                            if i != j
                        ])
                        self.category_similarities[category] = avg_similarity
            
            self.is_trained = True
            logger.info(f"Content-based cold start trained with {len(self.item_vectors)} items")
            
        except Exception as e:
            logger.error(f"Content-based training failed: {e}")
            raise
    
    def recommend_for_new_user(self, user_preferences: Dict[str, float], 
                              item_profiles: List[ItemProfile],
                              n_recommendations: int = 10) -> List[Tuple[str, float]]:
        """Recommend items for new user based on stated preferences."""
        if not self.is_trained:
            return []
        
        try:
            # Create user preference vector
            preference_text = " ".join([
                pref for pref, score in user_preferences.items() 
                if score > 0.5
            ])
            
            if not preference_text:
                return self._recommend_popular_items(item_profiles, n_recommendations)
            
            # Transform preference text to vector
            user_vector = self.tfidf_vectorizer.transform([preference_text]).toarray().flatten()
            
            # Calculate similarities
            similarities = []
            for profile in item_profiles:
                if profile.item_id in self.item_vectors:
                    item_vector = self.item_vectors[profile.item_id]
                    similarity = cosine_similarity([user_vector], [item_vector])[0][0]
                    
                    # Boost by quality and popularity
                    boosted_score = similarity * (0.7 + 0.3 * profile.quality_score)
                    similarities.append((profile.item_id, boosted_score))
            
            # Sort and return top recommendations
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:n_recommendations]
            
        except Exception as e:
            logger.error(f"Content-based recommendation failed: {e}")
            return self._recommend_popular_items(item_profiles, n_recommendations)
    
    def recommend_for_new_item(self, new_item_profile: ItemProfile,
                              interaction_data: pd.DataFrame,
                              n_similar_items: int = 10) -> List[Tuple[str, float]]:
        """Find similar items for a new item."""
        if not self.is_trained or new_item_profile.item_id in self.item_vectors:
            return []
        
        try:
            # Create vector for new item
            text_parts = [new_item_profile.category] if new_item_profile.category else []
            text_parts.extend(new_item_profile.tags)
            
            for feature in self.content_features:
                value = new_item_profile.content_features.get(feature, "")
                if isinstance(value, str) and value:
                    text_parts.append(value)
            
            new_item_text = " ".join(text_parts)
            new_item_vector = self.tfidf_vectorizer.transform([new_item_text]).toarray().flatten()
            
            # Calculate similarities with existing items
            similarities = []
            for item_id, item_vector in self.item_vectors.items():
                similarity = cosine_similarity([new_item_vector], [item_vector])[0][0]
                similarities.append((item_id, similarity))
            
            # Sort by similarity
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:n_similar_items]
            
        except Exception as e:
            logger.error(f"New item similarity calculation failed: {e}")
            return []
    
    def _recommend_popular_items(self, item_profiles: List[ItemProfile],
                               n_recommendations: int) -> List[Tuple[str, float]]:
        """Fallback to popular items."""
        popular_items = sorted(
            [(p.item_id, p.popularity_score) for p in item_profiles],
            key=lambda x: x[1],
            reverse=True
        )
        return popular_items[:n_recommendations]

class PopularityBasedRecommender:
    """Popularity-based recommender with trending detection."""
    
    def __init__(self, time_decay_factor: float = 0.1):
        self.time_decay_factor = time_decay_factor
        self.popularity_scores = {}
        self.trending_items = {}
        self.category_popularity = {}
        self.is_trained = False
    
    def train(self, interaction_data: pd.DataFrame, item_profiles: List[ItemProfile]):
        """Train popularity models."""
        try:
            current_time = datetime.now()
            
            # Calculate time-weighted popularity
            popularity_counts = defaultdict(float)
            category_counts = defaultdict(lambda: defaultdict(float))
            
            for _, interaction in interaction_data.iterrows():
                item_id = interaction['item_id']
                timestamp = pd.to_datetime(interaction.get('timestamp', current_time))
                
                # Time decay
                days_ago = (current_time - timestamp).days
                weight = np.exp(-self.time_decay_factor * days_ago)
                
                # Rating weight
                rating_weight = interaction.get('rating', 1.0)
                
                popularity_counts[item_id] += weight * rating_weight
                
                # Category popularity
                item_profile = next((p for p in item_profiles if p.item_id == item_id), None)
                if item_profile and item_profile.category:
                    category_counts[item_profile.category][item_id] += weight * rating_weight
            
            # Normalize popularity scores
            max_popularity = max(popularity_counts.values()) if popularity_counts else 1.0
            self.popularity_scores = {
                item_id: score / max_popularity
                for item_id, score in popularity_counts.items()
            }
            
            # Calculate trending items (high recent activity)
            recent_cutoff = current_time - timedelta(days=7)
            recent_interactions = interaction_data[
                pd.to_datetime(interaction_data.get('timestamp', current_time)) >= recent_cutoff
            ]
            
            recent_counts = recent_interactions['item_id'].value_counts().to_dict()
            max_recent = max(recent_counts.values()) if recent_counts else 1.0
            
            self.trending_items = {
                item_id: count / max_recent
                for item_id, count in recent_counts.items()
            }
            
            # Category popularity
            for category, items in category_counts.items():
                max_cat_popularity = max(items.values()) if items else 1.0
                self.category_popularity[category] = {
                    item_id: score / max_cat_popularity
                    for item_id, score in items.items()
                }
            
            self.is_trained = True
            logger.info(f"Popularity recommender trained with {len(self.popularity_scores)} items")
            
        except Exception as e:
            logger.error(f"Popularity training failed: {e}")
            raise
    
    def recommend(self, category: Optional[str] = None, 
                 include_trending: bool = True,
                 n_recommendations: int = 10) -> List[Tuple[str, float]]:
        """Generate popularity-based recommendations."""
        if not self.is_trained:
            return []
        
        try:
            # Choose source based on category
            if category and category in self.category_popularity:
                popularity_source = self.category_popularity[category]
            else:
                popularity_source = self.popularity_scores
            
            recommendations = []
            
            for item_id, popularity in popularity_source.items():
                score = popularity
                
                # Boost with trending if enabled
                if include_trending and item_id in self.trending_items:
                    trending_score = self.trending_items[item_id]
                    score = 0.7 * popularity + 0.3 * trending_score
                
                recommendations.append((item_id, score))
            
            # Sort and return top recommendations
            recommendations.sort(key=lambda x: x[1], reverse=True)
            return recommendations[:n_recommendations]
            
        except Exception as e:
            logger.error(f"Popularity recommendation failed: {e}")
            return []

class ColdStartSolver:
    """Main cold start problem solver."""
    
    def __init__(self, config: ColdStartConfig = None):
        self.config = config or ColdStartConfig()
        
        # Component recommenders
        self.demographic_recommender = None
        self.content_recommender = None
        self.popularity_recommender = None
        
        # Data storage
        self.user_profiles: Dict[str, UserProfile] = {}
        self.item_profiles: Dict[str, ItemProfile] = {}
        self.interaction_data: Optional[pd.DataFrame] = None
        
        # Thread pool for async operations
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        active_cold_start_models.set(1)
        logger.info("ColdStartSolver initialized")
    
    def train(self, user_profiles: List[UserProfile], 
             item_profiles: List[ItemProfile],
             interaction_data: pd.DataFrame,
             demographic_features: List[str],
             content_features: List[str]):
        """Train all cold start components."""
        try:
            # Store data
            self.user_profiles = {p.user_id: p for p in user_profiles}
            self.item_profiles = {p.item_id: p for p in item_profiles}
            self.interaction_data = interaction_data
            
            # Initialize and train demographic recommender
            self.demographic_recommender = DemographicRecommender(demographic_features)
            self.demographic_recommender.train(user_profiles, interaction_data)
            
            # Initialize and train content-based recommender
            self.content_recommender = ContentBasedColdStart(content_features)
            self.content_recommender.train(item_profiles)
            
            # Initialize and train popularity recommender
            self.popularity_recommender = PopularityBasedRecommender()
            self.popularity_recommender.train(interaction_data, item_profiles)
            
            logger.info("All cold start components trained successfully")
            
        except Exception as e:
            logger.error(f"Cold start training failed: {e}")
            raise
    
    def is_new_user(self, user_profile: UserProfile) -> bool:
        """Check if user is considered new."""
        days_since_signup = (datetime.now() - user_profile.signup_timestamp).days
        return (days_since_signup <= self.config.new_user_threshold_days or
                user_profile.interaction_count < self.config.min_interactions_for_warm)
    
    def is_new_item(self, item_profile: ItemProfile) -> bool:
        """Check if item is considered new."""
        days_since_creation = (datetime.now() - item_profile.creation_timestamp).days
        return (days_since_creation <= self.config.new_item_threshold_days or
                item_profile.interaction_count < self.config.min_interactions_for_warm)
    
    async def recommend_for_new_user(self, user_profile: UserProfile) -> List[ColdStartRecommendation]:
        """Generate recommendations for new user."""
        with cold_start_latency.time():
            cold_start_requests.labels(user_type='new_user', strategy='hybrid').inc()
            
            try:
                recommendations = []
                
                # Strategy 1: Demographic-based recommendations
                if self.demographic_recommender and self.demographic_recommender.is_trained:
                    demo_recs = await asyncio.get_event_loop().run_in_executor(
                        self.executor,
                        self.demographic_recommender.recommend,
                        user_profile,
                        self.config.max_recommendations // 3
                    )
                    
                    for item_id, score in demo_recs:
                        recommendations.append(ColdStartRecommendation(
                            item_id=item_id,
                            score=score * self.config.demographic_weight,
                            confidence=0.6,
                            strategy_used="demographic",
                            explanation=f"Recommended based on similar users in your demographic group",
                            fallback_reason=None,
                            metadata={"demographic_score": score}
                        ))
                
                # Strategy 2: Content-based on preferences
                if (self.content_recommender and self.content_recommender.is_trained and
                    user_profile.preferences):
                    
                    content_recs = await asyncio.get_event_loop().run_in_executor(
                        self.executor,
                        self.content_recommender.recommend_for_new_user,
                        user_profile.preferences,
                        list(self.item_profiles.values()),
                        self.config.max_recommendations // 3
                    )
                    
                    for item_id, score in content_recs:
                        recommendations.append(ColdStartRecommendation(
                            item_id=item_id,
                            score=score * self.config.content_weight,
                            confidence=0.7,
                            strategy_used="content_preferences",
                            explanation=f"Matches your stated preferences",
                            fallback_reason=None,
                            metadata={"content_score": score}
                        ))
                
                # Strategy 3: Popularity-based fallback
                if self.popularity_recommender and self.popularity_recommender.is_trained:
                    # Try to use preferred category if available
                    preferred_category = None
                    if user_profile.preferences:
                        max_pref = max(user_profile.preferences.items(), key=lambda x: x[1])
                        if max_pref[1] > 0.7:
                            preferred_category = max_pref[0]
                    
                    pop_recs = await asyncio.get_event_loop().run_in_executor(
                        self.executor,
                        self.popularity_recommender.recommend,
                        preferred_category,
                        True,  # include trending
                        self.config.max_recommendations // 3
                    )
                    
                    for item_id, score in pop_recs:
                        recommendations.append(ColdStartRecommendation(
                            item_id=item_id,
                            score=score * self.config.popularity_weight,
                            confidence=0.5,
                            strategy_used="popularity",
                            explanation=f"Popular item trending among users",
                            fallback_reason=None,
                            metadata={"popularity_score": score}
                        ))
                
                # Combine and deduplicate recommendations
                final_recommendations = self._combine_recommendations(recommendations)
                
                # Apply diversity filter
                diverse_recommendations = self._apply_diversity_filter(final_recommendations)
                
                cold_start_success.inc()
                return diverse_recommendations[:self.config.max_recommendations]
                
            except Exception as e:
                logger.error(f"New user recommendation failed: {e}")
                return self._fallback_recommendations("new_user_error")
    
    async def recommend_for_new_item(self, item_profile: ItemProfile) -> List[str]:
        """Find users who might be interested in a new item."""
        try:
            cold_start_requests.labels(user_type='new_item', strategy='content_similarity').inc()
            
            if not self.content_recommender or not self.content_recommender.is_trained:
                return []
            
            # Find similar items
            similar_items = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.content_recommender.recommend_for_new_item,
                item_profile,
                self.interaction_data,
                10
            )
            
            if not similar_items:
                return []
            
            # Find users who liked similar items
            target_users = set()
            for similar_item_id, similarity_score in similar_items:
                if similarity_score > 0.5:  # Only consider highly similar items
                    similar_item_users = self.interaction_data[
                        (self.interaction_data['item_id'] == similar_item_id) &
                        (self.interaction_data.get('rating', 1.0) >= 4.0)  # High ratings only
                    ]['user_id'].tolist()
                    
                    target_users.update(similar_item_users)
            
            return list(target_users)
            
        except Exception as e:
            logger.error(f"New item user targeting failed: {e}")
            return []
    
    def _combine_recommendations(self, recommendations: List[ColdStartRecommendation]) -> List[ColdStartRecommendation]:
        """Combine recommendations from different strategies."""
        # Group by item_id
        item_scores = defaultdict(list)
        item_metadata = {}
        
        for rec in recommendations:
            item_scores[rec.item_id].append(rec)
            if rec.item_id not in item_metadata:
                item_metadata[rec.item_id] = rec
        
        # Combine scores for duplicate items
        combined_recommendations = []
        for item_id, rec_list in item_scores.items():
            if len(rec_list) == 1:
                combined_recommendations.append(rec_list[0])
            else:
                # Weighted average of scores
                total_score = sum(rec.score for rec in rec_list)
                avg_confidence = sum(rec.confidence for rec in rec_list) / len(rec_list)
                strategies = [rec.strategy_used for rec in rec_list]
                
                combined_rec = ColdStartRecommendation(
                    item_id=item_id,
                    score=total_score,
                    confidence=avg_confidence,
                    strategy_used="+".join(strategies),
                    explanation=f"Recommended by multiple strategies: {', '.join(strategies)}",
                    fallback_reason=None,
                    metadata={"combined_strategies": strategies, "component_scores": [r.score for r in rec_list]}
                )
                combined_recommendations.append(combined_rec)
        
        # Sort by combined score
        combined_recommendations.sort(key=lambda x: x.score, reverse=True)
        return combined_recommendations
    
    def _apply_diversity_filter(self, recommendations: List[ColdStartRecommendation]) -> List[ColdStartRecommendation]:
        """Apply diversity filter to avoid too similar recommendations."""
        if not recommendations:
            return recommendations
        
        diverse_recs = [recommendations[0]]  # Always include top recommendation
        
        for rec in recommendations[1:]:
            # Check diversity against already selected items
            is_diverse = True
            
            for selected_rec in diverse_recs:
                # Get item profiles
                current_item = self.item_profiles.get(rec.item_id)
                selected_item = self.item_profiles.get(selected_rec.item_id)
                
                if current_item and selected_item:
                    # Simple diversity check based on category
                    if (current_item.category == selected_item.category and
                        len([r for r in diverse_recs if 
                             self.item_profiles.get(r.item_id, {}).category == current_item.category]) >= 3):
                        is_diverse = False
                        break
            
            if is_diverse:
                diverse_recs.append(rec)
        
        return diverse_recs
    
    def _fallback_recommendations(self, reason: str) -> List[ColdStartRecommendation]:
        """Generate fallback recommendations when other strategies fail."""
        try:
            if self.popularity_recommender and self.popularity_recommender.is_trained:
                pop_recs = self.popularity_recommender.recommend(n_recommendations=10)
                
                fallback_recs = []
                for item_id, score in pop_recs:
                    fallback_recs.append(ColdStartRecommendation(
                        item_id=item_id,
                        score=score,
                        confidence=0.3,
                        strategy_used="popularity_fallback",
                        explanation="Popular item (fallback recommendation)",
                        fallback_reason=reason,
                        metadata={"is_fallback": True}
                    ))
                
                return fallback_recs
            
            return []
            
        except Exception as e:
            logger.error(f"Fallback recommendations failed: {e}")
            return []
    
    def update_user_profile(self, user_id: str, interaction_data: Dict[str, Any]):
        """Update user profile with new interaction data."""
        if user_id in self.user_profiles:
            profile = self.user_profiles[user_id]
            profile.interaction_count += 1
            profile.last_activity = datetime.now()
            
            # Update preferences based on interaction
            item_id = interaction_data.get('item_id')
            rating = interaction_data.get('rating', 1.0)
            
            if item_id in self.item_profiles:
                item_profile = self.item_profiles[item_id]
                category = item_profile.category
                
                if category:
                    if category not in profile.preferences:
                        profile.preferences[category] = 0.0
                    
                    # Update preference with learning rate
                    learning_rate = 0.1
                    profile.preferences[category] = (
                        (1 - learning_rate) * profile.preferences[category] +
                        learning_rate * (rating / 5.0)  # Normalize to 0-1
                    )
    
    def update_item_profile(self, item_id: str, interaction_data: Dict[str, Any]):
        """Update item profile with new interaction data."""
        if item_id in self.item_profiles:
            profile = self.item_profiles[item_id]
            profile.interaction_count += 1
            
            # Update popularity score
            rating = interaction_data.get('rating', 1.0)
            current_pop = profile.popularity_score
            
            # Weighted average with decay
            decay_factor = 0.9
            profile.popularity_score = (
                decay_factor * current_pop + 
                (1 - decay_factor) * (rating / 5.0)
            )
    
    def get_cold_start_statistics(self) -> Dict[str, Any]:
        """Get statistics about cold start scenarios."""
        current_time = datetime.now()
        
        new_users = sum(1 for profile in self.user_profiles.values() 
                       if self.is_new_user(profile))
        
        new_items = sum(1 for profile in self.item_profiles.values() 
                       if self.is_new_item(profile))
        
        # Calculate profile completeness distribution
        completeness_scores = [p.profile_completeness for p in self.user_profiles.values()]
        avg_completeness = np.mean(completeness_scores) if completeness_scores else 0.0
        
        return {
            'total_users': len(self.user_profiles),
            'new_users': new_users,
            'new_user_percentage': (new_users / len(self.user_profiles)) * 100 if self.user_profiles else 0,
            'total_items': len(self.item_profiles),
            'new_items': new_items,
            'new_item_percentage': (new_items / len(self.item_profiles)) * 100 if self.item_profiles else 0,
            'average_profile_completeness': avg_completeness,
            'demographic_recommender_trained': (self.demographic_recommender and 
                                               self.demographic_recommender.is_trained),
            'content_recommender_trained': (self.content_recommender and 
                                           self.content_recommender.is_trained),
            'popularity_recommender_trained': (self.popularity_recommender and 
                                              self.popularity_recommender.is_trained)
        }
