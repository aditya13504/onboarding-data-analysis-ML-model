"""Storage tier management with automated lifecycle policies and intelligent data placement.

Manages hot, warm, and cold data tiers with automatic promotion/demotion based on access patterns.
"""
from __future__ import annotations
import json
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from collections import defaultdict
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, SessionSummary, ArchivedEvent

logger = logging.getLogger(__name__)

class StorageTier(Enum):
    HOT = "hot"        # Frequently accessed, high-performance storage
    WARM = "warm"      # Moderately accessed, balanced performance/cost
    COLD = "cold"      # Rarely accessed, low-cost storage
    ARCHIVE = "archive" # Long-term retention, lowest cost

class TieringStrategy(Enum):
    TIME_BASED = "time_based"           # Based on data age
    ACCESS_BASED = "access_based"       # Based on access frequency
    SIZE_BASED = "size_based"           # Based on data size
    HYBRID = "hybrid"                   # Combination of factors
    PREDICTIVE = "predictive"           # ML-based prediction

class DataCategory(Enum):
    RAW_EVENTS = "raw_events"
    SESSION_DATA = "session_data"
    AGGREGATED_DATA = "aggregated_data"
    ANALYTICS_DATA = "analytics_data"
    ARCHIVE_DATA = "archive_data"
    BACKUP_DATA = "backup_data"

@dataclass
class TierConfig:
    tier: StorageTier
    storage_class: str  # Storage backend class (e.g., "ssd", "hdd", "s3_standard")
    cost_per_gb_month: float
    iops_limit: int
    bandwidth_mbps: int
    availability_sla: float
    durability_nines: int
    max_size_gb: Optional[int] = None

@dataclass
class LifecycleRule:
    name: str
    data_category: DataCategory
    strategy: TieringStrategy
    rules: List[Dict[str, Any]]
    enabled: bool = True
    priority: int = 100

@dataclass
class DataAccessPattern:
    table_name: str
    partition_key: str
    access_count: int = 0
    last_access: Optional[datetime] = None
    total_size_bytes: int = 0
    average_query_time: float = 0.0
    read_frequency: float = 0.0  # accesses per day
    write_frequency: float = 0.0
    cost_score: float = 0.0  # Cost efficiency score

@dataclass
class TieringRecommendation:
    table_name: str
    partition_key: str
    current_tier: StorageTier
    recommended_tier: StorageTier
    confidence_score: float
    reason: str
    potential_savings: float
    estimated_performance_impact: float

@dataclass
class TierMigration:
    id: str
    table_name: str
    partition_key: str
    source_tier: StorageTier
    target_tier: StorageTier
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "pending"
    data_size_bytes: int = 0
    migration_duration: Optional[float] = None
    error_message: Optional[str] = None

# Metrics
TIER_DATA_SIZE = Gauge('storage_tier_size_bytes', 'Data size by tier', ['tier', 'category'])
TIER_MIGRATIONS = Counter('storage_tier_migrations_total', 'Tier migrations', ['source', 'target', 'result'])
TIER_MIGRATION_DURATION = Histogram('storage_tier_migration_seconds', 'Migration duration', ['source', 'target'])
TIER_COST_SAVINGS = Gauge('storage_tier_cost_savings_monthly', 'Monthly cost savings', ['category'])
DATA_ACCESS_FREQUENCY = Gauge('data_access_frequency_daily', 'Daily access frequency', ['table', 'tier'])
TIER_PERFORMANCE_SCORE = Gauge('storage_tier_performance_score', 'Performance score by tier', ['tier'])

class StorageTierManager:
    """Manages automated storage tiering with intelligent data placement."""
    
    def __init__(self):
        self.tier_configs = self._initialize_tier_configs()
        self.lifecycle_rules = self._load_lifecycle_rules()
        self.access_patterns: Dict[str, DataAccessPattern] = {}
        self.active_migrations: Dict[str, TierMigration] = {}
        self.cost_tracking: Dict[StorageTier, float] = defaultdict(float)
        
    def _initialize_tier_configs(self) -> Dict[StorageTier, TierConfig]:
        """Initialize storage tier configurations."""
        return {
            StorageTier.HOT: TierConfig(
                tier=StorageTier.HOT,
                storage_class="nvme_ssd",
                cost_per_gb_month=0.50,
                iops_limit=10000,
                bandwidth_mbps=1000,
                availability_sla=0.9999,
                durability_nines=11,
                max_size_gb=1000
            ),
            StorageTier.WARM: TierConfig(
                tier=StorageTier.WARM,
                storage_class="ssd",
                cost_per_gb_month=0.25,
                iops_limit=3000,
                bandwidth_mbps=500,
                availability_sla=0.999,
                durability_nines=11,
                max_size_gb=5000
            ),
            StorageTier.COLD: TierConfig(
                tier=StorageTier.COLD,
                storage_class="hdd",
                cost_per_gb_month=0.10,
                iops_limit=500,
                bandwidth_mbps=100,
                availability_sla=0.99,
                durability_nines=9
            ),
            StorageTier.ARCHIVE: TierConfig(
                tier=StorageTier.ARCHIVE,
                storage_class="glacier",
                cost_per_gb_month=0.01,
                iops_limit=50,
                bandwidth_mbps=10,
                availability_sla=0.95,
                durability_nines=12
            )
        }
    
    def _load_lifecycle_rules(self) -> List[LifecycleRule]:
        """Load data lifecycle rules."""
        return [
            LifecycleRule(
                name="raw_events_aging",
                data_category=DataCategory.RAW_EVENTS,
                strategy=TieringStrategy.TIME_BASED,
                rules=[
                    {"condition": "age_days <= 7", "tier": "hot"},
                    {"condition": "age_days <= 30", "tier": "warm"},
                    {"condition": "age_days <= 90", "tier": "cold"},
                    {"condition": "age_days > 90", "tier": "archive"}
                ],
                priority=10
            ),
            LifecycleRule(
                name="session_data_access_based",
                data_category=DataCategory.SESSION_DATA,
                strategy=TieringStrategy.ACCESS_BASED,
                rules=[
                    {"condition": "access_frequency > 10", "tier": "hot"},
                    {"condition": "access_frequency > 1", "tier": "warm"},
                    {"condition": "access_frequency > 0.1", "tier": "cold"},
                    {"condition": "access_frequency <= 0.1", "tier": "archive"}
                ],
                priority=20
            ),
            LifecycleRule(
                name="analytics_hybrid_tiering",
                data_category=DataCategory.ANALYTICS_DATA,
                strategy=TieringStrategy.HYBRID,
                rules=[
                    {
                        "condition": "age_days <= 14 AND access_frequency > 5",
                        "tier": "hot"
                    },
                    {
                        "condition": "age_days <= 60 AND access_frequency > 1",
                        "tier": "warm"
                    },
                    {
                        "condition": "age_days <= 180",
                        "tier": "cold"
                    },
                    {
                        "condition": "age_days > 180",
                        "tier": "archive"
                    }
                ],
                priority=15
            ),
            LifecycleRule(
                name="large_data_size_based",
                data_category=DataCategory.RAW_EVENTS,
                strategy=TieringStrategy.SIZE_BASED,
                rules=[
                    {"condition": "size_gb > 100 AND age_days > 3", "tier": "cold"},
                    {"condition": "size_gb > 500", "tier": "archive"}
                ],
                priority=30
            )
        ]
    
    async def analyze_access_patterns(self) -> Dict[str, DataAccessPattern]:
        """Analyze data access patterns to inform tiering decisions."""
        try:
            with SessionLocal() as session:
                # Analyze raw events access patterns
                await self._analyze_table_patterns(session, "raw_events", DataCategory.RAW_EVENTS)
                
                # Analyze session summaries
                await self._analyze_table_patterns(session, "session_summaries", DataCategory.SESSION_DATA)
                
                # Analyze archived events
                await self._analyze_table_patterns(session, "archived_events", DataCategory.ARCHIVE_DATA)
                
                logger.info(f"Analyzed access patterns for {len(self.access_patterns)} data partitions")
                return self.access_patterns
                
        except Exception as e:
            logger.error(f"Failed to analyze access patterns: {e}")
            return {}
    
    async def _analyze_table_patterns(self, session, table_name: str, category: DataCategory):
        """Analyze access patterns for a specific table."""
        try:
            # This would typically analyze query logs, but for demo we'll simulate
            # In production, integrate with query log analysis or monitoring tools
            
            # Simulate partition analysis
            if table_name == "raw_events":
                partitions = self._get_time_partitions(table_name, session)
            else:
                partitions = [f"{table_name}_main"]
            
            for partition in partitions:
                pattern_key = f"{table_name}_{partition}"
                
                # Simulate access pattern data
                pattern = DataAccessPattern(
                    table_name=table_name,
                    partition_key=partition,
                    access_count=self._simulate_access_count(partition),
                    last_access=self._simulate_last_access(partition),
                    total_size_bytes=self._simulate_partition_size(partition),
                    average_query_time=self._simulate_query_time(partition),
                    read_frequency=self._simulate_read_frequency(partition),
                    write_frequency=self._simulate_write_frequency(partition)
                )
                
                pattern.cost_score = self._calculate_cost_score(pattern)
                self.access_patterns[pattern_key] = pattern
                
                # Update metrics
                current_tier = self._get_current_tier(table_name, partition)
                DATA_ACCESS_FREQUENCY.labels(
                    table=table_name, 
                    tier=current_tier.value
                ).set(pattern.read_frequency)
                
        except Exception as e:
            logger.error(f"Failed to analyze patterns for {table_name}: {e}")
    
    def _get_time_partitions(self, table_name: str, session) -> List[str]:
        """Get time-based partitions for a table."""
        # Simulate partition discovery
        # In production, query information_schema or pg_partitions
        now = datetime.utcnow()
        partitions = []
        
        for days_back in range(0, 100, 7):  # Weekly partitions for ~3 months
            partition_date = now - timedelta(days=days_back)
            partition_name = f"{partition_date.strftime('%Y%m%d')}"
            partitions.append(partition_name)
        
        return partitions
    
    def _simulate_access_count(self, partition: str) -> int:
        """Simulate access count based on partition age."""
        if "raw_events" in partition:
            # Newer partitions have more access
            if any(recent in partition for recent in ["20241201", "20241124"]):
                return 1000 + hash(partition) % 500
            elif any(medium in partition for medium in ["20241117", "20241110"]):
                return 100 + hash(partition) % 200
            else:
                return hash(partition) % 50
        return hash(partition) % 100
    
    def _simulate_last_access(self, partition: str) -> datetime:
        """Simulate last access time."""
        if "raw_events" in partition:
            if any(recent in partition for recent in ["20241201", "20241124"]):
                return datetime.utcnow() - timedelta(hours=hash(partition) % 24)
            else:
                return datetime.utcnow() - timedelta(days=hash(partition) % 30)
        return datetime.utcnow() - timedelta(days=hash(partition) % 7)
    
    def _simulate_partition_size(self, partition: str) -> int:
        """Simulate partition size in bytes."""
        base_size = 1024 * 1024 * 1024  # 1GB base
        return base_size + (hash(partition) % (5 * base_size))
    
    def _simulate_query_time(self, partition: str) -> float:
        """Simulate average query time."""
        return 0.1 + (hash(partition) % 100) / 1000.0
    
    def _simulate_read_frequency(self, partition: str) -> float:
        """Simulate read frequency (accesses per day)."""
        access_count = self._simulate_access_count(partition)
        return access_count / 7.0  # Convert to daily frequency
    
    def _simulate_write_frequency(self, partition: str) -> float:
        """Simulate write frequency."""
        if "raw_events" in partition:
            # Recent partitions have more writes
            if any(recent in partition for recent in ["20241201", "20241124"]):
                return 100.0 + hash(partition) % 50
            else:
                return hash(partition) % 10
        return hash(partition) % 5
    
    def _calculate_cost_score(self, pattern: DataAccessPattern) -> float:
        """Calculate cost efficiency score for data placement."""
        # Higher score = better cost efficiency
        size_gb = pattern.total_size_bytes / (1024**3)
        
        # Cost per access
        cost_per_access = size_gb * 0.1 / max(pattern.read_frequency, 0.1)
        
        # Performance factor
        performance_factor = 1.0 / max(pattern.average_query_time, 0.01)
        
        # Access frequency factor
        frequency_factor = min(pattern.read_frequency / 10.0, 1.0)
        
        return performance_factor * frequency_factor / cost_per_access
    
    def _get_current_tier(self, table_name: str, partition: str) -> StorageTier:
        """Get current storage tier for data."""
        # Simulate current tier detection
        # In production, query storage configuration or metadata
        if "20241201" in partition or "20241124" in partition:
            return StorageTier.HOT
        elif "20241117" in partition or "20241110" in partition:
            return StorageTier.WARM
        else:
            return StorageTier.COLD
    
    async def generate_tiering_recommendations(self) -> List[TieringRecommendation]:
        """Generate intelligent tiering recommendations."""
        recommendations = []
        
        try:
            # Ensure we have current access patterns
            if not self.access_patterns:
                await self.analyze_access_patterns()
            
            for pattern_key, pattern in self.access_patterns.items():
                current_tier = self._get_current_tier(pattern.table_name, pattern.partition_key)
                recommended_tier = self._recommend_tier(pattern)
                
                if current_tier != recommended_tier:
                    confidence = self._calculate_recommendation_confidence(pattern, current_tier, recommended_tier)
                    
                    if confidence > 0.7:  # Only recommend if confident
                        savings = self._calculate_potential_savings(pattern, current_tier, recommended_tier)
                        performance_impact = self._estimate_performance_impact(current_tier, recommended_tier)
                        
                        recommendation = TieringRecommendation(
                            table_name=pattern.table_name,
                            partition_key=pattern.partition_key,
                            current_tier=current_tier,
                            recommended_tier=recommended_tier,
                            confidence_score=confidence,
                            reason=self._get_recommendation_reason(pattern, recommended_tier),
                            potential_savings=savings,
                            estimated_performance_impact=performance_impact
                        )
                        
                        recommendations.append(recommendation)
            
            # Sort by potential savings
            recommendations.sort(key=lambda x: x.potential_savings, reverse=True)
            
            logger.info(f"Generated {len(recommendations)} tiering recommendations")
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate recommendations: {e}")
            return []
    
    def _recommend_tier(self, pattern: DataAccessPattern) -> StorageTier:
        """Recommend storage tier based on access patterns and rules."""
        # Apply lifecycle rules
        for rule in sorted(self.lifecycle_rules, key=lambda x: x.priority):
            if not rule.enabled:
                continue
            
            if self._pattern_matches_category(pattern, rule.data_category):
                tier = self._evaluate_rule(pattern, rule)
                if tier:
                    return tier
        
        # Fallback to default logic
        return self._default_tier_recommendation(pattern)
    
    def _pattern_matches_category(self, pattern: DataAccessPattern, category: DataCategory) -> bool:
        """Check if access pattern matches data category."""
        table_mapping = {
            DataCategory.RAW_EVENTS: ["raw_events"],
            DataCategory.SESSION_DATA: ["session_summaries"],
            DataCategory.ANALYTICS_DATA: ["analytics", "aggregated"],
            DataCategory.ARCHIVE_DATA: ["archived_events"],
        }
        
        category_tables = table_mapping.get(category, [])
        return any(table in pattern.table_name for table in category_tables)
    
    def _evaluate_rule(self, pattern: DataAccessPattern, rule: LifecycleRule) -> Optional[StorageTier]:
        """Evaluate a lifecycle rule against access pattern."""
        for rule_condition in rule.rules:
            if self._evaluate_condition(pattern, rule_condition["condition"]):
                tier_name = rule_condition["tier"]
                return StorageTier(tier_name)
        return None
    
    def _evaluate_condition(self, pattern: DataAccessPattern, condition: str) -> bool:
        """Evaluate a rule condition."""
        try:
            # Calculate values for condition evaluation
            age_days = (datetime.utcnow() - (pattern.last_access or datetime.utcnow())).days
            access_frequency = pattern.read_frequency
            size_gb = pattern.total_size_bytes / (1024**3)
            
            # Replace variables in condition
            condition = condition.replace("age_days", str(age_days))
            condition = condition.replace("access_frequency", str(access_frequency))
            condition = condition.replace("size_gb", str(size_gb))
            
            # Safe evaluation (in production, use a proper expression evaluator)
            allowed_names = {
                "__builtins__": {},
                "True": True,
                "False": False,
                "AND": " and ",
                "OR": " or ",
            }
            
            # Simple condition evaluation
            condition = condition.replace(" AND ", " and ").replace(" OR ", " or ")
            
            return eval(condition, allowed_names)
            
        except Exception as e:
            logger.warning(f"Failed to evaluate condition '{condition}': {e}")
            return False
    
    def _default_tier_recommendation(self, pattern: DataAccessPattern) -> StorageTier:
        """Default tier recommendation logic."""
        # High frequency access -> Hot
        if pattern.read_frequency > 10:
            return StorageTier.HOT
        
        # Medium frequency -> Warm
        elif pattern.read_frequency > 1:
            return StorageTier.WARM
        
        # Low frequency -> Cold
        elif pattern.read_frequency > 0.1:
            return StorageTier.COLD
        
        # Very low frequency -> Archive
        else:
            return StorageTier.ARCHIVE
    
    def _calculate_recommendation_confidence(self, pattern: DataAccessPattern, 
                                           current_tier: StorageTier, 
                                           recommended_tier: StorageTier) -> float:
        """Calculate confidence score for recommendation."""
        confidence = 0.5  # Base confidence
        
        # Higher confidence for clear access patterns
        if pattern.access_count > 100:
            confidence += 0.2
        
        # Higher confidence for significant tier differences
        tier_distance = abs(list(StorageTier).index(current_tier) - 
                           list(StorageTier).index(recommended_tier))
        confidence += tier_distance * 0.1
        
        # Higher confidence for data with clear cost benefit
        if pattern.cost_score > 0.8:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _calculate_potential_savings(self, pattern: DataAccessPattern, 
                                   current_tier: StorageTier, 
                                   recommended_tier: StorageTier) -> float:
        """Calculate potential monthly cost savings."""
        size_gb = pattern.total_size_bytes / (1024**3)
        
        current_cost = size_gb * self.tier_configs[current_tier].cost_per_gb_month
        new_cost = size_gb * self.tier_configs[recommended_tier].cost_per_gb_month
        
        return max(0, current_cost - new_cost)
    
    def _estimate_performance_impact(self, current_tier: StorageTier, 
                                   recommended_tier: StorageTier) -> float:
        """Estimate performance impact of tier change."""
        current_iops = self.tier_configs[current_tier].iops_limit
        new_iops = self.tier_configs[recommended_tier].iops_limit
        
        # Return percentage change in performance
        return (new_iops - current_iops) / current_iops
    
    def _get_recommendation_reason(self, pattern: DataAccessPattern, 
                                 recommended_tier: StorageTier) -> str:
        """Get human-readable reason for recommendation."""
        if recommended_tier == StorageTier.HOT:
            return f"High access frequency ({pattern.read_frequency:.1f}/day) warrants hot storage"
        elif recommended_tier == StorageTier.WARM:
            return f"Moderate access frequency ({pattern.read_frequency:.1f}/day) suits warm storage"
        elif recommended_tier == StorageTier.COLD:
            return f"Low access frequency ({pattern.read_frequency:.1f}/day) suggests cold storage"
        else:
            return f"Very low access frequency ({pattern.read_frequency:.1f}/day) indicates archive storage"
    
    async def execute_tier_migration(self, recommendation: TieringRecommendation) -> str:
        """Execute a tier migration based on recommendation."""
        migration_id = f"migration_{recommendation.table_name}_{recommendation.partition_key}_{int(time.time())}"
        
        migration = TierMigration(
            id=migration_id,
            table_name=recommendation.table_name,
            partition_key=recommendation.partition_key,
            source_tier=recommendation.current_tier,
            target_tier=recommendation.recommended_tier,
            started_at=datetime.utcnow(),
            data_size_bytes=self.access_patterns[f"{recommendation.table_name}_{recommendation.partition_key}"].total_size_bytes
        )
        
        self.active_migrations[migration_id] = migration
        
        try:
            TIER_MIGRATIONS.labels(
                source=recommendation.current_tier.value,
                target=recommendation.recommended_tier.value,
                result='started'
            ).inc()
            
            start_time = time.time()
            
            # Execute the migration
            migration.status = "migrating"
            success = await self._perform_tier_migration(migration)
            
            migration_duration = time.time() - start_time
            migration.migration_duration = migration_duration
            
            if success:
                migration.status = "completed"
                migration.completed_at = datetime.utcnow()
                
                TIER_MIGRATIONS.labels(
                    source=recommendation.current_tier.value,
                    target=recommendation.recommended_tier.value,
                    result='success'
                ).inc()
                
                TIER_MIGRATION_DURATION.labels(
                    source=recommendation.current_tier.value,
                    target=recommendation.recommended_tier.value
                ).observe(migration_duration)
                
                # Update cost savings
                TIER_COST_SAVINGS.labels(
                    category=self._get_data_category(recommendation.table_name).value
                ).inc(recommendation.potential_savings)
                
                logger.info(f"Migration {migration_id} completed successfully in {migration_duration:.2f}s")
            else:
                migration.status = "failed"
                TIER_MIGRATIONS.labels(
                    source=recommendation.current_tier.value,
                    target=recommendation.recommended_tier.value,
                    result='failure'
                ).inc()
                
        except Exception as e:
            migration.status = "failed"
            migration.error_message = str(e)
            TIER_MIGRATIONS.labels(
                source=recommendation.current_tier.value,
                target=recommendation.recommended_tier.value,
                result='error'
            ).inc()
            logger.error(f"Migration {migration_id} failed: {e}")
        
        return migration_id
    
    async def _perform_tier_migration(self, migration: TierMigration) -> bool:
        """Perform the actual tier migration."""
        try:
            logger.info(f"Migrating {migration.table_name}.{migration.partition_key} "
                       f"from {migration.source_tier.value} to {migration.target_tier.value}")
            
            # Simulate migration time based on data size and tier change
            size_gb = migration.data_size_bytes / (1024**3)
            
            # Calculate migration time (simplified)
            base_time = size_gb * 0.1  # 0.1 seconds per GB base
            
            # Add tier-specific factors
            if migration.target_tier == StorageTier.ARCHIVE:
                base_time *= 2  # Archives take longer
            elif migration.source_tier == StorageTier.ARCHIVE:
                base_time *= 3  # Restoring from archive takes longer
            
            await asyncio.sleep(min(base_time, 10))  # Cap at 10 seconds for demo
            
            # In production, this would:
            # 1. Copy data to new storage tier
            # 2. Update metadata/configuration
            # 3. Verify data integrity
            # 4. Update partition routing
            # 5. Clean up old storage
            
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
    
    def _get_data_category(self, table_name: str) -> DataCategory:
        """Get data category for a table."""
        if "raw_events" in table_name:
            return DataCategory.RAW_EVENTS
        elif "session" in table_name:
            return DataCategory.SESSION_DATA
        elif "archived" in table_name:
            return DataCategory.ARCHIVE_DATA
        else:
            return DataCategory.ANALYTICS_DATA
    
    def get_tier_statistics(self) -> Dict[str, Any]:
        """Get comprehensive tier statistics."""
        stats = {
            "tiers": {},
            "total_data_size_gb": 0,
            "total_monthly_cost": 0,
            "migrations": {
                "active": len([m for m in self.active_migrations.values() if m.status in ["pending", "migrating"]]),
                "completed": len([m for m in self.active_migrations.values() if m.status == "completed"]),
                "failed": len([m for m in self.active_migrations.values() if m.status == "failed"])
            }
        }
        
        # Calculate tier statistics
        tier_data = defaultdict(lambda: {"size_gb": 0, "partitions": 0, "cost": 0})
        
        for pattern in self.access_patterns.values():
            tier = self._get_current_tier(pattern.table_name, pattern.partition_key)
            size_gb = pattern.total_size_bytes / (1024**3)
            
            tier_data[tier]["size_gb"] += size_gb
            tier_data[tier]["partitions"] += 1
            tier_data[tier]["cost"] += size_gb * self.tier_configs[tier].cost_per_gb_month
        
        for tier, data in tier_data.items():
            stats["tiers"][tier.value] = {
                "size_gb": round(data["size_gb"], 2),
                "partitions": data["partitions"],
                "monthly_cost": round(data["cost"], 2),
                "config": {
                    "storage_class": self.tier_configs[tier].storage_class,
                    "cost_per_gb": self.tier_configs[tier].cost_per_gb_month,
                    "iops_limit": self.tier_configs[tier].iops_limit,
                    "availability_sla": self.tier_configs[tier].availability_sla
                }
            }
            
            stats["total_data_size_gb"] += data["size_gb"]
            stats["total_monthly_cost"] += data["cost"]
        
        stats["total_data_size_gb"] = round(stats["total_data_size_gb"], 2)
        stats["total_monthly_cost"] = round(stats["total_monthly_cost"], 2)
        
        return stats
    
    def get_migration_status(self, migration_id: str) -> Optional[TierMigration]:
        """Get status of a specific migration."""
        return self.active_migrations.get(migration_id)

# Global storage tier manager
tier_manager = StorageTierManager()

async def analyze_and_recommend_tiering() -> List[TieringRecommendation]:
    """Convenience function to analyze and generate recommendations."""
    await tier_manager.analyze_access_patterns()
    return await tier_manager.generate_tiering_recommendations()

async def execute_tiering_recommendation(recommendation: TieringRecommendation) -> str:
    """Convenience function to execute a tiering recommendation."""
    return await tier_manager.execute_tier_migration(recommendation)
