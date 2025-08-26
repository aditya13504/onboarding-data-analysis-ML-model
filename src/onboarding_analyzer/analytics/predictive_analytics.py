"""
Predictive Analytics System

Provides forecasting, predictive modeling, and future trend analysis for business metrics
using time series analysis, machine learning models, and statistical forecasting methods.
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union, Callable
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
import json
import pickle
from pathlib import Path
import warnings

# Time series and forecasting libraries
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit, cross_val_score

# Statistical forecasting
from scipy import stats
from scipy.optimize import minimize

# Suppress warnings
warnings.filterwarnings('ignore')

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
predictions_generated = Counter('analytics_predictions_generated_total', 'Predictions generated', ['model_type', 'metric'])
prediction_accuracy = Histogram('analytics_prediction_accuracy', 'Prediction accuracy scores')
model_training_time = Histogram('analytics_model_training_duration_seconds', 'Model training time')
forecast_requests = Counter('analytics_forecast_requests_total', 'Forecast requests', ['forecast_horizon'])

logger = logging.getLogger(__name__)

@dataclass
class ForecastResult:
    """Forecast result with confidence intervals."""
    metric_name: str
    forecast_values: List[float]
    forecast_dates: List[datetime]
    confidence_intervals: List[Tuple[float, float]]
    model_type: str
    accuracy_metrics: Dict[str, float]
    feature_importance: Optional[Dict[str, float]]
    forecast_horizon: int
    created_at: datetime

@dataclass
class ModelPerformance:
    """Model performance metrics."""
    model_name: str
    mae: float  # Mean Absolute Error
    mse: float  # Mean Squared Error
    rmse: float  # Root Mean Squared Error
    mape: float  # Mean Absolute Percentage Error
    r2: float   # R-squared
    cross_val_score: float
    training_time: float
    prediction_time: float

@dataclass
class PredictiveModel:
    """Predictive model container."""
    model_id: str
    model_type: str
    model: Any
    scaler: Any
    feature_names: List[str]
    target_name: str
    performance: ModelPerformance
    created_at: datetime
    last_updated: datetime
    is_trained: bool

@dataclass
class SeasonalDecomposition:
    """Seasonal decomposition result."""
    trend: pd.Series
    seasonal: pd.Series
    residual: pd.Series
    seasonal_period: int
    trend_strength: float
    seasonal_strength: float

class TimeSeriesAnalyzer:
    """Analyzes time series data for forecasting."""
    
    def __init__(self):
        self.seasonal_periods = [7, 30, 90, 365]  # Daily, monthly, quarterly, yearly
    
    def decompose_time_series(self, data: pd.Series, period: Optional[int] = None) -> SeasonalDecomposition:
        """Decompose time series into trend, seasonal, and residual components."""
        try:
            if len(data) < 20:
                raise ValueError("Insufficient data for decomposition")
            
            # Auto-detect seasonality if not provided
            if period is None:
                period = self._detect_seasonality(data)
            
            if period is None or period < 2 or period >= len(data) // 2:
                # No valid seasonality, use simple trend extraction
                trend = self._extract_trend(data)
                seasonal = pd.Series(0, index=data.index)
                residual = data - trend
                seasonal_strength = 0.0
            else:
                # Perform seasonal decomposition
                trend, seasonal, residual = self._seasonal_decompose(data, period)
                seasonal_strength = self._calculate_seasonal_strength(seasonal, residual)
            
            trend_strength = self._calculate_trend_strength(trend, residual)
            
            return SeasonalDecomposition(
                trend=trend,
                seasonal=seasonal,
                residual=residual,
                seasonal_period=period or 0,
                trend_strength=trend_strength,
                seasonal_strength=seasonal_strength
            )
            
        except Exception as e:
            logger.error(f"Time series decomposition failed: {e}")
            # Return minimal decomposition
            return SeasonalDecomposition(
                trend=data,
                seasonal=pd.Series(0, index=data.index),
                residual=pd.Series(0, index=data.index),
                seasonal_period=0,
                trend_strength=0.0,
                seasonal_strength=0.0
            )
    
    def _detect_seasonality(self, data: pd.Series) -> Optional[int]:
        """Detect seasonality period using autocorrelation."""
        try:
            max_period = min(len(data) // 3, max(self.seasonal_periods))
            best_period = None
            best_correlation = 0.0
            
            for period in range(2, max_period + 1):
                if len(data) > period * 2:
                    # Calculate autocorrelation at this lag
                    shifted = data.shift(period)
                    correlation = data.corr(shifted)
                    
                    if not np.isnan(correlation) and abs(correlation) > best_correlation:
                        best_correlation = abs(correlation)
                        best_period = period
            
            # Only return if correlation is significant
            return best_period if best_correlation > 0.3 else None
            
        except Exception as e:
            logger.error(f"Seasonality detection failed: {e}")
            return None
    
    def _extract_trend(self, data: pd.Series, window: Optional[int] = None) -> pd.Series:
        """Extract trend using moving average."""
        if window is None:
            window = max(3, len(data) // 10)
        
        return data.rolling(window=window, center=True).mean().fillna(method='bfill').fillna(method='ffill')
    
    def _seasonal_decompose(self, data: pd.Series, period: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Perform seasonal decomposition."""
        # Simple seasonal decomposition
        
        # Calculate trend using centered moving average
        if period % 2 == 0:
            # Even period - need two-step averaging
            ma1 = data.rolling(window=period, center=True).mean()
            trend = ma1.rolling(window=2, center=True).mean()
        else:
            # Odd period - single step
            trend = data.rolling(window=period, center=True).mean()
        
        # Fill missing trend values
        trend = trend.fillna(method='bfill').fillna(method='ffill')
        
        # Calculate seasonal component
        detrended = data - trend
        
        # Average seasonal pattern
        seasonal_pattern = {}
        for i in range(period):
            period_values = [detrended.iloc[j] for j in range(i, len(detrended), period) 
                           if not np.isnan(detrended.iloc[j])]
            seasonal_pattern[i] = np.mean(period_values) if period_values else 0.0
        
        # Normalize seasonal pattern (sum to zero)
        seasonal_mean = np.mean(list(seasonal_pattern.values()))
        seasonal_pattern = {k: v - seasonal_mean for k, v in seasonal_pattern.items()}
        
        # Create seasonal series
        seasonal = pd.Series(
            [seasonal_pattern[i % period] for i in range(len(data))],
            index=data.index
        )
        
        # Calculate residual
        residual = data - trend - seasonal
        
        return trend, seasonal, residual
    
    def _calculate_trend_strength(self, trend: pd.Series, residual: pd.Series) -> float:
        """Calculate strength of trend component."""
        try:
            trend_var = np.var(trend.dropna())
            residual_var = np.var(residual.dropna())
            
            if trend_var + residual_var == 0:
                return 0.0
            
            return trend_var / (trend_var + residual_var)
        except:
            return 0.0
    
    def _calculate_seasonal_strength(self, seasonal: pd.Series, residual: pd.Series) -> float:
        """Calculate strength of seasonal component."""
        try:
            seasonal_var = np.var(seasonal.dropna())
            residual_var = np.var(residual.dropna())
            
            if seasonal_var + residual_var == 0:
                return 0.0
            
            return seasonal_var / (seasonal_var + residual_var)
        except:
            return 0.0

class ForecastingEngine:
    """Main forecasting engine with multiple models."""
    
    def __init__(self):
        self.models: Dict[str, PredictiveModel] = {}
        self.time_series_analyzer = TimeSeriesAnalyzer()
        self.model_configs = self._get_model_configurations()
    
    def _get_model_configurations(self) -> Dict[str, Dict]:
        """Get configuration for different model types."""
        return {
            'linear_regression': {
                'model_class': LinearRegression,
                'params': {},
                'scaler': StandardScaler()
            },
            'ridge_regression': {
                'model_class': Ridge,
                'params': {'alpha': 1.0},
                'scaler': StandardScaler()
            },
            'random_forest': {
                'model_class': RandomForestRegressor,
                'params': {'n_estimators': 100, 'random_state': 42, 'n_jobs': -1},
                'scaler': None
            },
            'gradient_boosting': {
                'model_class': GradientBoostingRegressor,
                'params': {'n_estimators': 100, 'random_state': 42},
                'scaler': StandardScaler()
            }
        }
    
    def create_features(self, data: pd.Series, lag_features: int = 7, 
                       seasonal_features: bool = True) -> pd.DataFrame:
        """Create features for forecasting."""
        try:
            df = pd.DataFrame(index=data.index)
            df['value'] = data.values
            
            # Lag features
            for lag in range(1, lag_features + 1):
                df[f'lag_{lag}'] = data.shift(lag)
            
            # Rolling statistics
            for window in [3, 7, 14, 30]:
                if len(data) > window:
                    df[f'rolling_mean_{window}'] = data.rolling(window=window).mean()
                    df[f'rolling_std_{window}'] = data.rolling(window=window).std()
                    df[f'rolling_min_{window}'] = data.rolling(window=window).min()
                    df[f'rolling_max_{window}'] = data.rolling(window=window).max()
            
            # Time-based features
            if hasattr(data.index, 'dayofweek'):
                df['day_of_week'] = data.index.dayofweek
                df['is_weekend'] = (data.index.dayofweek >= 5).astype(int)
                df['day_of_month'] = data.index.day
                df['month'] = data.index.month
                df['quarter'] = data.index.quarter
            
            # Seasonal decomposition features
            if seasonal_features and len(data) > 20:
                try:
                    decomposition = self.time_series_analyzer.decompose_time_series(data)
                    df['trend'] = decomposition.trend
                    df['seasonal'] = decomposition.seasonal
                    
                    # Trend direction
                    df['trend_diff'] = decomposition.trend.diff()
                    df['trend_direction'] = np.sign(df['trend_diff'])
                    
                except Exception as e:
                    logger.warning(f"Seasonal feature creation failed: {e}")
            
            # Remove rows with NaN values
            df = df.dropna()
            
            return df
            
        except Exception as e:
            logger.error(f"Feature creation failed: {e}")
            return pd.DataFrame()
    
    def train_model(self, data: pd.Series, model_type: str = 'auto',
                   forecast_horizon: int = 30, test_split: float = 0.2) -> str:
        """Train a forecasting model."""
        try:
            with model_training_time.time():
                if model_type == 'auto':
                    model_type = self._select_best_model(data, forecast_horizon, test_split)
                
                # Create features
                feature_df = self.create_features(data)
                if feature_df.empty:
                    raise ValueError("Failed to create features")
                
                # Prepare target and features
                target = feature_df['value'].shift(-1).dropna()  # Predict next value
                features = feature_df[:-1]  # Remove last row to match target
                
                # Align features and target
                common_index = features.index.intersection(target.index)
                features = features.loc[common_index]
                target = target.loc[common_index]
                
                if len(features) < 10:
                    raise ValueError("Insufficient data for training")
                
                # Split data
                split_point = int(len(features) * (1 - test_split))
                train_features = features[:split_point]
                train_target = target[:split_point]
                test_features = features[split_point:]
                test_target = target[split_point:]
                
                # Get model configuration
                config = self.model_configs[model_type]
                model = config['model_class'](**config['params'])
                scaler = config['scaler']
                
                # Scale features if needed
                if scaler:
                    scaler.fit(train_features)
                    train_features_scaled = scaler.transform(train_features)
                    test_features_scaled = scaler.transform(test_features)
                else:
                    train_features_scaled = train_features.values
                    test_features_scaled = test_features.values
                
                # Train model
                model.fit(train_features_scaled, train_target)
                
                # Evaluate model
                train_pred = model.predict(train_features_scaled)
                test_pred = model.predict(test_features_scaled)
                
                # Calculate performance metrics
                performance = self._calculate_performance(
                    test_target, test_pred, train_target, train_pred, model_type
                )
                
                # Get feature importance
                feature_importance = self._get_feature_importance(model, features.columns)
                
                # Create model ID
                model_id = f"{model_type}_{data.name}_{int(datetime.now().timestamp())}"
                
                # Store model
                predictive_model = PredictiveModel(
                    model_id=model_id,
                    model_type=model_type,
                    model=model,
                    scaler=scaler,
                    feature_names=list(features.columns),
                    target_name=data.name,
                    performance=performance,
                    created_at=datetime.now(),
                    last_updated=datetime.now(),
                    is_trained=True
                )
                
                self.models[model_id] = predictive_model
                
                logger.info(f"Model {model_id} trained successfully. Performance: R² = {performance.r2:.3f}")
                return model_id
                
        except Exception as e:
            logger.error(f"Model training failed: {e}")
            raise
    
    def _select_best_model(self, data: pd.Series, forecast_horizon: int, 
                          test_split: float) -> str:
        """Select best model based on cross-validation."""
        try:
            feature_df = self.create_features(data)
            if feature_df.empty:
                return 'linear_regression'  # Fallback
            
            target = feature_df['value'].shift(-1).dropna()
            features = feature_df[:-1]
            
            common_index = features.index.intersection(target.index)
            features = features.loc[common_index]
            target = target.loc[common_index]
            
            if len(features) < 20:
                return 'linear_regression'  # Fallback for small datasets
            
            best_model = 'linear_regression'
            best_score = -np.inf
            
            # Time series cross-validation
            tscv = TimeSeriesSplit(n_splits=3)
            
            for model_type, config in self.model_configs.items():
                try:
                    model = config['model_class'](**config['params'])
                    scaler = config['scaler']
                    
                    scores = []
                    for train_idx, val_idx in tscv.split(features):
                        train_X, val_X = features.iloc[train_idx], features.iloc[val_idx]
                        train_y, val_y = target.iloc[train_idx], target.iloc[val_idx]
                        
                        if scaler:
                            scaler_temp = type(scaler)()
                            train_X_scaled = scaler_temp.fit_transform(train_X)
                            val_X_scaled = scaler_temp.transform(val_X)
                        else:
                            train_X_scaled = train_X.values
                            val_X_scaled = val_X.values
                        
                        model.fit(train_X_scaled, train_y)
                        val_pred = model.predict(val_X_scaled)
                        score = r2_score(val_y, val_pred)
                        scores.append(score)
                    
                    avg_score = np.mean(scores)
                    if avg_score > best_score:
                        best_score = avg_score
                        best_model = model_type
                        
                except Exception as e:
                    logger.warning(f"Model evaluation failed for {model_type}: {e}")
                    continue
            
            logger.info(f"Best model selected: {best_model} (score: {best_score:.3f})")
            return best_model
            
        except Exception as e:
            logger.error(f"Model selection failed: {e}")
            return 'linear_regression'
    
    def _calculate_performance(self, test_target: pd.Series, test_pred: np.ndarray,
                             train_target: pd.Series, train_pred: np.ndarray,
                             model_type: str) -> ModelPerformance:
        """Calculate model performance metrics."""
        try:
            # Basic metrics on test set
            mae = mean_absolute_error(test_target, test_pred)
            mse = mean_squared_error(test_target, test_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(test_target, test_pred)
            
            # MAPE (Mean Absolute Percentage Error)
            mape = np.mean(np.abs((test_target - test_pred) / np.maximum(np.abs(test_target), 1e-8))) * 100
            
            # Cross-validation score (simplified)
            cv_score = r2
            
            return ModelPerformance(
                model_name=model_type,
                mae=mae,
                mse=mse,
                rmse=rmse,
                mape=mape,
                r2=r2,
                cross_val_score=cv_score,
                training_time=0.0,  # Would be measured in actual implementation
                prediction_time=0.0
            )
            
        except Exception as e:
            logger.error(f"Performance calculation failed: {e}")
            return ModelPerformance(
                model_name=model_type,
                mae=0.0, mse=0.0, rmse=0.0, mape=0.0, r2=0.0,
                cross_val_score=0.0, training_time=0.0, prediction_time=0.0
            )
    
    def _get_feature_importance(self, model: Any, feature_names: List[str]) -> Optional[Dict[str, float]]:
        """Get feature importance from model."""
        try:
            if hasattr(model, 'feature_importances_'):
                # Tree-based models
                importances = model.feature_importances_
            elif hasattr(model, 'coef_'):
                # Linear models
                importances = np.abs(model.coef_)
            else:
                return None
            
            return dict(zip(feature_names, importances))
            
        except Exception as e:
            logger.warning(f"Feature importance extraction failed: {e}")
            return None
    
    def generate_forecast(self, model_id: str, horizon: int = 30,
                         confidence_level: float = 0.95) -> ForecastResult:
        """Generate forecast using trained model."""
        try:
            if model_id not in self.models:
                raise ValueError(f"Model {model_id} not found")
            
            model_obj = self.models[model_id]
            if not model_obj.is_trained:
                raise ValueError(f"Model {model_id} is not trained")
            
            forecast_requests.labels(forecast_horizon=str(horizon)).inc()
            
            # Get last known data to start forecasting from
            # This would typically come from the original data source
            # For now, we'll simulate the forecasting process
            
            forecasts = []
            confidence_intervals = []
            forecast_dates = []
            
            # Start from current time
            start_date = datetime.now()
            
            # Generate sequential forecasts
            for i in range(horizon):
                forecast_date = start_date + timedelta(days=i)
                
                # Create synthetic features for demonstration
                # In practice, these would be derived from actual recent data
                synthetic_features = self._create_synthetic_features(model_obj, i)
                
                if model_obj.scaler:
                    features_scaled = model_obj.scaler.transform([synthetic_features])
                else:
                    features_scaled = np.array([synthetic_features])
                
                # Generate point forecast
                point_forecast = model_obj.model.predict(features_scaled)[0]
                
                # Calculate confidence interval (simplified approach)
                # In practice, this would use model-specific methods
                forecast_std = abs(point_forecast) * 0.1  # 10% standard deviation
                z_score = stats.norm.ppf((1 + confidence_level) / 2)
                margin = z_score * forecast_std
                
                confidence_interval = (
                    point_forecast - margin,
                    point_forecast + margin
                )
                
                forecasts.append(point_forecast)
                confidence_intervals.append(confidence_interval)
                forecast_dates.append(forecast_date)
            
            # Calculate accuracy metrics if we have recent actual data
            accuracy_metrics = {
                'model_r2': model_obj.performance.r2,
                'model_mae': model_obj.performance.mae,
                'model_mape': model_obj.performance.mape
            }
            
            prediction_accuracy.observe(model_obj.performance.r2)
            
            predictions_generated.labels(
                model_type=model_obj.model_type,
                metric=model_obj.target_name
            ).inc()
            
            return ForecastResult(
                metric_name=model_obj.target_name,
                forecast_values=forecasts,
                forecast_dates=forecast_dates,
                confidence_intervals=confidence_intervals,
                model_type=model_obj.model_type,
                accuracy_metrics=accuracy_metrics,
                feature_importance=self._get_feature_importance(
                    model_obj.model, model_obj.feature_names
                ),
                forecast_horizon=horizon,
                created_at=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Forecast generation failed: {e}")
            raise
    
    def _create_synthetic_features(self, model_obj: PredictiveModel, step: int) -> List[float]:
        """Create synthetic features for forecasting demonstration."""
        # This is a simplified version - in practice, features would be
        # derived from actual recent data and forecasted external variables
        
        num_features = len(model_obj.feature_names)
        synthetic_features = []
        
        for i, feature_name in enumerate(model_obj.feature_names):
            if 'lag' in feature_name:
                # Lag features - use decreasing values
                value = 100.0 * np.exp(-step * 0.01) + np.random.normal(0, 5)
            elif 'rolling_mean' in feature_name:
                # Rolling means - gradual trend
                value = 100.0 + step * 0.5 + np.random.normal(0, 3)
            elif 'trend' in feature_name:
                # Trend features
                value = step * 0.3 + np.random.normal(0, 2)
            elif 'seasonal' in feature_name:
                # Seasonal features
                value = 10 * np.sin(2 * np.pi * step / 7) + np.random.normal(0, 1)
            elif 'day_of_week' in feature_name:
                # Day of week (0-6)
                value = step % 7
            elif 'is_weekend' in feature_name:
                # Weekend indicator
                value = 1 if (step % 7) >= 5 else 0
            else:
                # Default random feature
                value = np.random.normal(50, 10)
            
            synthetic_features.append(value)
        
        return synthetic_features
    
    def update_model(self, model_id: str, new_data: pd.Series):
        """Update existing model with new data."""
        try:
            if model_id not in self.models:
                raise ValueError(f"Model {model_id} not found")
            
            # For simplicity, retrain the model with new data
            # In practice, this could use incremental learning
            model_obj = self.models[model_id]
            model_type = model_obj.model_type
            
            # Retrain model
            new_model_id = self.train_model(new_data, model_type)
            
            # Update the existing model entry
            self.models[model_id] = self.models[new_model_id]
            self.models[model_id].model_id = model_id
            self.models[model_id].last_updated = datetime.now()
            
            # Clean up temporary model
            del self.models[new_model_id]
            
            logger.info(f"Model {model_id} updated successfully")
            
        except Exception as e:
            logger.error(f"Model update failed: {e}")
            raise
    
    def get_model_info(self, model_id: str) -> Dict[str, Any]:
        """Get information about a trained model."""
        if model_id not in self.models:
            return {}
        
        model_obj = self.models[model_id]
        return {
            'model_id': model_obj.model_id,
            'model_type': model_obj.model_type,
            'target_name': model_obj.target_name,
            'feature_names': model_obj.feature_names,
            'performance': asdict(model_obj.performance),
            'created_at': model_obj.created_at.isoformat(),
            'last_updated': model_obj.last_updated.isoformat(),
            'is_trained': model_obj.is_trained
        }
    
    def list_models(self) -> List[Dict[str, Any]]:
        """List all trained models."""
        return [self.get_model_info(model_id) for model_id in self.models.keys()]
    
    def delete_model(self, model_id: str):
        """Delete a trained model."""
        if model_id in self.models:
            del self.models[model_id]
            logger.info(f"Model {model_id} deleted")
        else:
            logger.warning(f"Model {model_id} not found for deletion")

class BusinessMetricsPredictor:
    """Specialized predictor for business metrics."""
    
    def __init__(self, forecasting_engine: ForecastingEngine):
        self.forecasting_engine = forecasting_engine
        self.business_models: Dict[str, str] = {}  # metric -> model_id mapping
        
    def setup_business_metric_prediction(self, metric_data: Dict[str, pd.Series]):
        """Setup prediction models for key business metrics."""
        try:
            key_metrics = [
                'conversion_rate', 'revenue', 'user_acquisition',
                'churn_rate', 'engagement_score', 'customer_lifetime_value'
            ]
            
            for metric_name, data in metric_data.items():
                if any(key in metric_name.lower() for key in key_metrics):
                    if len(data) >= 30:  # Minimum data requirement
                        try:
                            model_id = self.forecasting_engine.train_model(
                                data, model_type='auto', forecast_horizon=30
                            )
                            self.business_models[metric_name] = model_id
                            logger.info(f"Business model created for {metric_name}")
                        except Exception as e:
                            logger.error(f"Failed to create model for {metric_name}: {e}")
                    else:
                        logger.warning(f"Insufficient data for {metric_name}: {len(data)} points")
            
            logger.info(f"Setup complete for {len(self.business_models)} business metrics")
            
        except Exception as e:
            logger.error(f"Business metrics setup failed: {e}")
    
    def predict_business_outcomes(self, horizon_days: int = 30) -> Dict[str, ForecastResult]:
        """Predict key business outcomes."""
        predictions = {}
        
        for metric_name, model_id in self.business_models.items():
            try:
                forecast = self.forecasting_engine.generate_forecast(
                    model_id, horizon=horizon_days
                )
                predictions[metric_name] = forecast
                logger.info(f"Generated forecast for {metric_name}")
            except Exception as e:
                logger.error(f"Prediction failed for {metric_name}: {e}")
        
        return predictions
    
    def get_business_insights(self, predictions: Dict[str, ForecastResult]) -> List[Dict[str, Any]]:
        """Generate business insights from predictions."""
        insights = []
        
        for metric_name, forecast in predictions.items():
            try:
                # Calculate trend direction
                values = forecast.forecast_values
                if len(values) >= 2:
                    start_avg = np.mean(values[:7])  # First week
                    end_avg = np.mean(values[-7:])   # Last week
                    trend_direction = "increasing" if end_avg > start_avg else "decreasing"
                    trend_magnitude = abs((end_avg - start_avg) / start_avg) * 100
                    
                    # Generate insight
                    insight = {
                        'metric': metric_name,
                        'trend_direction': trend_direction,
                        'trend_magnitude': trend_magnitude,
                        'confidence': forecast.accuracy_metrics.get('model_r2', 0.0),
                        'forecast_summary': {
                            'period_start': values[0],
                            'period_end': values[-1],
                            'average_value': np.mean(values),
                            'volatility': np.std(values)
                        },
                        'business_impact': self._assess_business_impact(
                            metric_name, trend_direction, trend_magnitude
                        )
                    }
                    insights.append(insight)
                    
            except Exception as e:
                logger.error(f"Insight generation failed for {metric_name}: {e}")
        
        return insights
    
    def _assess_business_impact(self, metric_name: str, trend_direction: str, 
                              magnitude: float) -> str:
        """Assess business impact of predicted trends."""
        impact_level = "low"
        
        # High-impact metrics
        high_impact_metrics = ['revenue', 'conversion_rate', 'churn_rate']
        
        if any(metric in metric_name.lower() for metric in high_impact_metrics):
            if magnitude > 20:
                impact_level = "high"
            elif magnitude > 10:
                impact_level = "medium"
        
        # Revenue-specific logic
        if 'revenue' in metric_name.lower():
            if trend_direction == 'increasing' and magnitude > 15:
                impact_level = "high_positive"
            elif trend_direction == 'decreasing' and magnitude > 10:
                impact_level = "high_negative"
        
        # Churn-specific logic
        if 'churn' in metric_name.lower():
            if trend_direction == 'increasing' and magnitude > 15:
                impact_level = "high_negative"
            elif trend_direction == 'decreasing' and magnitude > 10:
                impact_level = "high_positive"
        
        return impact_level

class PredictiveAnalyticsManager:
    """Main manager for predictive analytics capabilities."""
    
    def __init__(self):
        self.forecasting_engine = ForecastingEngine()
        self.business_predictor = BusinessMetricsPredictor(self.forecasting_engine)
        self.active_forecasts: Dict[str, ForecastResult] = {}
        
    async def setup_predictive_models(self, historical_data: Dict[str, pd.Series]):
        """Setup predictive models for all metrics."""
        try:
            # Setup business metrics prediction
            self.business_predictor.setup_business_metric_prediction(historical_data)
            
            # Train models for other important metrics
            for metric_name, data in historical_data.items():
                if metric_name not in self.business_predictor.business_models:
                    if len(data) >= 20:  # Minimum requirement
                        try:
                            model_id = self.forecasting_engine.train_model(
                                data, model_type='auto'
                            )
                            logger.info(f"Predictive model created for {metric_name}")
                        except Exception as e:
                            logger.warning(f"Model creation failed for {metric_name}: {e}")
            
            logger.info("Predictive analytics setup completed")
            
        except Exception as e:
            logger.error(f"Predictive analytics setup failed: {e}")
    
    async def generate_forecasts(self, horizon_days: int = 30) -> Dict[str, ForecastResult]:
        """Generate forecasts for all configured metrics."""
        try:
            # Generate business forecasts
            business_forecasts = self.business_predictor.predict_business_outcomes(horizon_days)
            
            # Generate forecasts for other metrics
            all_forecasts = business_forecasts.copy()
            
            for model_id in self.forecasting_engine.models:
                model_info = self.forecasting_engine.get_model_info(model_id)
                metric_name = model_info.get('target_name', 'unknown')
                
                if metric_name not in all_forecasts:
                    try:
                        forecast = self.forecasting_engine.generate_forecast(
                            model_id, horizon=horizon_days
                        )
                        all_forecasts[metric_name] = forecast
                    except Exception as e:
                        logger.error(f"Forecast generation failed for {metric_name}: {e}")
            
            # Store active forecasts
            self.active_forecasts = all_forecasts
            
            return all_forecasts
            
        except Exception as e:
            logger.error(f"Forecast generation failed: {e}")
            return {}
    
    def get_predictive_insights(self) -> Dict[str, Any]:
        """Get comprehensive predictive insights."""
        try:
            if not self.active_forecasts:
                return {'error': 'No active forecasts available'}
            
            # Generate business insights
            business_insights = self.business_predictor.get_business_insights(
                self.active_forecasts
            )
            
            # Calculate overall trends
            overall_trends = self._calculate_overall_trends()
            
            # Risk assessment
            risk_assessment = self._assess_prediction_risks()
            
            return {
                'business_insights': business_insights,
                'overall_trends': overall_trends,
                'risk_assessment': risk_assessment,
                'forecast_summary': {
                    'total_metrics': len(self.active_forecasts),
                    'forecast_horizon': max([f.forecast_horizon for f in self.active_forecasts.values()]) if self.active_forecasts else 0,
                    'average_confidence': np.mean([
                        f.accuracy_metrics.get('model_r2', 0.0) 
                        for f in self.active_forecasts.values()
                    ]) if self.active_forecasts else 0.0
                },
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Predictive insights generation failed: {e}")
            return {'error': str(e)}
    
    def _calculate_overall_trends(self) -> Dict[str, Any]:
        """Calculate overall business trends."""
        try:
            trends = {
                'positive_trends': [],
                'negative_trends': [],
                'stable_metrics': []
            }
            
            for metric_name, forecast in self.active_forecasts.items():
                values = forecast.forecast_values
                if len(values) >= 2:
                    start_val = np.mean(values[:3])
                    end_val = np.mean(values[-3:])
                    change_pct = ((end_val - start_val) / start_val) * 100 if start_val != 0 else 0
                    
                    if change_pct > 5:
                        trends['positive_trends'].append({
                            'metric': metric_name,
                            'change_pct': change_pct
                        })
                    elif change_pct < -5:
                        trends['negative_trends'].append({
                            'metric': metric_name,
                            'change_pct': change_pct
                        })
                    else:
                        trends['stable_metrics'].append(metric_name)
            
            return trends
            
        except Exception as e:
            logger.error(f"Overall trends calculation failed: {e}")
            return {}
    
    def _assess_prediction_risks(self) -> Dict[str, Any]:
        """Assess risks in predictions."""
        try:
            risks = {
                'high_uncertainty': [],
                'declining_metrics': [],
                'volatile_forecasts': []
            }
            
            for metric_name, forecast in self.active_forecasts.items():
                # Check model confidence
                confidence = forecast.accuracy_metrics.get('model_r2', 0.0)
                if confidence < 0.5:
                    risks['high_uncertainty'].append({
                        'metric': metric_name,
                        'confidence': confidence
                    })
                
                # Check for declining trends
                values = forecast.forecast_values
                if len(values) >= 2:
                    trend_slope = (values[-1] - values[0]) / len(values)
                    if trend_slope < 0 and 'revenue' in metric_name.lower():
                        risks['declining_metrics'].append({
                            'metric': metric_name,
                            'decline_rate': trend_slope
                        })
                
                # Check volatility
                if len(values) > 1:
                    volatility = np.std(values) / np.mean(values) if np.mean(values) != 0 else 0
                    if volatility > 0.3:  # 30% coefficient of variation
                        risks['volatile_forecasts'].append({
                            'metric': metric_name,
                            'volatility': volatility
                        })
            
            return risks
            
        except Exception as e:
            logger.error(f"Risk assessment failed: {e}")
            return {}
    
    def get_forecast_data(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed forecast data for a specific metric."""
        if metric_name not in self.active_forecasts:
            return None
        
        forecast = self.active_forecasts[metric_name]
        return {
            'metric_name': forecast.metric_name,
            'forecast_values': forecast.forecast_values,
            'forecast_dates': [d.isoformat() for d in forecast.forecast_dates],
            'confidence_intervals': forecast.confidence_intervals,
            'model_type': forecast.model_type,
            'accuracy_metrics': forecast.accuracy_metrics,
            'feature_importance': forecast.feature_importance,
            'created_at': forecast.created_at.isoformat()
        }
