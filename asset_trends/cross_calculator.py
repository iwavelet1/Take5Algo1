"""
Cross Calculator

Stateless algorithm for predicting golden/death cross signals between different barTimeFrames.
"""

import logging
import math
from typing import Dict, List, Optional, Any
from asset_trends.asset_trend_processor import SignalAlgorithm


class CrossCalculator(SignalAlgorithm):
    """
    Stateless cross calculator that predicts golden/death cross signals
    between different barTimeFrames for the same asset.
    """
    
    def __init__(self, max_prediction_seconds: int = 3600, min_angle_threshold: float = 0.01):
        """
        Initialize CrossCalculator with filtering thresholds.
        
        Args:
            max_prediction_seconds: Maximum seconds into future to predict (default: 1 hour)
            min_angle_threshold: Minimum angle threshold to avoid shallow crosses (radians)
        """
        self.max_prediction_seconds = max_prediction_seconds
        self.min_angle_threshold = min_angle_threshold
        self.logger = logging.getLogger(f"{__name__}.CrossCalculator")
    
    def process(self, event, topic_asset_data: dict):
        """
        Process cross signals using event and pre-retrieved data.
        Assumes AssetTrendGraph structure with TrendData containing slopes/intercepts.
        
        Args:
            event: AssetDataChanged event
            topic_asset_data: Dict mapping barTimeFrame -> TimeBasedSlidingWindow
        """
        try:
            asset = event.asset
            
            # Layer 1: Use pre-retrieved AssetTrendGraph records
            if len(topic_asset_data) < 2:
                self.logger.debug(f"Not enough timeframes for {asset}: {len(topic_asset_data)}")
                return
            
            # Process specific pairs: 1-3 (60 vs 720) and 2-4 (300 vs 1560)
            # Define the pairs we want to analyze
            target_pairs = [
                (60, 720),    # 1-3: 1-minute vs 12-minute
                (300, 1560)   # 2-4: 5-minute vs 26-minute
            ]
            
            for tf_a, tf_b in target_pairs:
                if tf_a in topic_asset_data and tf_b in topic_asset_data:
                    self._process_timeframe_pair(asset, tf_a, tf_b, topic_asset_data)
                else:
                    missing = [tf for tf in [tf_a, tf_b] if tf not in topic_asset_data]
                    self.logger.info(f"Skipping pair {tf_a}-{tf_b} for {asset}: missing timeframes {missing}")
                    
        except Exception as e:
            self.logger.error(f"Error processing cross signals for {asset}: {e}")
    

    
    def _process_timeframe_pair(self, asset: str, tf_a: int, tf_b: int, records_by_timeframe: Dict[int, Any]):
        """
        Process a pair of timeframes for cross prediction.
        """
        rec_a = records_by_timeframe[tf_a]
        rec_b = records_by_timeframe[tf_b]
        
        # Extract take5Time from both records
        take5_time_a = self._extract_take5_time(rec_a)
        take5_time_b = self._extract_take5_time(rec_b)
        
        if take5_time_a is None or take5_time_b is None:
            self.logger.warning(f"Missing take5Time for {asset}: tf_a={tf_a}, tf_b={tf_b}")
            return
        
        if take5_time_a != take5_time_b:
            self.logger.info(f"Drop {asset}: timeframe {tf_a} take5Time={take5_time_a} != timeframe {tf_b} take5Time={take5_time_b}")
            return
        
        # Layer 2: Calculate cross prediction
        prediction = self._calc_cross_prediction(asset, tf_a, tf_b, rec_a, rec_b, take5_time_a)
        
        if prediction and prediction.get('crossType') != 'none':
            self.logger.info(f"Cross prediction for {asset}: {prediction}")
            # TODO: Publish or emit the prediction
    
    def _extract_take5_time(self, record) -> Optional[int]:
        """
        Extract take5Time from a record's PublishTime block.
        """
        try:
            if isinstance(record, dict):
                return record.get('PublishTime', {}).get('take5Time')
            elif hasattr(record, 'PublishTime'):
                if hasattr(record.PublishTime, 'take5Time'):
                    return record.PublishTime.take5Time
                elif isinstance(record.PublishTime, dict):
                    return record.PublishTime.get('take5Time')
            return None
        except Exception as e:
            self.logger.error(f"Error extracting take5Time: {e}")
            return None
    
    def _calc_cross_prediction(self, asset: str, tf_a: int, tf_b: int, rec_a: Any, rec_b: Any, take5_time: int) -> Optional[Dict]:
        """
        Layer 2: Calculate cross prediction between two records.
        
        Returns:
            Prediction dict or None if no valid cross
        """
        try:
            # Extract trend data from both records
            trend_a = self._extract_trend_data(rec_a)
            trend_b = self._extract_trend_data(rec_b)
            
            if not trend_a or not trend_b:
                return None
            
            # Get slopes and intercepts (using SMA for now)
            slope_a = trend_a.get('wSma_slope', trend_a.get('smaSlope'))
            intercept_a = trend_a.get('wSma_intercept', trend_a.get('smaIntercept'))
            slope_err_a = trend_a.get('wSma_slope_std_err', trend_a.get('smaSlopeStdErr'))
            
            slope_b = trend_b.get('wSma_slope', trend_b.get('smaSlope'))
            intercept_b = trend_b.get('wSma_intercept', trend_b.get('smaIntercept'))
            slope_err_b = trend_b.get('wSma_slope_std_err', trend_b.get('smaSlopeStdErr'))
            
            if None in [slope_a, intercept_a, slope_b, intercept_b]:
                self.logger.warning(f"Missing slope/intercept data for {asset}")
                return None
            
            # Calculate t-statistics for statistical significance
            t_stat_a = abs(slope_a / slope_err_a) if slope_err_a and slope_err_a != 0 else 0
            t_stat_b = abs(slope_b / slope_err_b) if slope_err_b and slope_err_b != 0 else 0
            
            # Require both slopes to be statistically significant (t > 2)
            if t_stat_a < 2.0 or t_stat_b < 2.0:
                return {'crossType': 'none', 'reason': 'slopes not statistically significant'}
            
            # Calculate intersection point
            slope_diff = slope_a - slope_b
            if abs(slope_diff) < 1e-10:  # Parallel lines
                return {'crossType': 'none', 'reason': 'parallel lines'}
            
            # Time to intersection (in seconds from now)
            intercept_diff = intercept_b - intercept_a
            predicted_cross_time = intercept_diff / slope_diff
            
            # Filter out crosses that are too far in the future
            if abs(predicted_cross_time) > self.max_prediction_seconds:
                return {'crossType': 'none', 'reason': f'cross too far: {predicted_cross_time}s'}
            
            # Calculate angle of intersection
            angle = abs(math.atan(slope_diff))
            
            # Filter out shallow crosses
            if angle < self.min_angle_threshold:
                return {'crossType': 'none', 'reason': f'angle too shallow: {angle}'}
            
            # Determine cross type
            current_value_a = intercept_a  # Current value of line A
            current_value_b = intercept_b  # Current value of line B
            
            if predicted_cross_time > 0:  # Cross in the future
                if slope_a > slope_b:  # A is rising faster than B
                    cross_type = 'golden' if current_value_a < current_value_b else 'death'
                else:  # B is rising faster than A
                    cross_type = 'death' if current_value_a < current_value_b else 'golden'
            else:
                return {'crossType': 'none', 'reason': 'cross in the past'}
            
            # Calculate confidence based on t-statistics
            confidence = min(1.0, (t_stat_a + t_stat_b) / 10.0)
            
            return {
                'asset': asset,
                'barTimeFrameA': tf_a,
                'barTimeFrameB': tf_b,
                'take5Time': take5_time,
                'crossType': cross_type,
                'predictedCrossTime': int(predicted_cross_time),
                'angle': angle,
                'A': {
                    'slope': slope_a,
                    'intercept': intercept_a,
                    'slopeStdErr': slope_err_a,
                    'tStat': t_stat_a
                },
                'B': {
                    'slope': slope_b,
                    'intercept': intercept_b,
                    'slopeStdErr': slope_err_b,
                    'tStat': t_stat_b
                },
                'confidence': confidence
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating cross prediction for {asset}: {e}")
            return None
    
    def _extract_trend_data(self, record) -> Optional[Dict]:
        """
        Extract TrendData from a record.
        """
        try:
            if isinstance(record, dict):
                # Try different possible paths to TrendData
                trend_vector = record.get('trendVector', {})
                return trend_vector.get('trendData', {})
            elif hasattr(record, 'trendVector'):
                if hasattr(record.trendVector, 'trendData'):
                    # Convert to dict if it's an object
                    trend_data = record.trendVector.trendData
                    if hasattr(trend_data, '__dict__'):
                        return trend_data.__dict__
                    return trend_data
            return None
        except Exception as e:
            self.logger.error(f"Error extracting trend data: {e}")
            return None 