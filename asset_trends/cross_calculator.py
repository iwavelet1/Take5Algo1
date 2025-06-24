"""
Cross Calculator

Stateless algorithm for calculating golden/death cross signals between two AssetTrendGraph records.
"""

import logging
import math
from typing import Optional, Dict, Any


class CrossCalculator:
    """
    Stateless cross calculator for golden/death cross predictions.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.CrossCalculator")
    
    def calc(self, trend_a: Any, trend_b: Any) -> Optional[Dict]:
        """
        Calculate cross prediction between two AssetTrendGraph records.
        
        Args:
            trend_a: First AssetTrendGraph record
            trend_b: Second AssetTrendGraph record
            
        Returns:
            Cross prediction dict or None if no valid cross
        """
        try:
            # Extract trend data
            data_a = self._extract_trend_data(trend_a)
            data_b = self._extract_trend_data(trend_b)
            
            # Get slopes and intercepts
            slope_a = data_a.get('wSma_slope', data_a.get('smaSlope'))
            intercept_a = data_a.get('wSma_intercept', data_a.get('smaIntercept'))
            slope_err_a = data_a.get('wSma_slope_std_err', data_a.get('smaSlopeStdErr'))
            
            slope_b = data_b.get('wSma_slope', data_b.get('smaSlope'))
            intercept_b = data_b.get('wSma_intercept', data_b.get('smaIntercept'))
            slope_err_b = data_b.get('wSma_slope_std_err', data_b.get('smaSlopeStdErr'))
            
            # Calculate t-statistics
            t_stat_a = abs(slope_a / slope_err_a) if slope_err_a and slope_err_a != 0 else 0
            t_stat_b = abs(slope_b / slope_err_b) if slope_err_b and slope_err_b != 0 else 0
            
            # Check statistical significance
            if t_stat_a < 2.0 or t_stat_b < 2.0:
                return {'crossType': 'none', 'reason': 'not_significant'}
            
            # Calculate intersection
            slope_diff = slope_a - slope_b
            if abs(slope_diff) < 1e-10:
                return {'crossType': 'none', 'reason': 'parallel'}
            
            intercept_diff = intercept_b - intercept_a
            predicted_cross_time = intercept_diff / slope_diff
            
            # Filter future crosses only
            if predicted_cross_time <= 0:
                return {'crossType': 'none', 'reason': 'past'}
            
            # Filter time range
            if predicted_cross_time > 3600:  # 1 hour max
                return {'crossType': 'none', 'reason': 'too_far'}
            
            # Filter shallow angles  
            angle = abs(math.atan(slope_diff))
            if angle < 0.01:
                return {'crossType': 'none', 'reason': 'shallow'}
            
            # Determine cross type
            current_value_a = intercept_a
            current_value_b = intercept_b
            
            if slope_a > slope_b:
                cross_type = 'golden' if current_value_a < current_value_b else 'death'
            else:
                cross_type = 'death' if current_value_a < current_value_b else 'golden'
            
            confidence = min(1.0, (t_stat_a + t_stat_b) / 10.0)
            
            return {
                'crossType': cross_type,
                'predictedCrossTime': int(predicted_cross_time),
                'angle': angle,
                'confidence': confidence,
                'slopeA': slope_a,
                'slopeB': slope_b,
                'interceptA': intercept_a,
                'interceptB': intercept_b
            }
            
        except Exception as e:
            self.logger.error(f"Error in cross calculation: {e}")
            return None
    
    def _extract_trend_data(self, record) -> Dict:
        """Extract TrendData from AssetTrendGraph record."""
        return record.get('trendVector', {}).get('trendData', {}) 