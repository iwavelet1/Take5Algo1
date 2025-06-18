"""
Asset Trends Analysis Class

This module provides the AssetTrends class for analyzing trends of individual assets
across different time frames and topics.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

# Import buffer manager for accessing asset data
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from asset_data.sliding_window_manager import AssetBufferManager, BufferInfo


@dataclass
class TrendAnalysis:
    """Results of trend analysis for an asset"""
    asset: str
    topic: str
    bar_time_frame: int
    trend_direction: str          # "UP", "DOWN", "FLAT", "VOLATILE"
    trend_strength: float         # 0.0 to 1.0
    confidence: float             # 0.0 to 1.0
    data_points: int              # Number of data points analyzed
    analysis_timestamp: datetime
    metadata: Dict[str, Any]      # Additional analysis data


class AssetTrends:
    """
    Analyzes trends for a specific asset across different topics and time frames.
    
    Each instance manages trend analysis for one asset, but can track multiple
    topics and bar time frames for that asset.
    """
    
    def __init__(self, asset: str, buffer_manager: AssetBufferManager):
        self.asset = asset
        self.buffer_manager = buffer_manager
        self.logger = logging.getLogger(f"{__name__}.{asset}")
        
        # Track which topic/timeframe combinations we're analyzing
        self.tracked_combinations: Dict[str, Dict[int, bool]] = {}  # topic -> timeframe -> active
        
        # Store recent trend analyses
        self.recent_analyses: List[TrendAnalysis] = []
        self.max_analyses_history = 100  # Keep last 100 analyses
        
        # Analysis parameters
        self.min_data_points = 5      # Minimum points needed for trend analysis
        self.analysis_window_minutes = 30  # Look at last N minutes for trend
        
        self.logger.info(f"🎯 AssetTrends initialized for {asset}")
    
    def add_topic_timeframe(self, topic: str, bar_time_frame: int):
        """Register a new topic/timeframe combination to track"""
        if topic not in self.tracked_combinations:
            self.tracked_combinations[topic] = {}
        
        if bar_time_frame not in self.tracked_combinations[topic]:
            self.tracked_combinations[topic][bar_time_frame] = True
            self.logger.info(
                f"📊 Now tracking {self.asset} trends for {topic}/{bar_time_frame}s"
            )
    
    def analyze_trends(self) -> List[TrendAnalysis]:
        """
        Analyze trends for all tracked topic/timeframe combinations.
        Returns list of trend analyses.
        """
        analyses = []
        
        for topic, timeframes in self.tracked_combinations.items():
            for bar_time_frame, is_active in timeframes.items():
                if not is_active:
                    continue
                
                analysis = self._analyze_single_combination(topic, bar_time_frame)
                if analysis:
                    analyses.append(analysis)
        
        # Store analyses in history
        self.recent_analyses.extend(analyses)
        self._trim_analyses_history()
        
        return analyses
    
    def _analyze_single_combination(self, topic: str, bar_time_frame: int) -> Optional[TrendAnalysis]:
        """Analyze trends for a specific topic/timeframe combination"""
        try:
            # Get buffer info
            buffer_info = self.buffer_manager.get_buffer_info(topic, self.asset, bar_time_frame)
            if not buffer_info or buffer_info.point_count < self.min_data_points:
                return None
            
            # Get recent messages
            messages = self.buffer_manager.get_messages(topic, self.asset, bar_time_frame)
            if not messages:
                return None
            
            # Filter to analysis window (last N minutes)
            cutoff_time = datetime.now().timestamp() * 1000 - (self.analysis_window_minutes * 60 * 1000)
            recent_messages = [
                msg for msg in messages 
                if msg.get('publishTime', {}).get('take5Time', 0) >= cutoff_time
            ]
            
            if len(recent_messages) < self.min_data_points:
                recent_messages = messages[-self.min_data_points:]  # Use last N messages
            
            # Perform trend analysis
            trend_direction, trend_strength, confidence, metadata = self._calculate_trend(
                recent_messages, topic, bar_time_frame
            )
            
            return TrendAnalysis(
                asset=self.asset,
                topic=topic,
                bar_time_frame=bar_time_frame,
                trend_direction=trend_direction,
                trend_strength=trend_strength,
                confidence=confidence,
                data_points=len(recent_messages),
                analysis_timestamp=datetime.now(),
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing {topic}/{self.asset}/{bar_time_frame}: {e}")
            return None
    
    def _calculate_trend(self, messages: List[Any], topic: str, bar_time_frame: int) -> tuple:
        """
        Calculate trend direction and strength from messages.
        Returns: (direction, strength, confidence, metadata)
        """
        try:
            # Extract price data (assuming bar data structure)
            prices = []
            timestamps = []
            
            for msg in messages:
                bar_data = msg.get('barData', {})
                pub_time = msg.get('publishTime', {}).get('take5Time')
                
                if bar_data and pub_time:
                    # Use close price for trend analysis
                    close_price = bar_data.get('close')
                    if close_price is not None:
                        prices.append(float(close_price))
                        timestamps.append(pub_time)
            
            if len(prices) < 2:
                return "INSUFFICIENT_DATA", 0.0, 0.0, {"reason": "Not enough price data"}
            
            # Simple trend analysis based on price movement
            price_changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
            
            # Calculate trend metrics
            positive_changes = [x for x in price_changes if x > 0]
            negative_changes = [x for x in price_changes if x < 0]
            
            total_change = prices[-1] - prices[0]
            avg_change = sum(price_changes) / len(price_changes)
            
            # Determine trend direction
            if abs(total_change) < (prices[0] * 0.001):  # Less than 0.1% change
                trend_direction = "FLAT"
                trend_strength = 0.1
            elif total_change > 0:
                trend_direction = "UP"
                trend_strength = min(1.0, abs(total_change) / prices[0] * 100)  # Percentage change
            else:
                trend_direction = "DOWN"
                trend_strength = min(1.0, abs(total_change) / prices[0] * 100)
            
            # Calculate confidence based on consistency
            consistent_direction = 0
            for change in price_changes:
                if (total_change > 0 and change > 0) or (total_change < 0 and change < 0):
                    consistent_direction += 1
            
            confidence = consistent_direction / len(price_changes) if price_changes else 0.0
            
            # Check for high volatility
            if len(positive_changes) > 0 and len(negative_changes) > 0:
                volatility = abs(sum(positive_changes)) + abs(sum(negative_changes))
                if volatility > abs(total_change) * 3:  # High volatility threshold
                    trend_direction = "VOLATILE"
                    confidence *= 0.5  # Reduce confidence for volatile assets
            
            metadata = {
                "total_change": total_change,
                "avg_change": avg_change,
                "price_range": [min(prices), max(prices)],
                "num_positive_changes": len(positive_changes),
                "num_negative_changes": len(negative_changes),
                "analysis_method": "simple_price_trend"
            }
            
            return trend_direction, trend_strength, confidence, metadata
            
        except Exception as e:
            self.logger.error(f"Error calculating trend: {e}")
            return "ERROR", 0.0, 0.0, {"error": str(e)}
    
    def get_latest_analysis(self, topic: str, bar_time_frame: int) -> Optional[TrendAnalysis]:
        """Get the most recent analysis for a specific combination"""
        for analysis in reversed(self.recent_analyses):
            if analysis.topic == topic and analysis.bar_time_frame == bar_time_frame:
                return analysis
        return None
    
    def get_all_latest_analyses(self) -> List[TrendAnalysis]:
        """Get the most recent analysis for each tracked combination"""
        latest_analyses = {}
        
        for analysis in reversed(self.recent_analyses):
            key = f"{analysis.topic}:{analysis.bar_time_frame}"
            if key not in latest_analyses:
                latest_analyses[key] = analysis
        
        return list(latest_analyses.values())
    
    def _trim_analyses_history(self):
        """Keep only the most recent analyses"""
        if len(self.recent_analyses) > self.max_analyses_history:
            self.recent_analyses = self.recent_analyses[-self.max_analyses_history:]
    
    def get_status(self) -> Dict[str, Any]:
        """Get status information about this asset trends analyzer"""
        return {
            "asset": self.asset,
            "tracked_combinations": len([
                1 for timeframes in self.tracked_combinations.values() 
                for active in timeframes.values() if active
            ]),
            "total_analyses": len(self.recent_analyses),
            "latest_analysis_time": (
                max(a.analysis_timestamp for a in self.recent_analyses) 
                if self.recent_analyses else None
            ),
            "combinations": {
                topic: list(timeframes.keys()) 
                for topic, timeframes in self.tracked_combinations.items()
            }
        } 