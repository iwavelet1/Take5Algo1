"""
Asset Data Events

This module defines event classes for the asset data management system.
"""

from dataclasses import dataclass
from typing import Any
from datetime import datetime
from .sliding_window_manager import BufferInfo


@dataclass
class AssetDataChanged:
    """
    Event fired when new data arrives for a topic/asset combination.
    
    This event is triggered when the sliding window buffer changes due to new data.
    """
    topic: str                      # e.g., "historic_stats", "asset_trends"
    asset: str                      # e.g., "SPY", "AAPL"
    bar_time_frame: int            # e.g., 60, 300, 720
    message: Any                   # The new message that arrived
    buffer_info: BufferInfo        # Current buffer state after the change
    timestamp: datetime            # When this event was created
    
    def __str__(self) -> str:
        return f"AssetDataChanged({self.topic}/{self.asset}/{self.bar_time_frame}s at {self.timestamp})"
    
    def get_combination_key(self) -> str:
        """Get a unique key for this topic/asset/timeframe combination"""
        return f"{self.topic}:{self.asset}:{self.bar_time_frame}" 