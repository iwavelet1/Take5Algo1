"""
Events for Asset Trends System

This module defines event classes used throughout the asset trends system.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime


@dataclass
class NewAssetEvent:
    """
    Event triggered when a new asset is detected in the buffer manager.
    
    This event is fired when the first message for a new asset/topic/barTimeFrame
    combination is processed.
    """
    topic: str                    # e.g., "bar_trends", "historic_stats"
    asset: str                    # e.g., "AAPL", "GOOGL"
    bar_time_frame: int          # e.g., 60, 180, 420
    first_message: Any           # The first message received for this combination
    timestamp: datetime          # When this event was created
    metadata: Optional[Dict[str, Any]] = None  # Additional context data
    
    def __str__(self) -> str:
        return f"NewAssetEvent({self.topic}/{self.asset}/{self.bar_time_frame}s at {self.timestamp})"
    
    def get_combination_key(self) -> str:
        """Get a unique key for this asset combination"""
        return f"{self.topic}:{self.asset}:{self.bar_time_frame}" 