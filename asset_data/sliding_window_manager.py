"""
Python implementation of AssetBufferManager for time-based sliding window management.

This implementation mirrors the TypeScript version from the Take5PublisherUi project,
providing the same functionality for managing real-time financial data streams.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime
from sortedcontainers import SortedDict


@dataclass
class DataPoint:
    """Data point structure matching TypeScript implementation"""
    time: int  # publishTime.take5Time (for sorting/filtering)
    message: Any  # Complete original message


@dataclass 
class BufferInfo:
    """Buffer information structure matching TypeScript implementation"""
    asset: str
    bar_time_frame: int
    latest_time: Optional[int]
    oldest_time: Optional[int]
    latest_time_str: Optional[str]
    oldest_time_str: Optional[str]
    window_span_ms: int
    point_count: int
    window_size_ms: int


class TimeBasedSlidingWindow:
    """
    Individual sliding window buffer for a specific asset/barTimeFrame combination.
    Matches the TypeScript TimeBasedSlidingWindow implementation.
    """
    
    def __init__(self, asset: str, bar_time_frame: int, window_size_minutes: int):
        self.asset = asset
        self.bar_time_frame = bar_time_frame
        self.window_size_ms = window_size_minutes * 60 * 1000  # Convert to milliseconds
        self.buffer: List[DataPoint] = []
        self.latest_time = 0
        self.logger = logging.getLogger(f"{__name__}.{asset}.{bar_time_frame}")
        
    def add_point(self, complete_message: Any) -> bool:
        """
        Add a message to the sliding window.
        Returns True if buffer content changed.
        """
        # Extract time from message
        publish_time = self._extract_publish_time(complete_message)
        if publish_time is None:
            self.logger.warning(
                f"Message missing publishTime.take5Time for {self.asset}:{self.bar_time_frame}: {complete_message}"
            )
            return False
        
 
        # Create data point with complete message
        data_point = DataPoint(time=publish_time, message=complete_message)
        
        # Capture initial length before adding new point
        initial_length = len(self.buffer)
        
        # Add new point
        self.buffer.append(data_point)
        self.latest_time = publish_time
        
        # Remove old points based on sliding window
        cutoff_time = publish_time - self.window_size_ms
        self.buffer = [p for p in self.buffer if p.time >= cutoff_time]
        
        # Sort by time to ensure proper ordering (should already be sorted, but safety)
        self.buffer.sort(key=lambda p: p.time)
        
        # Return True if buffer actually changed (new point added or old points removed)
        return len(self.buffer) != initial_length or len(self.buffer) == 1
    
    def get_messages(self) -> List[Any]:
        """Get all messages in the sliding window"""
        return [point.message for point in self.buffer]
    
    def get_data_points(self) -> List[DataPoint]:
        """Get all data points in the sliding window"""
        return self.buffer.copy()
    
    def get_current_message(self) -> Optional[Any]:
        """Get the most recent message"""
        return self.buffer[-1].message if self.buffer else None
    
    def get_asset(self) -> str:
        """Get the asset symbol"""
        return self.asset
    
    def get_bar_time_frame(self) -> int:
        """Get the bar time frame"""
        return self.bar_time_frame
    
    def get_window_info(self) -> BufferInfo:
        """Get comprehensive window information"""
        if not self.buffer:
            return BufferInfo(
                asset=self.asset,
                bar_time_frame=self.bar_time_frame,
                latest_time=None,
                oldest_time=None,
                latest_time_str=None,
                oldest_time_str=None,
                window_span_ms=0,
                point_count=0,
                window_size_ms=self.window_size_ms
            )
        
        latest = self.buffer[-1]
        oldest = self.buffer[0]
        
        return BufferInfo(
            asset=self.asset,
            bar_time_frame=self.bar_time_frame,
            latest_time=latest.time,
            oldest_time=oldest.time,
            latest_time_str=self._extract_time_str(latest.message),
            oldest_time_str=self._extract_time_str(oldest.message),
            window_span_ms=latest.time - oldest.time,
            point_count=len(self.buffer),
            window_size_ms=self.window_size_ms
        )
    

    
    def _extract_publish_time(self, message: Any) -> Optional[int]:
        """Extract publishTime.take5Time from message"""
        return message.get('publishTime', {}).get('take5Time')
    
    def _extract_time_str(self, message: Any) -> Optional[str]:
        """Extract publishTime.take5TimeStr from message"""
        return message.get('publishTime', {}).get('take5TimeStr')


class AssetBufferManager:
    """
    Global buffer manager for time-based sliding windows.
    Matches the TypeScript AssetBufferManager implementation.
    
    Structure: Topic → Asset → BarTimeFrame → Buffer
    """
    
    def __init__(self, window_size_minutes: int = 120):
        self.window_size_minutes = window_size_minutes
        # Topic → Asset → BarTimeFrame → Buffer
        # Using SortedDict for automatic key sorting on assets and timeframes
        self.buffers: Dict[str, SortedDict[str, SortedDict[int, TimeBasedSlidingWindow]]] = defaultdict(
            lambda: SortedDict(lambda: SortedDict())
        )
        self.change_listeners: Set[Callable[[str, str, int], None]] = set()
        
        # Event system for AssetDataChanged
        self.asset_data_listeners: Dict[str, Dict[str, List[Callable]]] = defaultdict(lambda: defaultdict(list))
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"AssetBufferManager initialized with {window_size_minutes} minute sliding windows")
    

    
    def add_message(self, topic: str, asset: str, bar_time_frame: int, complete_message: Any) -> bool:
        """Add a message to the appropriate buffer"""
        # Get or create buffer - SortedDict automatically maintains sorted order
        if topic not in self.buffers:
            self.buffers[topic] = SortedDict()
        if asset not in self.buffers[topic]:
            self.buffers[topic][asset] = SortedDict()
        if bar_time_frame not in self.buffers[topic][asset]:
            self.buffers[topic][asset][bar_time_frame] = TimeBasedSlidingWindow(
                asset, bar_time_frame, self.window_size_minutes
            )
        
        buffer = self.buffers[topic][asset][bar_time_frame]
        changed = buffer.add_point(complete_message)
        
        # Fire AssetDataChanged event and notify listeners if buffer changed
        if changed:
            self._fire_asset_data_changed(topic, asset, bar_time_frame, complete_message)
            self._notify_listeners(topic, asset, bar_time_frame)
        
        return changed
    
    def get_messages(self, topic: str, asset: str, bar_time_frame: int) -> List[Any]:
        """Get all messages for a specific combination"""
        topic_data = self.buffers.get(topic, SortedDict())
        asset_data = topic_data.get(asset, SortedDict())
        buffer = asset_data.get(bar_time_frame)
        return buffer.get_messages() if buffer else []
    
    def get_current_message(self, topic: str, asset: str, bar_time_frame: int) -> Optional[Any]:
        """Get the current (latest) message for a specific combination"""
        topic_data = self.buffers.get(topic, SortedDict())
        asset_data = topic_data.get(asset, SortedDict())
        buffer = asset_data.get(bar_time_frame)
        return buffer.get_current_message() if buffer else None
    
    def get_asset_from_topic(self, topic: str, asset: str) -> Dict[int, Any]:
        """Get all data for a topic/asset combination"""
        topic_data = self.buffers.get(topic, SortedDict())
        return topic_data.get(asset, SortedDict())
    
    def get_all_assets(self, topic: str) -> List[str]:
        """Get all assets for a topic"""
        # SortedDict keys are already sorted, no need to call sorted()
        return list(self.buffers.get(topic, SortedDict()).keys())
    
    def get_bar_time_frames(self, topic: str, asset: str) -> List[int]:
        """Get all bar time frames for a topic/asset combination"""
        # SortedDict keys are already sorted, no need to call sorted()
        topic_data = self.buffers.get(topic, SortedDict())
        asset_data = topic_data.get(asset, SortedDict())
        return list(asset_data.keys())
    
    def get_buffer_info(self, topic: str, asset: str, bar_time_frame: int) -> Optional[BufferInfo]:
        """Get buffer information for a specific combination"""
        topic_data = self.buffers.get(topic, SortedDict())
        asset_data = topic_data.get(asset, SortedDict())
        buffer = asset_data.get(bar_time_frame)
        return buffer.get_window_info() if buffer else None
    
    def subscribe(self, listener: Callable[[str, str, int], None]) -> Callable[[], None]:
        """
        Subscribe to buffer changes.
        Returns unsubscribe function.
        """
        self.change_listeners.add(listener)
        
        def unsubscribe():
            self.change_listeners.discard(listener)
        
        return unsubscribe
    
    def get_all_combinations(self, topic: str) -> List[Dict[str, Any]]:
        """Get all asset/barTimeFrame combinations for a topic"""
        combinations = []
        for asset, bar_time_frames in self.buffers.get(topic, {}).items():
            for bar_time_frame in bar_time_frames.keys():
                combinations.append({
                    'asset': asset,
                    'barTimeFrame': bar_time_frame
                })
        return combinations
    

    
    def get_buffer_stats(self, topic: str) -> Dict[str, Dict[int, BufferInfo]]:
        """Get buffer statistics for a specific topic"""
        stats = {}
        for asset, bar_time_frames in self.buffers.get(topic, {}).items():
            stats[asset] = {}
            for bar_time_frame, buffer in bar_time_frames.items():
                stats[asset][bar_time_frame] = buffer.get_window_info()
        return stats
    
    def get_all_buffer_stats(self) -> Dict[str, Dict[str, Dict[int, BufferInfo]]]:
        """Get buffer statistics for all topics"""
        all_stats = {}
        for topic, assets in self.buffers.items():
            all_stats[topic] = {}
            for asset, bar_time_frames in assets.items():
                all_stats[topic][asset] = {}
                for bar_time_frame, buffer in bar_time_frames.items():
                    all_stats[topic][asset][bar_time_frame] = buffer.get_window_info()
        return all_stats
    
    
    
    def get_all_topics(self) -> List[str]:
        """Get all topics"""
        # Topics are stored in regular dict, but typically few topics so sorting is fast
        return sorted(self.buffers.keys())
    
    def register_asset_data_listener(self, topic: str, asset: str, listener: Callable) -> Callable[[], None]:
        """
        Register a listener for AssetDataChanged events for specific topic/asset.
        Returns unsubscribe function.
        """
        self.asset_data_listeners[topic][asset].append(listener)
        
        def unsubscribe():
            if listener in self.asset_data_listeners[topic][asset]:
                self.asset_data_listeners[topic][asset].remove(listener)
        
        return unsubscribe
    
    def _fire_asset_data_changed(self, topic: str, asset: str, bar_time_frame: int, message: Any):
        """Fire AssetDataChanged event for topic/asset combination"""
        from .events import AssetDataChanged
        
        # Get current buffer info
        buffer_info = self.get_buffer_info(topic, asset, bar_time_frame)
        
        # Create event
        event = AssetDataChanged(
            topic=topic,
            asset=asset,
            bar_time_frame=bar_time_frame,
            message=message,
            buffer_info=buffer_info,
            timestamp=datetime.now()
        )
        
        # Notify listeners for this specific topic/asset
        for listener in self.asset_data_listeners[topic][asset]:
            try:
                listener(event)
            except Exception as e:
                self.logger.error(f"Error in AssetDataChanged listener: {e}")
    
    def _notify_listeners(self, topic: str, asset: str, bar_time_frame: int):
        """Notify all change listeners"""
        for listener in self.change_listeners:
            try:
                listener(topic, asset, bar_time_frame)
            except Exception as e:
                self.logger.error(f"Error in change listener: {e}")
    

    

    

    

    
 