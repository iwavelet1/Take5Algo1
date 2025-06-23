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
import threading
import asyncio
from datetime import datetime
import time


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
        
        # Server guarantees correct order, but keep basic validation
        if publish_time < self.latest_time:
            self.logger.warning(
                f"Unexpected older message for {self.asset}:{self.bar_time_frame}. "
                f"Latest: {self.latest_time}, Received: {publish_time}"
            )
            # Accept anyway since server should be handling order
        
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
    
    def clear(self):
        """Clear the buffer"""
        self.buffer.clear()
        self.latest_time = 0
    
    def _extract_publish_time(self, message: Any) -> Optional[int]:
        """Extract publishTime.take5Time from message"""
        try:
            if isinstance(message, dict):
                return message.get('publishTime', {}).get('take5Time')
            elif hasattr(message, 'publishTime'):
                if hasattr(message.publishTime, 'take5Time'):
                    return message.publishTime.take5Time
                elif isinstance(message.publishTime, dict):
                    return message.publishTime.get('take5Time')
            return None
        except (AttributeError, TypeError):
            return None
    
    def _extract_time_str(self, message: Any) -> Optional[str]:
        """Extract publishTime.take5TimeStr from message"""
        try:
            if isinstance(message, dict):
                return message.get('publishTime', {}).get('take5TimeStr')
            elif hasattr(message, 'publishTime'):
                if hasattr(message.publishTime, 'take5TimeStr'):
                    return message.publishTime.take5TimeStr
                elif isinstance(message.publishTime, dict):
                    return message.publishTime.get('take5TimeStr')
            return None
        except (AttributeError, TypeError):
            return None


class AssetBufferManager:
    """
    Global buffer manager for time-based sliding windows.
    Matches the TypeScript AssetBufferManager implementation.
    
    Structure: Topic → Asset → BarTimeFrame → Buffer
    """
    
    def __init__(self, window_size_minutes: int = 120):
        self.window_size_minutes = window_size_minutes
        # Topic → Asset → BarTimeFrame → Buffer
        self.buffers: Dict[str, Dict[str, Dict[int, TimeBasedSlidingWindow]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        self.change_listeners: Set[Callable[[str, str, int], None]] = set()
        
        # Event system for AssetDataChanged
        self.asset_data_listeners: Dict[str, Dict[str, List[Callable]]] = defaultdict(lambda: defaultdict(list))
        # Structure: topic -> asset -> [listener_functions]
        
        # Message queue for batch processing
        self.message_queue: List[List[Any]] = []
        self.queue_lock = threading.Lock()
        self.is_processing = False
        self.processing_lock = threading.Lock()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"AssetBufferManager initialized with {window_size_minutes} minute sliding windows")
    
    async def push_messages(self, messages: List[Any]) -> None:
        """
        WebSocket/Kafka pushes entire message array (thread-safe with minimal locking)
        """
        if not messages:
            return
        
        # Quick lock, push, unlock
        with self.queue_lock:
            self.message_queue.append(messages.copy())
        
        self.logger.debug(f"Queued {len(messages)} messages. Queue size: {len(self.message_queue)}")
        
        # Trigger processing (no lock needed)
        await self._process_queue()
    
    def push_messages_sync(self, messages: List[Any]) -> None:
        """Synchronous version of push_messages for non-async contexts"""
        if not messages:
            return
            
        with self.queue_lock:
            self.message_queue.append(messages.copy())
        
        self.logger.debug(f"Queued {len(messages)} messages. Queue size: {len(self.message_queue)}")
        
        # Process immediately in sync mode
        self._process_queue_sync()
    
    async def _process_queue(self) -> None:
        """Process queued message batches (single-threaded processor)"""
        with self.processing_lock:
            if self.is_processing:
                return  # Already processing
            self.is_processing = True
        
        try:
            while True:
                message_batch = None
                with self.queue_lock:
                    if not self.message_queue:
                        break
                    message_batch = self.message_queue.pop(0)
                
                if message_batch:
                    self.logger.debug(f"Processing batch of {len(message_batch)} messages")
                    processed_count = 0
                    
                    for ws_message in message_batch:
                        try:
                            # Extract data from WebSocket/Kafka message
                            message_data = self._extract_message_data(ws_message)
                            topic = self._extract_topic(ws_message)
                            
                            # Extract asset and barTimeFrame from message data
                            asset = self._extract_asset(message_data)
                            bar_time_frame = self._extract_bar_time_frame(message_data, topic)
                            
                            if asset and bar_time_frame and topic:
                                success = self.add_message(topic, asset, bar_time_frame, message_data)
                                if success:
                                    processed_count += 1
                        except Exception as e:
                            self.logger.error(f"Error processing message in batch: {e}")
                    
                    self.logger.debug(f"Successfully processed {processed_count}/{len(message_batch)} messages")
        finally:
            with self.processing_lock:
                self.is_processing = False
    
    def _process_queue_sync(self) -> None:
        """Synchronous version of _process_queue"""
        with self.processing_lock:
            if self.is_processing:
                return
            self.is_processing = True
        
        try:
            while True:
                message_batch = None
                with self.queue_lock:
                    if not self.message_queue:
                        break
                    message_batch = self.message_queue.pop(0)
                
                if message_batch:
                    self.logger.debug(f"Processing batch of {len(message_batch)} messages")
                    processed_count = 0
                    
                    for message in message_batch:
                        try:
                            # For direct message processing (not wrapped in WebSocket envelope)
                            if isinstance(message, dict):
                                # Extract asset and barTimeFrame from message data
                                asset = self._extract_asset(message)
                                bar_time_frame = self._extract_bar_time_frame(message, None)
                                topic = self._infer_topic_from_message(message)
                                
                                if asset and bar_time_frame and topic:
                                    success = self.add_message(topic, asset, bar_time_frame, message)
                                    if success:
                                        processed_count += 1
                        except Exception as e:
                            self.logger.error(f"Error processing message in sync batch: {e}")
                    
                    self.logger.debug(f"Successfully processed {processed_count}/{len(message_batch)} messages")
        finally:
            with self.processing_lock:
                self.is_processing = False
    
    def add_message(self, topic: str, asset: str, bar_time_frame: int, complete_message: Any) -> bool:
        """Add a message to the appropriate buffer"""
        # Get or create buffer
        if topic not in self.buffers:
            self.buffers[topic] = defaultdict(dict)
        if asset not in self.buffers[topic]:
            self.buffers[topic][asset] = {}
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
        buffer = self.buffers.get(topic, {}).get(asset, {}).get(bar_time_frame)
        return buffer.get_messages() if buffer else []
    
    def get_current_message(self, topic: str, asset: str, bar_time_frame: int) -> Optional[Any]:
        """Get the current (latest) message for a specific combination"""
        buffer = self.buffers.get(topic, {}).get(asset, {}).get(bar_time_frame)
        return buffer.get_current_message() if buffer else None
    
    def get_all_assets(self, topic: str) -> List[str]:
        """Get all assets for a topic"""
        return sorted(self.buffers.get(topic, {}).keys())
    
    def get_bar_time_frames(self, topic: str, asset: str) -> List[int]:
        """Get all bar time frames for a topic/asset combination"""
        return sorted(self.buffers.get(topic, {}).get(asset, {}).keys())
    
    def get_buffer_info(self, topic: str, asset: str, bar_time_frame: int) -> Optional[BufferInfo]:
        """Get buffer information for a specific combination"""
        buffer = self.buffers.get(topic, {}).get(asset, {}).get(bar_time_frame)
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
    
    def get_all_combinations_all_topics(self) -> List[Dict[str, Any]]:
        """Get all topic/asset/barTimeFrame combinations"""
        combinations = []
        for topic, assets in self.buffers.items():
            for asset, bar_time_frames in assets.items():
                for bar_time_frame in bar_time_frames.keys():
                    combinations.append({
                        'topic': topic,
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
    
    def clear_buffer(self, topic: str, asset: str, bar_time_frame: Optional[int] = None) -> bool:
        """Clear specific buffer(s)"""
        try:
            if topic not in self.buffers:
                return False
                
            if asset not in self.buffers[topic]:
                return False
            
            if bar_time_frame is None:
                # Clear all bar time frames for this asset
                for buffer in self.buffers[topic][asset].values():
                    buffer.clear()
                self.buffers[topic][asset].clear()
                return True
            else:
                # Clear specific buffer
                if bar_time_frame in self.buffers[topic][asset]:
                    self.buffers[topic][asset][bar_time_frame].clear()
                    del self.buffers[topic][asset][bar_time_frame]
                    return True
                return False
        except Exception as e:
            self.logger.error(f"Error clearing buffer: {e}")
            return False
    
    def clear_topic(self, topic: str) -> bool:
        """Clear all buffers for a topic"""
        try:
            if topic in self.buffers:
                for assets in self.buffers[topic].values():
                    for buffer in assets.values():
                        buffer.clear()
                del self.buffers[topic]
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error clearing topic: {e}")
            return False
    
    def clear_all_buffers(self) -> None:
        """Clear all buffers"""
        try:
            for assets in self.buffers.values():
                for bar_time_frames in assets.values():
                    for buffer in bar_time_frames.values():
                        buffer.clear()
            self.buffers.clear()
            self.logger.info("All buffers cleared")
        except Exception as e:
            self.logger.error(f"Error clearing all buffers: {e}")
    
    def get_all_topics(self) -> List[str]:
        """Get all topics"""
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
    
    def _extract_message_data(self, message: Any) -> Any:
        """Extract message data from WebSocket/Kafka wrapper"""
        if isinstance(message, dict):
            return message.get('data', message)
        elif hasattr(message, 'data'):
            return message.data
        return message
    
    def _extract_topic(self, message: Any) -> Optional[str]:
        """Extract topic from WebSocket/Kafka wrapper"""
        if isinstance(message, dict):
            return message.get('topic')
        elif hasattr(message, 'topic'):
            return message.topic
        return None
    
    def _extract_asset(self, message: Any) -> Optional[str]:
        """Extract asset from message data"""
        try:
            if isinstance(message, dict):
                return message.get('symbol')
            elif hasattr(message, 'symbol'):
                return message.symbol
            return None
        except (AttributeError, TypeError):
            return None
    
    def _extract_bar_time_frame(self, message: Any, topic: Optional[str]) -> Optional[int]:
        """Extract barTimeFrame from message data"""
        try:
            if isinstance(message, dict):
                # All topics use the same structure: message.barData.barTimeFrame
                bar_time_frame = message.get('barData', {}).get('barTimeFrame')
                if isinstance(bar_time_frame, (int, float)) and bar_time_frame > 0:
                    return int(bar_time_frame)
            elif hasattr(message, 'barData'):
                if hasattr(message.barData, 'barTimeFrame'):
                    return int(message.barData.barTimeFrame)
            return None
        except (AttributeError, TypeError, ValueError):
            return None
    
    def _infer_topic_from_message(self, message: Any) -> Optional[str]:
        """Infer topic from message structure when not provided"""
        # This is a fallback - ideally topic should always be provided
        if isinstance(message, dict):
            # Look for specific fields that indicate message type
            if 'lastSampleStats' in message:
                return 'bar_trends'
            elif 'stats' in message and 'z_score' in message:
                return 'historic_stats'
            elif 'trendData' in message:
                return 'asset_trends'
        return 'unknown' 