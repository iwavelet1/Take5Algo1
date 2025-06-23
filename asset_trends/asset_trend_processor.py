"""
Asset Trend Processor

Processes asset_trends topic data for individual assets.
Each processor runs in its own thread and handles events for one asset.
"""

import logging
import threading
from typing import Optional, List
from datetime import datetime
from asset_data import AssetBufferManager, AssetDataChanged


class SignalAlgorithm:
    """Base class for signal detection algorithms"""
    
    def process(self, event, data):
        """
        Process event and data to detect signals
        
        Args:
            event: AssetDataChanged event
            data: Topic/asset data from buffer manager
        """
        raise NotImplementedError("Subclasses must implement process method")


class AssetTrendProcessor:
    """
    Processes asset_trends data for a specific asset.
    
    Each processor:
    - Registers for AssetDataChanged events for asset_trends topic
    - Runs in its own thread
    - Wakes up on asset changes and gets latest state
    - Runs signal algorithms on the latest data
    - Prints API-format data with current state
    """
    
    def __init__(self, asset: str, buffer_manager: AssetBufferManager, asset_algorithms: dict):
        self.asset = asset
        self.buffer_manager = buffer_manager
        self.asset_algorithms = asset_algorithms
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.unsubscribe_func: Optional[callable] = None
        
        # Event signals for thread wake-up
        self.asset_changed_event = threading.Event()
        self.shutdown_event = threading.Event()
        self.latest_event: Optional[AssetDataChanged] = None
        
        self.logger = logging.getLogger(f"{__name__}.{asset}")
        self.logger.info(f"🎯 AssetTrendProcessor created for {asset}")
    

    
    def start(self):
        """Start the processor thread and register for events"""
        if self.running:
            self.logger.warning(f"AssetTrendProcessor for {self.asset} is already running")
            return
        
        self.running = True
        
        # Register for AssetDataChanged events for asset_trends topic
        self.unsubscribe_func = self.buffer_manager.register_asset_data_listener(
            "asset_trends", self.asset, self._on_asset_data_changed
        )
        
        # Start the processor thread
        self.thread = threading.Thread(
            target=self._run,
            name=f"AssetTrendProcessor-{self.asset}",
            daemon=True
        )
        self.thread.start()
        
        self.logger.info(f"🧵 AssetTrendProcessor thread started for {self.asset}")
    
    def stop(self):
        """Stop the processor thread and unregister from events"""
        if not self.running:
            return
        
        self.running = False
        
        # Signal thread to shutdown
        self.shutdown_event.set()
        
        # Unregister from events
        if self.unsubscribe_func:
            self.unsubscribe_func()
            self.unsubscribe_func = None
        
        # Wait for thread to finish
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        self.logger.info(f"🛑 AssetTrendProcessor stopped for {self.asset}")
    
    def _run(self):
        """Main thread loop - waits for asset change events"""
        self.logger.info(f"🏃 AssetTrendProcessor thread running for {self.asset}")
        
        try:
            while not self.shutdown_event.is_set():
                # Wait for asset change event (with timeout to check shutdown)
                if self.asset_changed_event.wait(timeout=1.0):
                    # Asset changed - run algorithms and print API format
                    if self.latest_event:
                        self._run_algorithms(self.latest_event)
                    # Clear the event for next signal
                    self.asset_changed_event.clear()
                    
        except Exception as e:
            self.logger.error(f"Error in AssetTrendProcessor thread for {self.asset}: {e}")
        finally:
            self.logger.info(f"🏁 AssetTrendProcessor thread finished for {self.asset}")
    
    def _on_asset_data_changed(self, event: AssetDataChanged):
        """Signal thread that asset data changed"""
        try:
            # Store the event for the thread to process
            self.latest_event = event
            # Signal the thread to wake up and process latest state
            self.asset_changed_event.set()
            self.logger.info(f"📡 AssetTrendProcessor received message for {self.asset} - waking up thread")
            
        except Exception as e:
            self.logger.error(f"Error signaling asset change for {self.asset}: {e}")

    def _run_algorithms(self, event: AssetDataChanged):
        """Run all registered algorithms and print API format"""
        try:
            # Get all data for this topic/asset combination once
            topic_asset_data = self.buffer_manager.get_asset_from_topic(event.topic, event.asset)
            
                        # Get distinct algorithms for this asset (wildcard + asset-specific)
            algorithms = list({type(algo): algo for algo in self.asset_algorithms.get("*", []) + self.asset_algorithms.get(event.asset, [])}.values())
            
            # Run all signal algorithms with the data
            for algo in algorithms:
                try:
                    algo.process(event, topic_asset_data)
                except Exception as e:
                    self.logger.error(f"Error in algorithm {algo.__class__.__name__} for {event.asset}: {e}")
            
            # Print current state in API format using the event's buffer_info
            self._print_api_format(event.buffer_info, event.bar_time_frame)
                
        except Exception as e:
            self.logger.error(f"Error processing latest state for {self.asset}: {e}")
    
    def _print_api_format(self, buffer_info, bar_time_frame: int):
        """Print the current state in the same format as asset_trends API"""
        timestamp = int(datetime.now().timestamp() * 1000)
        
        api_record = {
            "asset": self.asset,
            "barTimeFrame": bar_time_frame,
            "lastUpdateTime": buffer_info.latest_time,
            "numRecords": buffer_info.point_count
        }
        
        # Print in API response format
        print(f"📈 AssetTrends API Format - {self.asset} [Thread: {threading.current_thread().name}]:")
        print(f"   Asset: {api_record['asset']}")
        print(f"   BarTimeFrame: {api_record['barTimeFrame']}")
        print(f"   LastUpdateTime: {api_record['lastUpdateTime']}")
        print(f"   NumRecords: {api_record['numRecords']}")
        print(f"   Timestamp: {timestamp}")
        print()
    
    def get_status(self) -> dict:
        """Get status information about this processor"""
        algorithms = self.asset_algorithms.get(self.asset, [])
        return {
            "asset": self.asset,
            "running": self.running,
            "thread_alive": self.thread.is_alive() if self.thread else False,
            "registered_for_events": self.unsubscribe_func is not None,
            "waiting_for_changes": not self.asset_changed_event.is_set(),
            "num_algorithms": len(algorithms)
        } 