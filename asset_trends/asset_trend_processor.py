"""
Asset Trend Processor

Processes asset_trends topic data for individual assets.
Each processor runs in its own thread and handles events for one asset.
"""

import logging
import threading
from typing import Optional
from datetime import datetime
from asset_data import AssetBufferManager, AssetDataChanged


class AssetTrendProcessor:
    """
    Processes asset_trends data for a specific asset.
    
    Each processor:
    - Registers for AssetDataChanged events for asset_trends topic
    - Runs in its own thread
    - Wakes up on asset changes and gets latest state
    - Prints API-format data with current state
    """
    
    def __init__(self, asset: str, buffer_manager: AssetBufferManager):
        self.asset = asset
        self.buffer_manager = buffer_manager
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.unsubscribe_func: Optional[callable] = None
        
        # Event signals for thread wake-up
        self.asset_changed_event = threading.Event()
        self.shutdown_event = threading.Event()
        
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
                    # Asset changed - process latest state
                    self._process_latest_state()
                    # Clear the event for next signal
                    self.asset_changed_event.clear()
                    
        except Exception as e:
            self.logger.error(f"Error in AssetTrendProcessor thread for {self.asset}: {e}")
        finally:
            self.logger.info(f"🏁 AssetTrendProcessor thread finished for {self.asset}")
    
    def _on_asset_data_changed(self, event: AssetDataChanged):
        """Signal thread that asset data changed"""
        try:
            # Signal the thread to wake up and process latest state
            self.asset_changed_event.set()
            self.logger.debug(f"📡 Asset change signal sent for {self.asset}")
            
        except Exception as e:
            self.logger.error(f"Error signaling asset change for {self.asset}: {e}")
    
    def _process_latest_state(self):
        """Process the latest state of the asset (runs in processor thread)"""
        try:
            # Get latest buffer info for this asset from asset_trends topic
            buffer_info = self.buffer_manager.get_buffer_info("asset_trends", self.asset)
            
            if buffer_info:
                # Print current state in API format
                self._print_api_format(buffer_info)
            else:
                self.logger.warning(f"No buffer info found for asset_trends/{self.asset}")
                
        except Exception as e:
            self.logger.error(f"Error processing latest state for {self.asset}: {e}")
    
    def _print_api_format(self, buffer_info):
        """Print the current state in the same format as asset_trends API"""
        timestamp = int(datetime.now().timestamp() * 1000)
        
        # Extract bar time frame from buffer info (assuming it's available)
        # This might need adjustment based on actual buffer_info structure
        bar_time_frame = getattr(buffer_info, 'bar_time_frame', 'UNKNOWN')
        
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
        return {
            "asset": self.asset,
            "running": self.running,
            "thread_alive": self.thread.is_alive() if self.thread else False,
            "registered_for_events": self.unsubscribe_func is not None,
            "waiting_for_changes": not self.asset_changed_event.is_set()
        } 