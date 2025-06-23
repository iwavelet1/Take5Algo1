"""
Asset Trend Factory

Manages AssetTrendProcessor instances. Creates new processors when 
new assets are detected from the asset_trends topic.
"""

import logging
from typing import Dict, Set
from asset_data import AssetBufferManager, AssetDataChanged
from .asset_trend_processor import AssetTrendProcessor


class AssetTrendFactory:
    """
    Factory that manages AssetTrendProcessor instances.
    
    Monitors for new assets from asset_trends topic and automatically
    creates AssetTrendProcessor instances for them.
    """
    
    def __init__(self, buffer_manager: AssetBufferManager):
        self.buffer_manager = buffer_manager
        self.processors: Dict[str, AssetTrendProcessor] = {}
        self.seen_assets: Set[str] = set()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("🏭 AssetTrendFactory initialized")
        
        # Register to listen for ALL asset_trends events to detect new assets
        self._setup_asset_detection()
    
    def _setup_asset_detection(self):
        """Setup detection for new assets in asset_trends topic"""
        # We need to register for all assets, but the current API only allows specific asset registration
        # For now, we'll use the existing change listener system to detect new assets
        self.buffer_manager.subscribe(self._handle_buffer_change)
    
    def _handle_buffer_change(self, topic: str, asset: str, bar_time_frame: int):
        """Handle buffer changes to detect new assets from asset_trends topic"""
        if topic == "asset_trends" and asset not in self.seen_assets:
            self.logger.info(f"🆕 New asset detected in asset_trends: {asset}")
            self._create_processor_for_asset(asset)
            self.seen_assets.add(asset)
    
    def _create_processor_for_asset(self, asset: str):
        """Create and start a new AssetTrendProcessor for the given asset"""
        if asset in self.processors:
            self.logger.warning(f"AssetTrendProcessor already exists for {asset}")
            return
        
        # Create new processor
        processor = AssetTrendProcessor(asset, self.buffer_manager)
        
        # Start the processor
        processor.start()
        
        # Store the processor
        self.processors[asset] = processor
        
        self.logger.info(f"✅ Created and started AssetTrendProcessor for {asset}")
    
    def get_processor(self, asset: str) -> AssetTrendProcessor:
        """Get the processor for a specific asset"""
        return self.processors.get(asset)
    
    def get_all_assets(self) -> list:
        """Get list of all assets being processed"""
        return list(self.processors.keys())
    
    def stop_processor(self, asset: str):
        """Stop the processor for a specific asset"""
        if asset in self.processors:
            self.processors[asset].stop()
            del self.processors[asset]
            self.seen_assets.discard(asset)
            self.logger.info(f"🛑 Stopped AssetTrendProcessor for {asset}")
    
    def stop_all_processors(self):
        """Stop all processors"""
        for asset in list(self.processors.keys()):
            self.stop_processor(asset)
        self.logger.info("🛑 All AssetTrendProcessors stopped")
    
    def get_factory_status(self) -> dict:
        """Get status of the factory and all processors"""
        processor_statuses = {}
        for asset, processor in self.processors.items():
            processor_statuses[asset] = processor.get_status()
        
        return {
            "total_processors": len(self.processors),
            "seen_assets": list(self.seen_assets),
            "processors": processor_statuses
        } 