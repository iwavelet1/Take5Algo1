"""
Asset Trend Factory

Manages AssetTrendProcessor instances. Creates new processors when 
new assets are detected from the asset_trends topic.
"""

import logging
from typing import Dict, Set, List
from asset_data import AssetBufferManager, AssetDataChanged
from .asset_trend_processor import AssetTrendProcessor, SignalAlgorithm
from .cross_calculator import CrossCalculator


class AssetTrendFactory:
    """
    Factory that manages AssetTrendProcessor instances.
    
    Monitors for new assets from asset_trends topic and automatically
    creates AssetTrendProcessor instances for them.
    """
    
    def __init__(self, app):
        self.app = app
        self.buffer_manager = app.get_global_buffer_manager()
        self.processors: Dict[str, AssetTrendProcessor] = {}
        self.seen_assets: Set[str] = set()
        
        # Static map of asset to algorithms
        self.asset_algorithms: Dict[str, List[SignalAlgorithm]] = self._create_asset_algorithm_map()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("🏭 AssetTrendFactory initialized")
        
        # Register to listen for ALL asset_trends events to detect new assets
        self._setup_asset_detection()
    
    def _create_asset_algorithm_map(self) -> Dict[str, List[SignalAlgorithm]]:
        """Create static map of assets to their algorithms"""
        # Asset-specific algorithm map
        asset_algorithm_map = {
            # Wildcard "*" applies to all assets
            "*": [CrossCalculator()],
            
            # Add specific assets and their algorithms here
            "NVDA": [CrossCalculator()],
            # Example: "SPY": [CrossCalculator(), SomeOtherAlgorithm()],
            # Specific asset entries override the wildcard
        }
        
        return asset_algorithm_map
    
    def _setup_asset_detection(self):
        """Setup detection for new assets in asset_trends topic"""
        # First, check for any existing assets in asset_trends topic
        self._check_existing_assets()
        
        # Then, register for future buffer changes to detect new assets
        self.buffer_manager.subscribe(self._handle_buffer_change)
    
    def _check_existing_assets(self):
        """Check for existing assets in asset_trends topic and create processors for them"""
        try:
            existing_assets = self.buffer_manager.get_all_assets("asset_trends")
            self.logger.info(f"🔍 Checking existing assets in asset_trends: {existing_assets}")
            
            for asset in existing_assets:
                if asset not in self.seen_assets:
                    self.logger.info(f"🆕 Found existing asset in asset_trends: {asset}")
                    self._create_processor_for_asset(asset)
                    self.seen_assets.add(asset)
                    
        except Exception as e:
            self.logger.error(f"Error checking existing assets: {e}")
    
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
        
        # Get algorithms for this asset
        algorithms = self.get_algorithms_for_asset(asset)
        
        # Create new processor with algorithm map
        processor = AssetTrendProcessor(asset, self.buffer_manager, self.asset_algorithms)
        
        # Start the processor
        processor.start()
        
        # Store the processor
        self.processors[asset] = processor
        
        self.logger.info(f"✅ Created and started AssetTrendProcessor for {asset} with {len(algorithms)} algorithms")
    
    def get_algorithms_for_asset(self, asset: str) -> List[SignalAlgorithm]:
        """Get algorithms for a specific asset"""
        algorithms = []
        
        # Add wildcard "*" algorithms (apply to all assets)
        wildcard_algorithms = self.asset_algorithms.get("*", [])
        algorithms.extend(wildcard_algorithms)
        
        # Add asset-specific algorithms
        asset_specific_algorithms = self.asset_algorithms.get(asset, [])
        algorithms.extend(asset_specific_algorithms)
        
        # Remove duplicates while preserving order
        seen = set()
        distinct_algorithms = []
        for algo in algorithms:
            algo_type = type(algo)
            if algo_type not in seen:
                seen.add(algo_type)
                distinct_algorithms.append(algo)
        
        return distinct_algorithms
    
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