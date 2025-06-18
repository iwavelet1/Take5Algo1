"""
Asset Trend Factory

This module provides the AssetTrendFactory class that listens for NewAssetEvent
instances and creates/manages AssetTrends instances for each unique asset.
"""

import logging
from typing import Dict, List, Optional, Callable
from datetime import datetime

from .events import NewAssetEvent
from .asset_trends import AssetTrends, TrendAnalysis

# Import buffer manager
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from asset_data.sliding_window_manager import AssetBufferManager


class AssetTrendFactory:
    """
    Factory class that manages AssetTrends instances.
    
    Listens for NewAssetEvent and automatically creates AssetTrends instances
    for new assets, registering them to track the appropriate topic/timeframe
    combinations.
    """
    
    def __init__(self, buffer_manager: AssetBufferManager):
        self.buffer_manager = buffer_manager
        self.logger = logging.getLogger(__name__)
        
        # Store AssetTrends instances by asset symbol
        self.asset_trends: Dict[str, AssetTrends] = {}
        
        # Track which asset/topic/timeframe combinations we've seen
        self.seen_combinations: set = set()
        
        # Statistics
        self.events_processed = 0
        self.assets_created = 0
        self.start_time = datetime.now()
        
        self.logger.info("🏭 AssetTrendFactory initialized")
    
    def handle_new_asset_event(self, event: NewAssetEvent) -> None:
        """
        Handle a NewAssetEvent by creating or updating AssetTrends instances.
        
        This method is designed to be registered as a listener for NewAssetEvent.
        """
        try:
            self.events_processed += 1
            
            self.logger.info(f"🎯 Processing {event}")
            
            # Create combination key for tracking
            combination_key = event.get_combination_key()
            
            # Skip if we've already processed this combination
            if combination_key in self.seen_combinations:
                self.logger.debug(f"Already tracking combination: {combination_key}")
                return
            
            # Get or create AssetTrends instance for this asset
            asset_trends = self._get_or_create_asset_trends(event.asset)
            
            # Register the new topic/timeframe combination
            asset_trends.add_topic_timeframe(event.topic, event.bar_time_frame)
            
            # Mark this combination as seen
            self.seen_combinations.add(combination_key)
            
            self.logger.info(
                f"✅ Asset trend tracking added: {event.asset} -> {event.topic}/{event.bar_time_frame}s"
            )
            
        except Exception as e:
            self.logger.error(f"Error handling NewAssetEvent {event}: {e}")
    
    def _get_or_create_asset_trends(self, asset: str) -> AssetTrends:
        """Get existing AssetTrends or create a new one for the asset"""
        if asset not in self.asset_trends:
            self.asset_trends[asset] = AssetTrends(asset, self.buffer_manager)
            self.assets_created += 1
            self.logger.info(f"📈 Created new AssetTrends instance for {asset}")
        
        return self.asset_trends[asset]
    
    def get_asset_trends(self, asset: str) -> Optional[AssetTrends]:
        """Get AssetTrends instance for a specific asset"""
        return self.asset_trends.get(asset)
    
    def get_all_assets(self) -> List[str]:
        """Get list of all assets being tracked"""
        return list(self.asset_trends.keys())
    
    def analyze_all_trends(self) -> Dict[str, List[TrendAnalysis]]:
        """
        Run trend analysis for all tracked assets.
        Returns dictionary of asset -> list of analyses.
        """
        results = {}
        
        for asset, trends_analyzer in self.asset_trends.items():
            try:
                analyses = trends_analyzer.analyze_trends()
                if analyses:
                    results[asset] = analyses
                    self.logger.debug(f"Analyzed {len(analyses)} trends for {asset}")
            except Exception as e:
                self.logger.error(f"Error analyzing trends for {asset}: {e}")
        
        return results
    
    def get_latest_analyses_for_asset(self, asset: str) -> List[TrendAnalysis]:
        """Get the latest trend analyses for a specific asset"""
        asset_trends = self.asset_trends.get(asset)
        if asset_trends:
            return asset_trends.get_all_latest_analyses()
        return []
    
    def get_all_latest_analyses(self) -> Dict[str, List[TrendAnalysis]]:
        """Get the latest trend analyses for all assets"""
        results = {}
        
        for asset, trends_analyzer in self.asset_trends.items():
            analyses = trends_analyzer.get_all_latest_analyses()
            if analyses:
                results[asset] = analyses
        
        return results
    
    def get_factory_status(self) -> Dict[str, any]:
        """Get status information about the factory"""
        uptime = datetime.now() - self.start_time
        
        return {
            "factory_uptime_seconds": uptime.total_seconds(),
            "events_processed": self.events_processed,
            "assets_created": self.assets_created,
            "active_assets": len(self.asset_trends),
            "tracked_combinations": len(self.seen_combinations),
            "asset_list": list(self.asset_trends.keys()),
            "combination_list": list(self.seen_combinations)
        }
    
    def get_comprehensive_status(self) -> Dict[str, any]:
        """Get comprehensive status including individual asset statuses"""
        status = self.get_factory_status()
        
        # Add individual asset statuses
        asset_statuses = {}
        for asset, trends_analyzer in self.asset_trends.items():
            asset_statuses[asset] = trends_analyzer.get_status()
        
        status["asset_statuses"] = asset_statuses
        return status
    
    def clear_asset(self, asset: str) -> bool:
        """Remove tracking for a specific asset"""
        if asset in self.asset_trends:
            # Remove combinations for this asset
            combinations_to_remove = [
                combo for combo in self.seen_combinations 
                if combo.startswith(f":{asset}:")
            ]
            for combo in combinations_to_remove:
                self.seen_combinations.discard(combo)
            
            # Remove the asset trends instance
            del self.asset_trends[asset]
            
            self.logger.info(f"🗑️  Removed asset tracking for {asset}")
            return True
        
        return False
    
    def clear_all(self) -> None:
        """Clear all asset tracking"""
        self.asset_trends.clear()
        self.seen_combinations.clear()
        self.events_processed = 0
        self.assets_created = 0
        self.start_time = datetime.now()
        
        self.logger.info("🧹 Cleared all asset trend tracking")


# Convenience function to create and register the factory
def create_asset_trend_factory(buffer_manager: AssetBufferManager) -> AssetTrendFactory:
    """
    Create an AssetTrendFactory and return it ready for event registration.
    
    Usage in main:
        factory = create_asset_trend_factory(buffer_manager)
        buffer_manager.register_new_asset_listener(factory.handle_new_asset_event)
    """
    return AssetTrendFactory(buffer_manager) 