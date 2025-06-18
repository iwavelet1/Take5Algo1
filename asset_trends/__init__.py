"""
Asset Trends Module

This module provides asset trend analysis and factory pattern for creating
and managing asset trend instances based on new asset events.
"""

from .asset_trends import AssetTrends
from .asset_trend_factory import AssetTrendFactory
from .events import NewAssetEvent

__all__ = [
    'AssetTrends',
    'AssetTrendFactory', 
    'NewAssetEvent'
] 