"""
Asset Trends Processing Module

This module provides processors for handling asset_trends topic data.
"""

from .asset_trend_processor import AssetTrendProcessor
from .asset_trend_factory import AssetTrendFactory

__all__ = [
    'AssetTrendProcessor',
    'AssetTrendFactory'
] 