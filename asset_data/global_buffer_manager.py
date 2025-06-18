"""
Global singleton BufferManager instance for asset data management.

This ensures all components use the same buffer manager instance,
matching the TypeScript GlobalBufferManager implementation.
"""

import logging
from .sliding_window_manager import AssetBufferManager

# Global singleton BufferManager instance
# This ensures all components use the same buffer manager instance
global_buffer_manager = AssetBufferManager(120)  # 2 hour sliding window

# Setup logging
logger = logging.getLogger(__name__)
logger.info('🌍 [GlobalBufferManager] Created global singleton BufferManager with 120-minute sliding windows')

# Export for easy import
__all__ = ['global_buffer_manager'] 