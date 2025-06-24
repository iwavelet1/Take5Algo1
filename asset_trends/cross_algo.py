"""
Cross Algorithm - Outer Layer

Implements the outer layer that gets all datasets related to an asset and processes them.
"""

import logging
from typing import Dict, Any
from .asset_trend_processor import SignalAlgorithm
from .cross_calculator import CrossCalculator


class CrossAlgo(SignalAlgorithm):
    """
    Outer layer cross algorithm that handles dataset retrieval and processing.
    Gets all datasets related to an asset (currently 4 but code doesn't care about the count).
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.CrossAlgo")
        self.calculator = CrossCalculator()
    
    def process(self, event, data):
        """
        Process cross signals using event and pre-retrieved data.
        
        Args:
            event: AssetDataChanged event
            data: Dict mapping barTimeFrame -> TimeBasedSlidingWindow
            
        Returns:
            List of cross calculation results
        """
        # Get latest record from each timeframe
        records = {}
        for timeframe, window in data.items():
            current_msg = window.get_current_message()
            if current_msg:
                records[timeframe] = current_msg
        
        # Sort timeframes to get consistent ordering
        timeframes = sorted(records.keys())
        
        cross_results = []
        
        # Call calculator for 1-3 pair (1st & 3rd timeframes)
        if len(timeframes) >= 3:
            result_1_3 = self.calculator.calc(records[timeframes[0]], records[timeframes[2]])
            if result_1_3:
                cross_results.append(result_1_3)
        
        # Call calculator for 2-4 pair (2nd & 4th timeframes)
        if len(timeframes) >= 4:
            result_2_4 = self.calculator.calc(records[timeframes[1]], records[timeframes[3]])
            if result_2_4:
                cross_results.append(result_2_4)
        
        return cross_results 