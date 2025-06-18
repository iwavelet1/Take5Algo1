#!/usr/bin/env python3
"""
Temporary script to add NewAssetEvent support to sliding_window_manager.py
"""

import re

# Read the file
with open('asset_data/sliding_window_manager.py', 'r') as f:
    content = f.read()

# Add the new lines after change_listeners
new_lines = """        
        # NewAssetEvent listeners
        self.new_asset_listeners: Set[Callable] = set()
        
        # Track known asset/topic/timeframe combinations to detect new ones
        self.known_combinations: Set[str] = set()"""

# Find and replace
pattern = r'(        self\.change_listeners: Set\[Callable\[\[str, str, int\], None\]\] = set\(\))'
replacement = r'\1' + new_lines

content = re.sub(pattern, replacement, content)

# Now add the new methods after the subscribe method
new_methods = '''
    def register_new_asset_listener(self, listener: Callable) -> Callable[[], None]:
        """
        Register a listener for NewAssetEvent notifications.
        Returns unsubscribe function.
        """
        self.new_asset_listeners.add(listener)
        self.logger.info(f"📢 Registered NewAssetEvent listener: {listener.__name__}")
        
        def unsubscribe():
            self.new_asset_listeners.discard(listener)
            self.logger.info(f"🔇 Unregistered NewAssetEvent listener: {listener.__name__}")
        
        return unsubscribe
    
    def _notify_new_asset_event(self, topic: str, asset: str, bar_time_frame: int, complete_message: Any):
        """Notify listeners about new asset combinations"""
        if NewAssetEvent is None:
            return  # Skip if NewAssetEvent not available
        
        combination_key = f"{topic}:{asset}:{bar_time_frame}"
        
        # Only notify for truly new combinations
        if combination_key not in self.known_combinations:
            self.known_combinations.add(combination_key)
            
            # Create and send event
            event = NewAssetEvent(
                topic=topic,
                asset=asset,
                bar_time_frame=bar_time_frame,
                first_message=complete_message,
                timestamp=datetime.now(),
                metadata={
                    "buffer_manager_id": id(self),
                    "combination_count": len(self.known_combinations)
                }
            )
            
            # Notify all listeners
            for listener in self.new_asset_listeners:
                try:
                    listener(event)
                except Exception as e:
                    self.logger.error(f"Error in NewAssetEvent listener {listener.__name__}: {e}")
            
            self.logger.info(f"🎯 Notified {len(self.new_asset_listeners)} listeners of new asset: {combination_key}")

'''

# Add the new methods after the subscribe method
pattern = r'(        return unsubscribe\s+)(    def get_all_combinations)'
replacement = r'\1' + new_methods + r'\2'
content = re.sub(pattern, replacement, content, flags=re.DOTALL)

# Update the add_message method to call _notify_new_asset_event
pattern = r'(        if bar_time_frame not in self\.buffers\[topic\]\[asset\]:\s+)(            self\.buffers\[topic\]\[asset\]\[bar_time_frame\] = TimeBasedSlidingWindow\()'
replacement = r'\1            # This is a new combination - notify listeners before creating buffer\n            self._notify_new_asset_event(topic, asset, bar_time_frame, complete_message)\n            \n\2'
content = re.sub(pattern, replacement, content, flags=re.DOTALL)

# Write the file back
with open('asset_data/sliding_window_manager.py', 'w') as f:
    f.write(content)

print("✅ Added NewAssetEvent support to sliding_window_manager.py") 