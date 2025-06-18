"""
Enhanced Kafka Consumer with AssetBufferManager Integration

This module extends the existing Kafka consumer to feed data into the
AssetBufferManager instead of just dumping messages to console.
"""

import json
import logging
from typing import Dict, Callable, Any, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException

# Import existing Kafka components
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from kafka.config import KafkaConfig

# Import our buffer manager
from .global_buffer_manager import global_buffer_manager
from .sliding_window_manager import AssetBufferManager


class BufferManagedKafkaConsumer:
    """
    Enhanced Kafka consumer that feeds data into AssetBufferManager
    instead of just logging to console.
    """
    
    def __init__(self, config: KafkaConfig = None, buffer_manager: AssetBufferManager = None):
        # Use provided config or create default
        self.config = config or KafkaConfig()
        self.buffer_manager = buffer_manager or global_buffer_manager
        self.consumer = None
        self.running = False
        self.topics = self.config.topics
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Message handlers that feed into buffer manager
        self.message_handlers: Dict[str, Callable] = {
            "bar_trends": self._handle_bar_trends,
            "historic_stats": self._handle_historic_stats,
            "asset_trends": self._handle_asset_trends
        }
        
        # Statistics tracking
        self.message_count = 0
        self.processed_count = 0
        self.error_count = 0
    
    def _create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer"""
        return Consumer(self.config.get_consumer_config())
    
    def _handle_bar_trends(self, message: Any):
        """Handle bar_trends topic messages and feed to buffer manager"""
        topic = "bar_trends"
        self._process_message_to_buffer(message, topic)
    
    def _handle_historic_stats(self, message: Any):
        """Handle historic_stats topic messages and feed to buffer manager"""
        topic = "historic_stats"
        self._process_message_to_buffer(message, topic)
    
    def _handle_asset_trends(self, message: Any):
        """Handle asset_trends topic messages and feed to buffer manager"""
        topic = "asset_trends"
        self._process_message_to_buffer(message, topic)
    
    def _process_message_to_buffer(self, message: Any, topic: str):
        """Process Kafka message and add to buffer manager"""
        try:
            self.message_count += 1
            
            # Parse JSON message data
            message_data = None
            if message.value():
                try:
                    message_data = json.loads(message.value().decode('utf-8'))
                except json.JSONDecodeError:
                    self.logger.warning(f"Failed to parse JSON for {topic}: {message.value()}")
                    self.error_count += 1
                    return
            
            if not message_data:
                self.logger.warning(f"Empty message data for {topic}")
                self.error_count += 1
                return
            
            # Extract asset and barTimeFrame
            asset = self._extract_asset(message_data)
            bar_time_frame = self._extract_bar_time_frame(message_data)
            
            if not asset:
                self.logger.warning(f"No asset found in {topic} message: {message_data}")
                self.error_count += 1
                return
                
            if not bar_time_frame:
                self.logger.warning(f"No barTimeFrame found in {topic} message: {message_data}")
                self.error_count += 1
                return
            
            # Add to buffer manager
            success = self.buffer_manager.add_message(topic, asset, bar_time_frame, message_data)
            
            if success:
                self.processed_count += 1
                self.logger.debug(
                    f"✅ [{topic.upper()}] Added to buffer: {asset}:{bar_time_frame} "
                    f"(Total processed: {self.processed_count})"
                )
            else:
                self.logger.debug(f"📝 [{topic.upper()}] Buffer unchanged: {asset}:{bar_time_frame}")
            
            # Log progress every 100 messages
            if self.message_count % 100 == 0:
                self.logger.info(
                    f"📊 Kafka Consumer Stats - Messages: {self.message_count}, "
                    f"Processed: {self.processed_count}, Errors: {self.error_count}"
                )
                
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"Error processing {topic} message: {e}", exc_info=True)
    
    def _extract_asset(self, message_data: dict) -> Optional[str]:
        """Extract asset symbol from message data"""
        return message_data.get('symbol')
    
    def _extract_bar_time_frame(self, message_data: dict) -> Optional[int]:
        """Extract barTimeFrame from message data"""
        try:
            bar_data = message_data.get('barData', {})
            bar_time_frame = bar_data.get('barTimeFrame')
            if isinstance(bar_time_frame, (int, float)) and bar_time_frame > 0:
                return int(bar_time_frame)
            return None
        except (AttributeError, TypeError, ValueError):
            return None
    
    def start(self):
        """Start the Kafka consumer with buffer management"""
        if self.running:
            self.logger.warning("Consumer is already running")
            return
            
        try:
            self.consumer = self._create_consumer()
            self.consumer.subscribe(self.topics)
            self.running = True
            
            self.logger.info(f"🚀 Starting BufferManagedKafkaConsumer for topics: {self.topics}")
            self.logger.info(f"📡 Bootstrap servers: {self.config.bootstrap_servers}")
            self.logger.info(f"🗄️  Buffer manager window: {self.buffer_manager.window_size_minutes} minutes")
            print(f"🚀 Kafka Consumer with Buffer Management started")
            print(f"📋 Topics: {self.topics}")
            print(f"📡 Bootstrap servers: {self.config.bootstrap_servers}")
            print(f"🗄️  Sliding window: {self.buffer_manager.window_size_minutes} minutes")
            print("=" * 80)
            
            while self.running:
                try:
                    # Poll for messages
                    message = self.consumer.poll(timeout=1.0)
                    
                    if message is None:
                        continue
                        
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition - not an error
                            continue
                        else:
                            self.logger.error(f"Consumer error: {message.error()}")
                            continue
                    
                    # Process the message
                    topic = message.topic()
                    if topic in self.message_handlers:
                        self.message_handlers[topic](message)
                    else:
                        self.logger.warning(f"No handler for topic: {topic}")
                        
                except KeyboardInterrupt:
                    self.logger.info("Received interrupt signal")
                    break
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    
        except KafkaException as e:
            self.logger.error(f"Kafka exception: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the Kafka consumer"""
        if not self.running:
            return
            
        self.running = False
        if self.consumer:
            self.consumer.close()
            
        # Print final statistics
        self.logger.info("🛑 BufferManagedKafkaConsumer stopped")
        self.logger.info(
            f"📊 Final Stats - Messages: {self.message_count}, "
            f"Processed: {self.processed_count}, Errors: {self.error_count}"
        )
        print("🛑 Kafka Consumer with Buffer Management stopped")
        print(f"📊 Final Stats - Messages: {self.message_count}, "
              f"Processed: {self.processed_count}, Errors: {self.error_count}")
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get current buffer statistics"""
        return {
            'kafka_messages_received': self.message_count,
            'kafka_messages_processed': self.processed_count,
            'kafka_errors': self.error_count,
            'buffer_stats': self.buffer_manager.get_all_buffer_stats(),
            'all_topics': self.buffer_manager.get_all_topics(),
            'total_combinations': len(self.buffer_manager.get_all_combinations_all_topics())
        }
    
    def print_buffer_summary(self):
        """Print a summary of current buffer state"""
        print("\n" + "=" * 80)
        print("📊 BUFFER MANAGER SUMMARY")
        print("=" * 80)
        
        stats = self.get_buffer_stats()
        print(f"🔢 Kafka Messages Received: {stats['kafka_messages_received']}")
        print(f"✅ Messages Processed: {stats['kafka_messages_processed']}")
        print(f"❌ Errors: {stats['kafka_errors']}")
        print(f"🗂️  Active Topics: {len(stats['all_topics'])}")
        print(f"📈 Total Asset/TimeFrame Combinations: {stats['total_combinations']}")
        
        if stats['all_topics']:
            print(f"\nActive Topics: {', '.join(stats['all_topics'])}")
            
            for topic in stats['all_topics']:
                combinations = self.buffer_manager.get_all_combinations(topic)
                if combinations:
                    print(f"\n📋 {topic.upper()} ({len(combinations)} combinations):")
                    for combo in combinations[:5]:  # Show first 5
                        buffer_info = self.buffer_manager.get_buffer_info(
                            topic, combo['asset'], combo['barTimeFrame']
                        )
                        if buffer_info:
                            print(f"   • {combo['asset']}:{combo['barTimeFrame']}s "
                                  f"- {buffer_info.point_count} points")
                    if len(combinations) > 5:
                        print(f"   ... and {len(combinations) - 5} more")
        
        print("=" * 80) 