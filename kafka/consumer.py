import json
import logging
import threading
from typing import Dict, Callable, Any
from confluent_kafka import Consumer, KafkaError, KafkaException
from .config import KafkaConfig
from asset_data.global_buffer_manager import global_buffer_manager

class Take5KafkaConsumer:
    """
    Kafka consumer that listens to the same topics as the WebSocket publisher:
    - bar_trends: BarTrends messages
    - historic_stats: BarHistoricTrends messages  
    - asset_trends: AssetTrendGraph messages
    """
    
    def __init__(self, config: KafkaConfig = None):
        # Use provided config or create default
        self.config = config or KafkaConfig()
        self.consumer = None
        self.running = False
        self.topics = self.config.topics
        self.message_handlers: Dict[str, Callable] = {}
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Default message handlers that dump to console
        self.message_handlers = {
            "bar_trends": self._handle_bar_trends,
            "historic_stats": self._handle_historic_stats,
            "asset_trends": self._handle_asset_trends
        }
    
    def _create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer"""
        return Consumer(self.config.get_consumer_config())
    
    def _handle_bar_trends(self, message: Any):
        """Handle bar_trends topic messages (BarTrends)"""
        try:
            # Parse JSON and feed to buffer manager
            if message.value():
                data = json.loads(message.value().decode('utf-8'))
                symbol = data.get('symbol', 'UNKNOWN')
                bar_time_frame = data.get('barData', {}).get('barTimeFrame', 'UNKNOWN')
                
                # Feed message to global buffer manager with topic info
                if symbol != 'UNKNOWN' and bar_time_frame != 'UNKNOWN':
                    global_buffer_manager.add_message('bar_trends', symbol, bar_time_frame, data)
                
                self.logger.debug(f"📊 [BAR_TRENDS] {symbol}-{bar_time_frame}")
        except json.JSONDecodeError:
            self.logger.warning(f"📊 [BAR_TRENDS] Invalid JSON in message")
        except Exception as e:
            self.logger.error(f"📊 [BAR_TRENDS] Error processing message: {e}")
    
    def _handle_historic_stats(self, message: Any):
        """Handle historic_stats topic messages (BarHistoricTrends)"""
        try:
            # Parse JSON and feed to buffer manager
            if message.value():
                data = json.loads(message.value().decode('utf-8'))
                symbol = data.get('symbol', 'UNKNOWN')
                bar_time_frame = data.get('barData', {}).get('barTimeFrame', 'UNKNOWN')
                
                # Feed message to global buffer manager with topic info
                if symbol != 'UNKNOWN' and bar_time_frame != 'UNKNOWN':
                    global_buffer_manager.add_message('historic_stats', symbol, bar_time_frame, data)
                
                self.logger.debug(f"📈 [HISTORIC_STATS] {symbol}-{bar_time_frame}")
        except json.JSONDecodeError:
            self.logger.warning(f"📈 [HISTORIC_STATS] Invalid JSON in message")
        except Exception as e:
            self.logger.error(f"📈 [HISTORIC_STATS] Error processing message: {e}")
    
    def _handle_asset_trends(self, message: Any):
        """Handle asset_trends topic messages (AssetTrendGraph)"""
        try:
            # Parse JSON and feed to buffer manager
            if message.value():
                data = json.loads(message.value().decode('utf-8'))
                symbol = data.get('symbol', 'UNKNOWN')
                bar_time_frame = data.get('barData', {}).get('barTimeFrame', 'UNKNOWN')
                
                # Feed message to global buffer manager with topic info
                if symbol != 'UNKNOWN' and bar_time_frame != 'UNKNOWN':
                    global_buffer_manager.add_message('asset_trends', symbol, bar_time_frame, data)
                
                self.logger.debug(f"🔮 [ASSET_TRENDS] {symbol}-{bar_time_frame}")
        except json.JSONDecodeError:
            self.logger.warning(f"🔮 [ASSET_TRENDS] Invalid JSON in message")
        except Exception as e:
            self.logger.error(f"🔮 [ASSET_TRENDS] Error processing message: {e}")
    
    def start(self):
        """Start the Kafka consumer"""
        if self.running:
            self.logger.warning("Consumer is already running")
            return
            
        try:
            self.consumer = self._create_consumer()
            self.consumer.subscribe(self.topics)
            self.running = True
            
            self.logger.info(f"🚀 Starting Kafka consumer for topics: {self.topics}")
            print(f"🚀 Kafka Consumer started - listening to topics: {self.topics}")
            print(f"📡 Bootstrap servers: {self.config.bootstrap_servers}")
            print(f"🏃 Run mode: {self.config.run_mode}")
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
            self.logger.info("🛑 Kafka consumer stopped")
            print("🛑 Kafka consumer stopped")
    
    def set_message_handler(self, topic: str, handler: Callable):
        """Set a custom message handler for a specific topic"""
        if topic in self.topics:
            self.message_handlers[topic] = handler
            self.logger.info(f"Custom handler set for topic: {topic}")
        else:
            self.logger.warning(f"Topic {topic} not in subscribed topics: {self.topics}")

class KafkaConsumerThread(threading.Thread):
    """Thread wrapper for running Kafka consumer"""
    
    def __init__(self, config: KafkaConfig = None):
        super().__init__(daemon=True, name="KafkaConsumerThread")
        self.consumer = Take5KafkaConsumer(config)
        self.logger = logging.getLogger(__name__)
    
    def run(self):
        """Run the consumer in the thread"""
        try:
            self.logger.info("🧵 Starting Kafka consumer thread")
            self.consumer.start()
        except Exception as e:
            self.logger.error(f"Error in Kafka consumer thread: {e}")
    
    def stop(self):
        """Stop the consumer thread"""
        self.consumer.stop()
        self.logger.info("🧵 Kafka consumer thread stopped") 