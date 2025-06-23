import logging
from typing import Optional
from kafka.config import KafkaConfig
from kafka.consumer import KafkaConsumerThread
from asset_data.global_buffer_manager import global_buffer_manager
from asset_trends import AssetTrendFactory


class Take5Algo1:
    """Root application class that manages all components"""
    
    def __init__(self, config: Optional[KafkaConfig] = None):
        self.logger = logging.getLogger(__name__)
        self.config = config or KafkaConfig()
        
        # Initialize components
        self.global_buffer_manager = global_buffer_manager
        self.asset_trend_factory = AssetTrendFactory(app=self)
        self.kafka_thread = KafkaConsumerThread(config=self.config, app=self)
        
        self.logger.info("🚀 Take5Algo1 application initialized")
    
    def start(self):
        """Start all application components"""
        self.logger.info("🏁 Starting Take5Algo1 application...")
        
        # Start Kafka consumer thread
        self.kafka_thread.start()
        
        self.logger.info("✅ Take5Algo1 application started successfully")
    
    def stop(self):
        """Stop all application components"""
        self.logger.info("🛑 Stopping Take5Algo1 application...")
        
        # Stop AssetTrendFactory processors
        if hasattr(self.asset_trend_factory, 'stop_all_processors'):
            self.asset_trend_factory.stop_all_processors()
        
        # Stop Kafka consumer thread
        if self.kafka_thread.is_alive():
            self.kafka_thread.stop()
            self.kafka_thread.join(timeout=5.0)
        
        self.logger.info("✅ Take5Algo1 application stopped successfully")
    
    def get_global_buffer_manager(self):
        """Get the global buffer manager"""
        return self.global_buffer_manager
    
    def get_asset_trend_factory(self):
        """Get the asset trend factory"""
        return self.asset_trend_factory 