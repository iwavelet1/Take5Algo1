#!/usr/bin/env python3
"""
Demo Main Script for Asset Trends System

This script demonstrates how to:
1. Initialize the AssetBufferManager
2. Create an AssetTrendFactory
3. Register the factory as a listener for NewAssetEvent
4. Start the Kafka consumer that feeds data into the buffer manager
"""

import logging
import signal
import sys
import time
import threading
from typing import Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import our components
from asset_data.global_buffer_manager import global_buffer_manager
from asset_trends import create_asset_trend_factory
from asset_data.kafka_buffer_consumer import BufferManagedKafkaConsumer

# Import existing Kafka config
from kafka.config import KafkaConfig


class AssetTrendsMain:
    """Main application class for asset trends system"""
    
    def __init__(self):
        self.buffer_manager = global_buffer_manager
        self.trend_factory = None
        self.kafka_consumer = None
        self.running = False
        self.analysis_thread: Optional[threading.Thread] = None
        
        self.logger = logging.getLogger(__name__)
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
            self.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def initialize_trend_factory(self):
        """Initialize and register the AssetTrendFactory"""
        self.logger.info("🏭 Initializing AssetTrendFactory...")
        
        # Create the factory
        self.trend_factory = create_asset_trend_factory(self.buffer_manager)
        
        # Register the factory as a listener for NewAssetEvent
        # Note: This will work once the buffer manager has NewAssetEvent support
        try:
            unsubscribe = self.buffer_manager.register_new_asset_listener(
                self.trend_factory.handle_new_asset_event
            )
            self.logger.info("✅ AssetTrendFactory registered for NewAssetEvent notifications")
        except AttributeError:
            self.logger.warning("⚠️  BufferManager doesn't have NewAssetEvent support yet")
            self.logger.info("💡 You need to add the register_new_asset_listener method to AssetBufferManager")
            
        return self.trend_factory
    
    def start_kafka_consumer(self):
        """Start the Kafka consumer in a separate thread"""
        self.logger.info("🚀 Starting Kafka consumer...")
        
        config = KafkaConfig()
        self.kafka_consumer = BufferManagedKafkaConsumer(config, self.buffer_manager)
        
        # Start consumer in background thread
        consumer_thread = threading.Thread(
            target=self.kafka_consumer.start,
            daemon=True,
            name="KafkaConsumerThread"
        )
        consumer_thread.start()
        
        return consumer_thread
    
    def start_trend_analysis_loop(self):
        """Start the trend analysis loop in a separate thread"""
        def analysis_loop():
            while self.running:
                try:
                    if self.trend_factory:
                        # Run trend analysis for all tracked assets
                        analyses = self.trend_factory.analyze_all_trends()
                        
                        if analyses:
                            self.logger.info(f"📊 Analyzed trends for {len(analyses)} assets")
                            
                            # Print some sample results
                            for asset, trend_list in list(analyses.items())[:3]:  # Show first 3
                                for trend in trend_list[:1]:  # Show first trend per asset
                                    self.logger.info(
                                        f"📈 {asset}/{trend.topic}/{trend.bar_time_frame}s: "
                                        f"{trend.trend_direction} (strength: {trend.trend_strength:.2f}, "
                                        f"confidence: {trend.confidence:.2f})"
                                    )
                    
                    # Sleep for 60 seconds between analyses
                    time.sleep(60)
                    
                except Exception as e:
                    self.logger.error(f"Error in trend analysis loop: {e}")
                    time.sleep(30)  # Wait longer on error
        
        self.analysis_thread = threading.Thread(
            target=analysis_loop,
            daemon=True,
            name="TrendAnalysisThread"
        )
        self.analysis_thread.start()
        
        return self.analysis_thread
    
    def print_status_periodically(self):
        """Print system status every 5 minutes"""
        def status_loop():
            while self.running:
                time.sleep(300)  # 5 minutes
                
                if self.running:
                    print("\n" + "="*80)
                    print("📊 ASSET TRENDS SYSTEM STATUS")
                    print("="*80)
                    
                    # Buffer manager stats
                    buffer_stats = self.buffer_manager.get_all_buffer_stats()
                    topics = self.buffer_manager.get_all_topics()
                    
                    print(f"🗄️  Buffer Manager: {len(topics)} topics")
                    for topic in topics:
                        combinations = self.buffer_manager.get_all_combinations(topic)
                        print(f"   • {topic}: {len(combinations)} asset/timeframe combinations")
                    
                    # Trend factory stats
                    if self.trend_factory:
                        factory_status = self.trend_factory.get_factory_status()
                        print(f"🏭 Trend Factory: {factory_status['active_assets']} assets tracked")
                        print(f"   • Events processed: {factory_status['events_processed']}")
                        print(f"   • Assets created: {factory_status['assets_created']}")
                        print(f"   • Combinations tracked: {factory_status['tracked_combinations']}")
                    
                    # Kafka consumer stats
                    if self.kafka_consumer:
                        kafka_stats = self.kafka_consumer.get_buffer_stats()
                        print(f"📡 Kafka Consumer:")
                        print(f"   • Messages received: {kafka_stats['kafka_messages_received']}")
                        print(f"   • Messages processed: {kafka_stats['kafka_messages_processed']}")
                        print(f"   • Errors: {kafka_stats['kafka_errors']}")
                    
                    print("="*80)
        
        status_thread = threading.Thread(
            target=status_loop,
            daemon=True,
            name="StatusThread"
        )
        status_thread.start()
        
        return status_thread
    
    def run(self):
        """Run the main application"""
        print("🚀 Starting Asset Trends System")
        print("="*80)
        
        # Setup signal handlers
        self.setup_signal_handlers()
        
        # Initialize components
        self.trend_factory = self.initialize_trend_factory()
        
        # Start Kafka consumer
        kafka_thread = self.start_kafka_consumer()
        
        # Start trend analysis loop
        analysis_thread = self.start_trend_analysis_loop()
        
        # Start status reporting
        status_thread = self.print_status_periodically()
        
        self.running = True
        
        print("✅ All systems started successfully")
        print("📊 Waiting for Kafka messages to arrive...")
        print("💡 Press Ctrl+C to stop gracefully")
        print("="*80)
        
        try:
            # Main loop - just wait for shutdown
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n🛑 Interrupted by user")
        finally:
            self.stop()
    
    def stop(self):
        """Stop all components gracefully"""
        self.running = False
        
        if self.kafka_consumer:
            self.kafka_consumer.stop()
        
        print("🛑 Asset Trends System stopped")


def main():
    """Main entry point"""
    app = AssetTrendsMain()
    app.run()


if __name__ == "__main__":
    main() 