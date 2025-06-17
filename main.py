import logging
import signal
import sys
import time
from kafka.consumer import KafkaConsumerThread
from kafka.config import KafkaConfig

def setup_logging():
    """Setup colorful logging"""
    try:
        import colorlog
        
        # Create logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        
        # Create console handler with color
        handler = colorlog.StreamHandler()
        handler.setLevel(logging.INFO)
        
        # Create colorful formatter
        formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            }
        )
        
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    except ImportError:
        # Fallback to basic logging if colorlog is not available
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger = logging.getLogger(__name__)
    logger.info(f"🔴 Received signal {signum}, shutting down...")
    if 'kafka_thread' in globals() and kafka_thread:
        kafka_thread.stop()
    sys.exit(0)

def main():
    """Main function that starts Take5Algo1 with Kafka consumer"""
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize configuration
    config = KafkaConfig()
    
    print("🚀 Starting Take5Algo1 Python Analytics...")
    print(f"🔧 Configuration: {config}")
    logger.info("🚀 Starting Take5Algo1 Python Analytics...")
    logger.info(f"🔧 Configuration: {config}")
    
    # Start Kafka consumer in separate thread
    try:
        logger.info("🧵 Initializing Kafka consumer thread...")
        kafka_thread = KafkaConsumerThread(config=config)
        
        # Start the consumer thread
        kafka_thread.start()
        logger.info("🧵 Kafka consumer thread started successfully")
        
        # Keep main thread alive
        logger.info("💡 Main thread running... Press Ctrl+C to stop")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("🔴 Received keyboard interrupt")
    except Exception as e:
        logger.error(f"🔴 Error in main: {e}")
    finally:
        # Cleanup
        if 'kafka_thread' in locals() and kafka_thread:
            logger.info("🧹 Stopping Kafka consumer thread...")
            kafka_thread.stop()
            kafka_thread.join(timeout=5)
        
        logger.info("👋 Take5Algo1 shutdown complete")
        print("👋 Take5Algo1 shutdown complete")

if __name__ == "__main__":
    main()
