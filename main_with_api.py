#!/usr/bin/env python3
"""
Take5Algo1 Main Application with REST API

Runs both Kafka consumer and REST API server for complete analytics solution
"""

import logging
import signal
import sys
import time
import threading
from take5_algo1 import Take5Algo1
from kafka.config import KafkaConfig
from api_server import Take5ApiServer

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

class Take5MainWithApi:
    """Main application class that runs both Take5Algo1 and API server"""
    
    def __init__(self):
        self.take5_app = None
        self.api_server = None
        self.api_thread = None
        self.logger = logging.getLogger(__name__)
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"🔴 Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start both Take5Algo1 application and API server"""
        print("🚀 Starting Take5Algo1 with Analytics API...")
        print("="*80)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        try:
            # Initialize Kafka configuration
            config = KafkaConfig()
            
            print(f"🔧 Kafka Configuration: {config}")
            self.logger.info(f"🔧 Configuration: {config}")
            
            # Start Take5Algo1 application
            self.logger.info("🧵 Initializing Take5Algo1 application...")
            self.take5_app = Take5Algo1(config=config)
            self.take5_app.start()
            self.logger.info("✅ Take5Algo1 application started successfully")
            
            # Wait a moment for Kafka consumer to initialize
            time.sleep(2)
            
            # Start API server in separate thread
            self.logger.info("🌐 Initializing REST API server...")
            self.api_server = Take5ApiServer(host='0.0.0.0', port=8000)
            self.api_thread = self.api_server.run_in_thread(debug=False)
            self.logger.info("✅ REST API server started successfully")
            
            print("✅ All services started successfully!")
            print("")
            print("📊 Services Running:")
            print("   • Take5Algo1: Processing real-time data streams")
            print("   • REST API Server: http://localhost:8000")
            print("")
            print("🔗 Available Endpoints:")
            print("   • GET /asset_trends         - All sliding window data")
            print("   • GET /asset_trends/{asset} - Sliding windows for specific asset")
            print("   • GET /health               - Health check")
            print("   • GET /stats                - System statistics")
            print("   • GET /asset/{symbol}       - Specific asset details")
            print("")
            print("💡 Press Ctrl+C to stop all services")
            print("="*80)
            
            # Keep main thread alive
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("🔴 Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"🔴 Error in main: {e}")
        finally:
            self.stop()
    

    def stop(self):
        """Stop all services gracefully"""
        self.logger.info("🛑 Stopping all services...")
        
        # Stop API server first
        if self.api_server:
            self.logger.info("🧹 Stopping REST API server...")
            self.api_server.stop()
        
        # Stop Take5Algo1 application
        if self.take5_app:
            self.logger.info("🧹 Stopping Take5Algo1 application...")
            self.take5_app.stop()
        
        self.logger.info("👋 Take5Algo1 shutdown complete")
        print("👋 Take5Algo1 with API shutdown complete")


def main():
    """Main function"""
    # Setup logging
    setup_logging()
    
    # Create and start the main application
    app = Take5MainWithApi()
    app.start()


if __name__ == "__main__":
    main() 