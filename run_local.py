#!/usr/bin/env python3
"""
Local runner for Take5Algo1 Python Analytics
Sets environment variables for local development and runs the main application.
"""

import os
import sys

def main():
    """Set local environment variables and run the main application"""
    
    # Set environment variables for local development
    os.environ['RUN_MODE'] = 'local'
    os.environ['KAFKA_BOOTSTRAP_SERVERS'] = '127.0.0.1:29092'
    os.environ['KAFKA_GROUP_ID'] = 'take5_algo1_local_group'
    os.environ['LOG_LEVEL'] = 'INFO'
    os.environ['ENABLE_COLOR_LOGS'] = 'true'
    
    print("🏠 Running Take5Algo1 in LOCAL mode")
    print(f"🔧 Kafka Bootstrap Servers: {os.environ['KAFKA_BOOTSTRAP_SERVERS']}")
    print(f"👥 Consumer Group: {os.environ['KAFKA_GROUP_ID']}")
    print("-" * 60)
    
    # Import and run the main application
    try:
        from main import main as app_main
        app_main()
    except KeyboardInterrupt:
        print("\n👋 Local run stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error running application: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 