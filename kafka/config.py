import os
from typing import Dict, Any

class KafkaConfig:
    """Configuration class for Kafka consumer settings"""
    
    def __init__(self):
        # Determine run mode: 'local' or 'docker'
        self.run_mode = os.getenv('RUN_MODE', 'local').lower()
        
        # Kafka broker settings based on run mode
        if self.run_mode == 'docker':
            # Inside Docker network
            default_bootstrap_servers = 'broker_1:9092'
        else:
            # Local development (outside Docker cluster)
            default_bootstrap_servers = '127.0.0.1:29092'
            
        # Allow override via environment variable
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', default_bootstrap_servers)
        
        # Consumer group settings
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'take5_algo1_group')
        
        # Topics to subscribe to (matching WebSocket publisher)
        self.topics = [
            'bar_trends',      # BarTrends messages
            'historic_stats',  # BarHistoricTrends messages  
            'asset_trends'     # AssetTrendGraph messages
        ]
        
        # Consumer configuration
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': int(os.getenv('KAFKA_AUTO_COMMIT_INTERVAL', '1000')),
            'session.timeout.ms': int(os.getenv('KAFKA_SESSION_TIMEOUT', '45000')),
            'heartbeat.interval.ms': int(os.getenv('KAFKA_HEARTBEAT_INTERVAL', '3000')),
            'max.poll.interval.ms': int(os.getenv('KAFKA_MAX_POLL_INTERVAL', '300000')),
        }
        
        # Logging settings
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.enable_color_logs = os.getenv('ENABLE_COLOR_LOGS', 'true').lower() == 'true'
    
    def get_consumer_config(self) -> Dict[str, Any]:
        """Get the consumer configuration dictionary"""
        return self.consumer_config.copy()
    
    def __str__(self):
        return f"KafkaConfig(run_mode={self.run_mode}, bootstrap_servers={self.bootstrap_servers}, group_id={self.group_id}, topics={self.topics})" 