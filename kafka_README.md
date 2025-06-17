# Take5Algo1 Kafka Consumer Module

This module provides a Python Kafka consumer that listens to the same topics as the Take5TwsPublisher WebSocket service.

## Overview

The Kafka consumer connects to the same Kafka topics that the Java WebSocket publisher consumes from, allowing Python analytics to process the same real-time data streams.

## Topics Consumed

The consumer listens to these Kafka topics (matching the WebSocket publisher configuration):

| Topic | Message Type | Description |
|-------|-------------|-------------|
| `bar_trends` | BarTrends | Processed bar trend data |
| `historic_stats` | BarHistoricTrends | Historical trend statistics |
| `asset_trends` | AssetTrendGraph | Asset trend predictions |

## Files

- `__init__.py` - Package initialization
- `consumer.py` - Main Kafka consumer implementation
- `config.py` - Configuration management
- `README.md` - This documentation

## Usage

### Basic Usage (via main.py)

The consumer is automatically started when running the main Take5Algo1 application:

```bash
cd ../Take5Algo1
python main.py
```

### Docker Logs

When running via Docker, the Kafka consumer output will be visible in the container logs:

```bash
# View logs from Take5Algo1 container
docker logs -f take5algo1
```

### Programmatic Usage

```python
from kafka.consumer import Take5KafkaConsumer, KafkaConsumerThread
from kafka.config import KafkaConfig

# Create consumer with default configuration
consumer = Take5KafkaConsumer()

# Or with custom configuration
config = KafkaConfig()
consumer = Take5KafkaConsumer(config=config)

# Start consuming (blocking)
consumer.start()

# Or use in a thread
thread = KafkaConsumerThread()
thread.start()
```

## Configuration

Configuration is managed through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_GROUP_ID` | `take5_algo1_group` | Consumer group ID |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset reset strategy |
| `KAFKA_AUTO_COMMIT_INTERVAL` | `1000` | Auto commit interval (ms) |
| `KAFKA_SESSION_TIMEOUT` | `45000` | Session timeout (ms) |
| `KAFKA_HEARTBEAT_INTERVAL` | `3000` | Heartbeat interval (ms) |
| `KAFKA_MAX_POLL_INTERVAL` | `300000` | Max poll interval (ms) |
| `LOG_LEVEL` | `INFO` | Logging level |
| `ENABLE_COLOR_LOGS` | `true` | Enable colored console logs |

## Message Handling

By default, each topic handler dumps received messages to the console with:
- Message key
- Message value (parsed as JSON if possible)
- Timestamp
- Partition and offset information

### Custom Message Handlers

You can set custom message handlers for specific topics:

```python
def custom_bar_trends_handler(message):
    # Process bar trends message
    data = json.loads(message.value().decode('utf-8'))
    print(f"Processing bar trends for: {data.get('symbol', 'unknown')}")

consumer = Take5KafkaConsumer()
consumer.set_message_handler('bar_trends', custom_bar_trends_handler)
consumer.start()
```

## Docker Integration

The consumer works with the existing Docker setup. Make sure Kafka is running:

```bash
# From Take5TwsPublisher directory
docker-compose -f KafkaBroker.yml up -d
```

## Dependencies

- `confluent-kafka==2.3.0` - Kafka Python client
- `python-json-logger==2.0.7` - JSON logging support
- `colorlog==6.7.0` - Colored console logging

## Error Handling

The consumer includes comprehensive error handling:
- Graceful shutdown on SIGINT/SIGTERM
- Automatic reconnection on connection failures
- Proper cleanup of resources
- Detailed error logging

## Logging

The module uses Python's standard logging with optional color support. Log messages include:
- Consumer lifecycle events
- Message processing status
- Error conditions
- Performance metrics

## Thread Safety

The `KafkaConsumerThread` class provides a thread-safe wrapper for running the consumer in the background while the main application continues other processing. 