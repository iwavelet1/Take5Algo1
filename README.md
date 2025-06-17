# Take5Algo1 - Python Analytics

Python analytics service that consumes real-time market data from the same Kafka topics as the Take5TwsPublisher WebSocket service.

## Overview

Take5Algo1 is a Python-based analytics service that connects to Kafka topics to process real-time market data. It consumes the same data streams that feed the WebSocket publisher, enabling Python-based analysis and algorithmic trading strategies.

## Architecture

```
Take5TwsPublisher (Java) → Kafka Topics → Take5Algo1 (Python)
                                    ↓
                              WebSocket Publisher
```

### Data Flow

The service consumes from these Kafka topics:

| Topic | Message Type | Description | Producer Service |
|-------|-------------|-------------|------------------|
| `bar_trends` | BarTrends | Processed bar trend data | BarInfoProcessorService |
| `historic_stats` | BarHistoricTrends | Historical trend statistics | HistoricStatsService |
| `asset_trends` | AssetTrendGraph | Asset trend predictions | AssetTrendsService |

## Features

- **Real-time Kafka Consumer**: Listens to the same topics as WebSocket publisher
- **Multi-threaded**: Kafka consumer runs in separate daemon thread
- **Configurable**: Environment variable based configuration
- **Robust Error Handling**: Graceful shutdown and reconnection logic
- **Rich Logging**: Color-coded console output with message details
- **Docker Ready**: Containerized deployment with docker-compose

## Project Structure

```
Take5Algo1/
├── main.py                 # Main application entry point
├── requirements.txt        # Python dependencies
├── Dockerfile              # Container definition
├── docker-compose.yml      # Container orchestration
├── kafka/                  # Kafka consumer module
│   ├── __init__.py         # Package initialization
│   ├── consumer.py         # Main Kafka consumer implementation
│   └── config.py           # Configuration management
├── kafka_README.md         # Detailed Kafka module documentation
└── README.md               # This file
```

## Quick Start

### Prerequisites

- Docker and Docker Compose (for Docker deployment)
- Python 3.11+ (for local development)
- Take5TwsPublisher running with Kafka broker

### Running Options

#### Option 1: Local Development (Recommended for Development)

**Prerequisites:** 
- Kafka running on `127.0.0.1:29092` (outside Docker cluster)

**Run locally:**
```bash
# Install dependencies
pip install -r requirements.txt

# Run using the local runner (sets all environment variables)
python run_local.py

# OR run directly with manual environment setup
export RUN_MODE=local
python main.py
```

#### Option 2: Docker Deployment

**Prerequisites:**
- Take5TwsPublisher Kafka broker running in Docker network

**Steps:**

1. **Start Take5TwsPublisher Services:**
   ```bash
   # From Take5TwsPublisher directory
   docker-compose -f KafkaBroker.yml up -d
   ```

2. **Build and Run Take5Algo1:**
   ```bash
   cd Take5Algo1
   docker-compose up -d --build
   ```

3. **View Real-time Logs:**
   ```bash
   docker-compose logs -f python_analytics
   ```

You should see output like:
```
🚀 Kafka Consumer started - listening to topics: ['bar_trends', 'historic_stats', 'asset_trends']
📡 Bootstrap servers: localhost:9092
📊 [BAR_TRENDS] Received message:
   Key: QQQ
   Parsed JSON: {
     "symbol": "QQQ",
     "timestamp": "2024-06-17T21:15:30Z",
     ...
   }
```

## Configuration

The service automatically configures itself based on the `RUN_MODE` environment variable:

| RUN_MODE | Kafka Bootstrap Servers | Description |
|----------|-------------------------|-------------|
| `local` (default) | `127.0.0.1:29092` | Local development, Kafka outside Docker |
| `docker` | `broker_1:9092` | Docker deployment, Kafka in Docker network |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RUN_MODE` | `local` | Execution mode: `local` or `docker` |
| `KAFKA_BOOTSTRAP_SERVERS` | *auto-configured* | Override Kafka broker addresses |
| `KAFKA_GROUP_ID` | `take5_algo1_group` | Consumer group ID |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset reset strategy |
| `LOG_LEVEL` | `INFO` | Logging level |
| `ENABLE_COLOR_LOGS` | `true` | Enable colored console logs |

### Docker Environment Variables

Update `docker-compose.yml` to customize configuration:

```yaml
environment:
  - KAFKA_BOOTSTRAP_SERVERS=broker_1:9092
  - KAFKA_GROUP_ID=my_custom_group
  - LOG_LEVEL=DEBUG
```

## Development

### Local Development

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run locally:**
   ```bash
   python main.py
   ```

### Adding Custom Analytics

The Kafka consumer supports custom message handlers:

```python
from kafka.consumer import Take5KafkaConsumer
import json

def my_bar_trends_handler(message):
    data = json.loads(message.value().decode('utf-8'))
    # Your custom analytics logic here
    print(f"Analyzing {data['symbol']} trends...")

consumer = Take5KafkaConsumer()
consumer.set_message_handler('bar_trends', my_bar_trends_handler)
consumer.start()
```

## Message Format Examples

### BarTrends (bar_trends topic)
```json
{
  "symbol": "QQQ",
  "timestamp": "2024-06-17T21:15:30Z",
  "price": 485.23,
  "volume": 1000,
  "trend_data": {...}
}
```

### BarHistoricTrends (historic_stats topic)
```json
{
  "symbol": "AAPL",
  "timestamp": "2024-06-17T21:15:30Z",
  "historic_analysis": {...},
  "statistics": {...}
}
```

### AssetTrendGraph (asset_trends topic)
```json
{
  "symbol": "TSLA",
  "timestamp": "2024-06-17T21:15:30Z",
  "predictions": {...},
  "trend_vectors": {...}
}
```

## Monitoring

### Container Health

```bash
# Check container status
docker-compose ps

# View resource usage
docker stats take5algo1

# Access container shell
docker-compose exec python_analytics bash
```

### Kafka Consumer Health

The consumer includes built-in health monitoring:
- Connection status logging
- Message processing metrics
- Error rate tracking
- Graceful shutdown handling

## Troubleshooting

### Common Issues

1. **Cannot connect to Kafka:**
   - Ensure Take5TwsPublisher Kafka broker is running
   - Check `KAFKA_BOOTSTRAP_SERVERS` configuration
   - Verify network connectivity between containers

2. **No messages received:**
   - Verify Take5TwsPublisher services are producing messages
   - Check consumer group offset position
   - Ensure topics exist and have data

3. **Container fails to start:**
   - Check Docker logs: `docker-compose logs python_analytics`
   - Verify all dependencies are installed
   - Check Python syntax errors

### Debug Mode

Enable debug logging:
```bash
docker-compose up -d --build -e LOG_LEVEL=DEBUG
```

## Dependencies

- **confluent-kafka==2.3.0** - High-performance Kafka Python client
- **python-json-logger==2.0.7** - Structured JSON logging
- **colorlog==6.7.0** - Colored console logging

## Integration with Take5TwsPublisher

This service is designed to work seamlessly with the Take5TwsPublisher ecosystem:

1. **Shared Kafka Infrastructure**: Uses the same Kafka broker and topics
2. **Compatible Message Formats**: Processes the same JSON message structures
3. **Coordinated Deployment**: Can be deployed alongside Java services
4. **Unified Monitoring**: Logs integrate with existing monitoring setup

For more detailed information about the Kafka consumer implementation, see [kafka_README.md](kafka_README.md).

---
*Last updated: 2024-06-17*

<!-- Updated: 2025-06-17 -->
