# Take5Algo1 - Real-Time Financial Analytics

Advanced Python analytics system for real-time financial data processing with sliding window buffers, trend analysis, and Kafka integration.

## 🏗️ Architecture

### Core Components

- **🗄️ Sliding Window Manager** (`asset_data/`) - Time-based sliding window buffers for real-time data streams
- **📈 Asset Trends** (`asset_trends/`) - Trend analysis, pattern detection, and event-driven analytics
- **📡 Kafka Integration** (`kafka/`) - High-performance message consumption and processing
- **🚀 Multiple Entry Points** - Different modes for production, demo, and local development

### Key Features

- **Real-time data buffering** with configurable sliding windows (default: 120 minutes)
- **Multi-asset, multi-timeframe** trend analysis
- **Event-driven architecture** with asset trend factories
- **Thread-safe message processing** with batch optimization
- **Comprehensive logging** and status monitoring
- **Docker containerization** for easy deployment

## 📁 Project Structure

```
Take5Algo1/
├── asset_data/                 # Data buffer management
│   ├── sliding_window_manager.py    # Core sliding window implementation
│   ├── global_buffer_manager.py     # Global buffer instance
│   └── kafka_buffer_consumer.py     # Kafka integration with buffers
├── asset_trends/               # Trend analysis engine
│   ├── asset_trends.py             # Trend detection algorithms
│   ├── asset_trend_factory.py      # Asset trend creation & management
│   └── events.py                   # Event definitions
├── kafka/                      # Kafka configuration
│   ├── consumer.py                 # Kafka consumer implementation
│   └── config.py                  # Kafka connection settings
├── main.py                     # Production entry point
├── demo_main.py               # Demo with full analytics pipeline
├── run_local.py              # Local development runner
└── docker-compose.yml        # Container orchestration
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Docker & Docker Compose
- Access to Take5 Kafka streams

### Installation

1. **Clone and setup:**
   ```bash
   git clone <repo-url>
   cd Take5Algo1
   pip install -r requirements.txt
   ```

2. **Configure Kafka connection:**
   ```bash
   # Edit kafka/config.py with your Kafka broker details
   ```

### Running the System

#### 🔧 Production Mode
```bash
python main.py
```
Basic Kafka consumer with logging and graceful shutdown.

#### 📊 Demo Mode (Full Analytics)
```bash
python demo_main.py
```
Complete system with:
- Sliding window buffers
- Asset trend analysis
- Real-time trend detection
- Status monitoring every 5 minutes

#### 🐳 Docker Deployment
```bash
docker-compose up -d --build
```

#### 📝 View Logs
```bash
docker-compose logs -f python_analytics
```

## 🔧 Configuration

### Sliding Window Settings
- **Window Size:** 120 minutes (configurable in `AssetBufferManager`)
- **Buffer Structure:** Topic → Asset → BarTimeFrame → TimeBasedSlidingWindow
- **Message Processing:** Batch processing with thread-safe queues

### Data Format in Signal Algorithms
When algorithms receive data via `get_asset_from_topic()`, the format is:
```python
data: Dict[int, TimeBasedSlidingWindow] = {
    60: TimeBasedSlidingWindow,      # 1-minute timeframe window
    300: TimeBasedSlidingWindow,     # 5-minute timeframe window  
    720: TimeBasedSlidingWindow,     # 12-minute timeframe window
    1560: TimeBasedSlidingWindow,    # 26-minute timeframe window
    # ... any other timeframes from incoming messages
}
```
- **Keys:** `barTimeFrame` integers (dynamic, from message data)
- **Values:** `TimeBasedSlidingWindow` objects with methods like `get_current_message()`, `get_messages()`
- **Access Pattern:** `data[60].get_current_message()` gets latest message for 60-second timeframe

### Asset Trends
- **Analysis Frequency:** Every 60 seconds
- **Trend Detection:** Direction, strength, and confidence scoring
- **Event System:** NewAssetEvent notifications for dynamic asset tracking

## 📊 Data Flow

1. **Kafka Messages** → `BufferManagedKafkaConsumer`
2. **Message Processing** → `AssetBufferManager` (sliding windows)
3. **Trend Analysis** → `AssetTrendFactory` (trend detection)
4. **Event Notifications** → Asset trend updates and alerts

## 🔍 Monitoring

The demo mode provides comprehensive status reporting:
- Buffer manager statistics (topics, combinations)
- Trend factory metrics (assets tracked, events processed)
- Kafka consumer performance (messages, errors)

## 🛠️ Development

### Adding New Trend Algorithms
1. Extend classes in `asset_trends/asset_trends.py`
2. Register new trend types in `AssetTrendFactory`
3. Update event handling as needed

### Customizing Buffer Windows
```python
# Modify window size (in minutes)
buffer_manager = AssetBufferManager(window_size_minutes=180)
```

### Local Testing
```bash
python run_local.py  # Local development with mock data
```

## 📚 API Reference

### Core Classes
- `AssetBufferManager` - Main buffer management
- `TimeBasedSlidingWindow` - Individual asset/timeframe buffers  
- `AssetTrendFactory` - Trend analysis coordination
- `BufferManagedKafkaConsumer` - Kafka integration

## 🔄 Git Workflow

Use the automated save command:
```bash
# In chat/terminal, type:
save!
```
This runs the complete git workflow: stage, commit, and push to main branch.

---

*Last Updated: 2025-06-19*

<!-- Updated: 2025-06-19 -->

<!-- Updated: 2025-06-21 -->

<!-- Updated: 2025-06-22 -->

<!-- Updated: 2025-06-23 -->

<!-- Updated: 2025-06-24 -->

<!-- Updated: 2025-06-24 -->
