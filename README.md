# KafkaBoost 🚀

**KafkaBoost** is an enhanced Apache Kafka library that extends standard Kafka functionality with priority-based message processing, automatic topic management, and intelligent consumer orchestration.

## 🌟 Key Features

### 🎯 Priority-Based Message Processing
- **Priority Boost Mode**: Routes messages to priority-specific topics and serves highest priority first
- **Standard Mode**: Sorts messages by priority field within batches
- **Dynamic Consumer Management**: Automatically pauses/resumes consumers based on priority

### 🔧 Automatic Topic Management
- **Smart Topic Creation**: Automatically creates priority-specific topics with configurable partitions
- **S3 Configuration Integration**: Manages topic configurations through S3
- **Dynamic Configuration Updates**: Supports runtime configuration changes

### ⚡ Enhanced Consumer Experience
- **Async Support**: Non-blocking polling for async applications
- **Intelligent Partitioning**: Configurable partition counts per priority level
- **Consumer Group Management**: Unique group IDs for each priority level

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Producer      │    │   KafkaBoost     │    │   Consumer      │
│                 │    │                  │    │                 │
│ • Priority      │───▶│ • Topic Routing  │───▶│ • Priority      │
│   Routing       │    │ • Auto Creation  │    │   Queues        │
│ • S3 Config     │    │ • S3 Integration │    │ • Smart Polling │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Kafka Topics   │
                       │                  │
                       │ • base_topic     │
                       │ • base_topic_5   │
                       │ • base_topic_7   │
                       │ • base_topic_10  │
                       └──────────────────┘
```

## 🚀 Quick Start

### Installation

```bash
pip install kafkaboost
```

### Basic Usage

```python
from kafkaboost.consumer import KafkaboostConsumer
from kafkaboost.producer import KafkaboostProducer

# Producer with priority routing
producer = KafkaboostProducer(
    bootstrap_servers=['localhost:9092'],
    user_id='user123'  # Enables S3 config lookup
)

# Send messages with different priorities
producer.send('orders', {'order_id': 1, 'priority': 5})
producer.send('orders', {'order_id': 2, 'priority': 10})  # Higher priority

# Consumer with priority boost
consumer = KafkaboostConsumer(
    bootstrap_servers=['localhost:9092'],
    topics=['orders'],
    group_id='priority_group',
    user_id='user123'  # Enables priority boost mode
)

# Messages are automatically served by priority (10 first, then 5)
messages = consumer.poll(timeout_ms=1000)
```

## 📋 Configuration

### S3 Configuration Structure

```json
{
  "user_id": "user123",
  "max_priority": 10,
  "default_priority": 0,
  "Priority_boost": [
    {
      "topic_name": "orders",
      "priority_boost_min_value": 5,
      "number_of_partitions": 9
    }
  ],
  "Topics_priority": [
    {
      "topic": "notifications",
      "priority": 8
    }
  ],
  "Rule_Base_priority": [
    {
      "role_name": "admin",
      "value": "high",
      "priority": 9
    }
  ]
}
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `topic_name` | Base topic name for priority routing | Required |
| `priority_boost_min_value` | Minimum priority level for boost mode | 0 |
| `number_of_partitions` | Number of partitions for priority topics | 1 |
| `max_priority` | Maximum priority level supported | 10 |

## 🔄 Priority Boost Mode

### How It Works

1. **Topic Discovery**: Automatically finds priority-specific topics (e.g., `orders_5`, `orders_7`, `orders_10`)
2. **Consumer Creation**: Creates separate consumers for each priority level
3. **Smart Polling**: Serves messages from highest priority first
4. **Dynamic Management**: Pauses lower priority consumers when higher priority has messages

### Topic Naming Convention

Priority topics follow the pattern: `{base_topic}_{priority_level}`

Examples:
- `orders_0` - Lowest priority orders
- `orders_5` - Medium priority orders  
- `orders_10` - Highest priority orders

### Consumer Group Management

Each priority level gets its own consumer group:
- `group_id_base` - For base topic
- `group_id_priority_5` - For priority 5 topics
- `group_id_priority_10` - For priority 10 topics

## 🛠️ Advanced Usage

### Producer with Priority Routing

```python
from kafkaboost.producer import KafkaboostProducer

producer = KafkaboostProducer(
    bootstrap_servers=['localhost:9092'],
    user_id='user123'
)

# Messages are automatically routed to priority topics
producer.send('orders', {
    'order_id': 123,
    'customer_id': 'cust_456',
    'amount': 99.99
}, priority=10)  # Goes to orders_10 topic

producer.send('orders', {
    'order_id': 124,
    'customer_id': 'cust_789',
    'amount': 49.99
}, priority=5)   # Goes to orders_5 topic
```

### Consumer with Async Support

```python
import asyncio
from kafkaboost.consumer import KafkaboostConsumer

async def consume_orders():
    consumer = KafkaboostConsumer(
        bootstrap_servers=['localhost:9092'],
        topics=['orders'],
        user_id='user123'
    )
    
    try:
        while True:
            # Non-blocking async polling
            messages = await consumer.poll_async(timeout_ms=1000)
            
            for msg in messages:
                priority = msg.value.get('priority', 0)
                print(f"Processing order with priority {priority}")
                
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

# Run async consumer
asyncio.run(consume_orders())
```

### Configuration Management

```python
from kafkaboost.kafka_utils import KafkaConfigManager

# Initialize config manager
config_manager = KafkaConfigManager(
    bootstrap_servers='localhost:9092',
    user_id='user123'
)

# Ensure priority topics exist
config_manager.check_and_create_priority_topics()

# Get configuration summary
summary = config_manager.get_config_summary()
print(f"Max priority: {summary['max_priority']}")
print(f"Topics count: {summary['topics_count']}")
```

## 🔧 Automatic Topic Creation

### Features

- **Configurable Partitions**: Each priority topic can have different partition counts
- **Idempotent Creation**: Won't create topics that already exist
- **Error Handling**: Graceful handling of creation failures
- **S3 Integration**: Uses S3 configuration for topic specifications

### Example

```python
# Topics are automatically created based on configuration
# For config: {"topic_name": "orders", "priority_boost_min_value": 5, "number_of_partitions": 9}

# Creates:
# - orders_5 (9 partitions)
# - orders_6 (9 partitions)  
# - orders_7 (9 partitions)
# - orders_8 (9 partitions)
# - orders_9 (9 partitions)
# - orders_10 (9 partitions)
```

## 📊 Monitoring and Debugging

### Configuration Summary

```python
# Get detailed configuration information
summary = consumer.get_config_summary()
print(f"Priority boost enabled: {summary['priority_boost_enabled']}")
print(f"Current subscription: {summary['current_subscription']}")
print(f"Max priority: {summary['max_priority']}")
```

### Consumer State

```python
# Check consumer status
print(f"Priority boost enabled: {consumer.priority_boost_enabled}")
print(f"Active consumers: {len(consumer.priority_consumer_manager.consumers)}")
print(f"Current subscription: {consumer.current_subscription}")
```

## 🧪 Testing

### Run Priority Boost Tests

```bash
# Run comprehensive priority boost tests
python kafkaboost/tests/test_priority_boost_kafka.py

# Run specific test scenarios
python simple_priority_test.py
python fresh_consumer_test.py
```

### Test Results

The test suite validates:
- ✅ Priority-based message routing
- ✅ Consumer group management
- ✅ Topic creation and management
- ✅ Dynamic consumer pausing/resuming
- ✅ Configuration updates

## 🚨 Troubleshooting

### Common Issues

1. **Priority Boost Not Enabled**
   ```python
   # Check S3 configuration
   summary = consumer.get_config_summary()
   print(f"Priority boost: {summary['priority_boost_enabled']}")
   ```

2. **Topics Not Created**
   ```python
   # Manually create topics
   config_manager = KafkaConfigManager('localhost:9092', user_id='user123')
   config_manager.check_and_create_priority_topics()
   ```

3. **Consumer Group Issues**
   ```python
   # Use unique group IDs
   consumer = KafkaboostConsumer(
       bootstrap_servers=['localhost:9092'],
       topics=['orders'],
       group_id='unique_group_id',  # Ensure uniqueness
       user_id='user123'
   )
   ```

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable debug logging for detailed information
consumer = KafkaboostConsumer(
    bootstrap_servers=['localhost:9092'],
    topics=['orders'],
    user_id='user123'
)
```

## 📚 API Reference

### KafkaboostConsumer

#### Constructor Parameters
- `bootstrap_servers`: Kafka server address(es)
- `topics`: Topic(s) to consume from
- `group_id`: Consumer group ID
- `user_id`: User ID for S3 config lookup (enables priority boost)
- `auto_offset_reset`: Offset reset strategy ('earliest', 'latest', 'none')
- `**kwargs`: Additional KafkaConsumer parameters

#### Key Methods
- `poll(timeout_ms=1000, max_records=None)`: Poll for messages
- `poll_async(timeout_ms=1000, max_records=None)`: Async polling
- `refresh_config()`: Refresh configuration from S3
- `get_config_summary()`: Get configuration summary
- `close()`: Close consumer and cleanup

### KafkaboostProducer

#### Constructor Parameters
- `bootstrap_servers`: Kafka server address(es)
- `user_id`: User ID for S3 config lookup
- `**kwargs`: Additional KafkaProducer parameters

#### Key Methods
- `send(topic, value, priority=None)`: Send message with optional priority
- `close()`: Close producer

### KafkaConfigManager

#### Constructor Parameters
- `bootstrap_servers`: Kafka server address(es)
- `user_id`: User ID for S3 config lookup

#### Key Methods
- `check_and_create_priority_topics()`: Create priority topics
- `get_config_summary()`: Get configuration summary
- `find_matching_topics(base_topics)`: Find priority topic variants

## 🔄 Migration Guide

### From Standard Kafka Consumer

```python
# Before
from kafka import KafkaConsumer
consumer = KafkaConsumer('orders', bootstrap_servers=['localhost:9092'])

# After
from kafkaboost.consumer import KafkaboostConsumer
consumer = KafkaboostConsumer(
    bootstrap_servers=['localhost:9092'],
    topics=['orders'],
    user_id='user123'  # Enables priority boost
)
```

### From Standard Kafka Producer

```python
# Before
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# After
from kafkaboost.producer import KafkaboostProducer
producer = KafkaboostProducer(
    bootstrap_servers=['localhost:9092'],
    user_id='user123'  # Enables priority routing
)
```

## 🏆 Best Practices

### Topic Design
- Use descriptive base topic names
- Keep priority levels manageable (0-10 recommended)
- Ensure consistent naming across environments

### Consumer Groups
- Use different group IDs for different priority requirements
- Consider separate consumers for different priority ranges
- Monitor consumer group rebalancing

### Performance
- Priority boost mode is most effective with high message volumes
- Consider batch sizes for optimal throughput
- Monitor partition assignment and rebalancing

### Error Handling
- Always close consumers in finally blocks
- Handle configuration refresh errors gracefully
- Monitor partition pausing/resuming for performance

## 📦 Dependencies

- `kafka-python` - Core Kafka functionality
- `boto3` - S3 configuration management
- `asyncio` - Async support (Python 3.7+)

## 📄 License

This project extends the existing kafkaboost library with priority-aware features while maintaining backward compatibility.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📞 Support

For issues and questions:
- Check the troubleshooting section
- Review the test examples
- Open an issue on GitHub

---

**KafkaBoost** - Making Kafka priority-aware and production-ready! 🚀
