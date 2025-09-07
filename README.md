# KafkaBoost 🚀

**KafkaBoost** is an enhanced Apache Kafka library that extends standard Kafka functionality with priority-based message processing, automatic topic management, and intelligent consumer orchestration.

## 🌟 Key Features

### 🎯 Priority-Based Message Processing
- **Automatic Priority Detection**: Detects priority by topic name, message content rules, or manual specification
- **Priority Boost Mode**: Routes messages to priority-specific topics and serves highest priority first
- **Standard Mode**: Sorts messages by priority field within batches
- **Dynamic Consumer Management**: Automatically pauses/resumes consumers based on priority

### 🔧 Automatic Topic Management
- **Smart Topic Creation**: Automatically creates priority-specific topics with configurable partitions
- **S3 Configuration Integration**: Manages topic configurations through S3
- **Dynamic Configuration Updates**: Supports runtime configuration changes

### ⚡ Enhanced Consumer Experience
- **Intelligent Partitioning**: Configurable partition counts per priority level
- **Consumer Group Management**: Unique group IDs for each priority level
- **Priority-First Consumption**: Always serves highest priority messages first

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    KafkaBoost Wrapper                           │
│                                                                 │
│  ┌─────────────────┐    ┌──────────────────┐    ┌─────────────┐ │
│  │ Kafkaboost      │    │ Kafkaboost       │    │ Kafkaboost  │ │
│  │ Producer        │    │ Consumer         │    │ Config      │ │
│  │                 │    │                  │    │ Manager     │ │
│  │ • Priority      │    │ • Priority       │    │             │ │
│  │   Routing       │    │   Queues         │    │ • Auto      │ │
│  │ • S3 Config     │    │ • Smart Polling  │    │   Topic     │ │
│  │ • Enhanced      │    │ • Consumer       │    │   Creation  │ │
│  │   Kafka Client  │    │   Management     │    │ • S3        │ │
│  └─────────────────┘    └──────────────────┘    │   Config    │ │
│           │                       │             │   Manager   │ │
│           ▼                       ▼             └─────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                Apache Kafka (Black Box)                     ││
│  │                                                             ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          ││
│  │  │ base_topic  │  │base_topic_5 │  │base_topic_7 │  ...     ││
│  │  └─────────────┘  └─────────────┘  └─────────────┘          ││
│  │                                                             ││
│  │  • Message Storage    • Partitioning    • Replication       ││
│  │  • Consumer Groups   • Offset Management • Ordering         ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

**KafkaBoost enhances Kafka by:**
- 🎯 **Automatic priority detection** by topic, rules, or manual specification
- 🔧 **Priority-based routing** to topic variants
- 📊 **Automatic topic creation** with configurable partitions  
- ⚙️ **Smart consumer management** with priority queues
- 🚀 **S3 configuration integration** for dynamic settings
- 📈 **Priority-first consumption** for optimal message processing

## 🚀 Quick Start

### Installation

```bash
pip install kafkaboost
```

### Basic Usage

#### Step 1: Configure Your Settings
1. **Visit the KafkaBoost Configuration Website**:https://master.d1disovd4gm7yp.amplifyapp.com
2. **Login** to your account
3. **Select your required configuration** (topics, priorities, partitions, etc.)
4. **Copy your User ID** from the dashboard

#### Step 2: Use KafkaBoost (Just like Kafka + User ID)

```python
from kafkaboost.consumer import KafkaboostConsumer
from kafkaboost.producer import KafkaboostProducer

# Producer with priority routing (just add user_id to your existing Kafka code)
producer = KafkaboostProducer(
    bootstrap_servers=['localhost:9092'],
    user_id='your-user-id-from-website'  # Copy from configuration website
)

# Send messages with different priorities
producer.send('orders', {'order_id': 1, 'priority': 5})
producer.send('orders', {'order_id': 2, 'priority': 10})  # Higher priority

# Consumer with priority boost (just add user_id to your existing Kafka code)
consumer = KafkaboostConsumer(
    bootstrap_servers=['localhost:9092'],
    topics=['orders'],
    group_id='priority_group',
    user_id='your-user-id-from-website'  # Copy from configuration website
)

# Messages are automatically served by priority (10 first, then 5)
messages = consumer.poll(timeout_ms=1000)
```

#### Step 3: That's It!
- ✅ **No configuration files needed** - everything is managed on the website
- ✅ **Automatic topic creation** - topics are created based on your configuration
- ✅ **Priority routing** - messages are automatically routed to priority topics
- ✅ **Smart consumption** - highest priority messages are served first

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

## 🎯 Automatic Priority Detection

KafkaBoost automatically detects message priority using three methods:

### 1. **Topic-Based Priority** (`Topics_priority`)
Messages sent to specific topics automatically get assigned priority:

```json
"Topics_priority": [
  {
    "topic": "urgent_orders",
    "priority": 9
  },
  {
    "topic": "notifications", 
    "priority": 7
  },
  {
    "topic": "reports",
    "priority": 3
  }
]
```

**Usage:**
```python
# Messages to 'urgent_orders' automatically get priority 9
producer.send('urgent_orders', {'order_id': 123, 'customer': 'VIP'})

# Messages to 'notifications' automatically get priority 7  
producer.send('notifications', {'message': 'Order shipped'})

# Messages to 'reports' automatically get priority 3
producer.send('reports', {'report_type': 'daily_summary'})
```

### 2. **Rule-Based Priority** (`Rule_Base_priority`)
Messages are prioritized based on content rules:

```json
"Rule_Base_priority": [
  {
    "role_name": "user_role",
    "value": "admin", 
    "priority": 9
  },
  {
    "role_name": "user_role",
    "value": "premium",
    "priority": 7
  },
  {
    "role_name": "order_type",
    "value": "express",
    "priority": 8
  }
]
```

**Usage:**
```python
# Message with admin role gets priority 9
producer.send('orders', {
    'order_id': 123,
    'user_role': 'admin',  # Matches rule: priority 9
    'amount': 100
})

# Message with premium user gets priority 7
producer.send('orders', {
    'order_id': 124, 
    'user_role': 'premium',  # Matches rule: priority 7
    'amount': 50
})

# Message with express order gets priority 8
producer.send('orders', {
    'order_id': 125,
    'order_type': 'express',  # Matches rule: priority 8
    'amount': 75
})
```

### 3. **Manual Priority** (Fallback)
If no automatic rules match, you can still specify priority manually:

```python
# Manual priority override
producer.send('orders', {
    'order_id': 126,
    'amount': 200
}, priority=10)  # Explicit priority 10
```

### **Priority Resolution Order:**
1. **Manual priority** (if specified) - Highest precedence
2. **Rule-based priority** (if message matches rules)
3. **Topic-based priority** (if topic has priority configured)
4. **Default priority** (from configuration)

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

### Continuous Message Processing

```python
from kafkaboost.consumer import KafkaboostConsumer

# Create consumer with your user ID from the website
consumer = KafkaboostConsumer(
    bootstrap_servers=['localhost:9092'],
    topics=['orders'],
    group_id='order_processing_group',
    user_id='your-user-id-from-website'
)

try:
    while True:
        # Poll for messages (highest priority first)
        messages = consumer.poll(timeout_ms=1000)
        
        for msg in messages:
            # Get message data
            order_data = msg.value
            priority = order_data.get('priority', 0)
            order_id = order_data.get('order_id')
            
            print(f"Processing order {order_id} with priority {priority}")
            
            # Process the order based on priority
            if priority >= 8:
                print(f"🚨 URGENT: Processing high-priority order {order_id}")
            elif priority >= 5:
                print(f"⚡ Processing medium-priority order {order_id}")
            else:
                print(f"📋 Processing standard order {order_id}")
                
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
```

### Working with Multiple Topics

```python
from kafkaboost.consumer import KafkaboostConsumer

# Consumer can handle multiple topics
consumer = KafkaboostConsumer(
    bootstrap_servers=['localhost:9092'],
    topics=['orders', 'notifications', 'payments'],
    group_id='multi_topic_group',
    user_id='your-user-id-from-website'
)

try:
    while True:
        messages = consumer.poll(timeout_ms=1000)
        
        for msg in messages:
            topic = msg.topic
            data = msg.value
            
            # Handle different message types
            if 'orders' in topic:
                print(f"📦 Order message: {data}")
            elif 'notifications' in topic:
                print(f"🔔 Notification: {data}")
            elif 'payments' in topic:
                print(f"💳 Payment: {data}")
                
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
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

## 🚨 Troubleshooting

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
- `poll(timeout_ms=1000, max_records=None)`: Poll for messages (highest priority first)
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
- `get_config_summary()`: Get configuration summary
- `find_matching_topics(base_topics)`: Find priority topic variants

## 🔄 Migration Guide

### Step 1: Configure on Website
1. **Visit**: [https://master.d1hgz5clxamnqf.amplifyapp.com/](https://master.d1hgz5clxamnqf.amplifyapp.com/)
2. **Login** and configure your topics, priorities, and settings
3. **Copy your User ID** from the dashboard

### Step 2: Update Your Code

#### From Standard Kafka Consumer

```python
# Before
from kafka import KafkaConsumer
consumer = KafkaConsumer('orders', bootstrap_servers=['localhost:9092'])

# After (just add user_id!)
from kafkaboost.consumer import KafkaboostConsumer
consumer = KafkaboostConsumer(
    bootstrap_servers=['localhost:9092'],
    topics=['orders'],
    user_id='your-user-id-from-website'  # Copy from configuration website
)
```

#### From Standard Kafka Producer

```python
# Before
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# After (just add user_id!)
from kafkaboost.producer import KafkaboostProducer
producer = KafkaboostProducer(
    bootstrap_servers=['localhost:9092'],
    user_id='your-user-id-from-website'  # Copy from configuration website
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

## 📄 License

This project extends the existing kafkaboost library with priority-aware features while maintaining backward compatibility.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

---

**KafkaBoost** - Making Kafka priority-aware and production-ready! 🚀
