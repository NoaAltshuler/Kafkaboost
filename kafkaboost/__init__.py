"""
KafkaBoost - Enhanced Apache Kafka library with priority-based message processing.

This package extends standard Kafka functionality with:
- Priority-based message routing and consumption
- Automatic topic creation and management
- S3 configuration integration
- Async support for non-blocking operations
"""

__version__ = "0.1.0"
__author__ = "KafkaBoost Team"
__email__ = "support@kafkaboost.com"

from .consumer import KafkaboostConsumer
from .producer import KafkaboostProducer
from .kafka_utils import KafkaConfigManager

__all__ = [
    "KafkaboostConsumer",
    "KafkaboostProducer", 
    "KafkaConfigManager"
]
