"""
Priority Consumer Manager for KafkaBoost Priority Boost Mode.

This module implements a multi-consumer architecture where separate consumers
are created for each priority level, enabling true priority-based message processing.
"""

import json
import time
import threading
import asyncio
from typing import Any, Optional, Union, List, Dict, Set, Callable
from datetime import datetime
from collections import defaultdict, deque
from dataclasses import dataclass
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from .s3_config_manager import S3ConfigManager
from .kafka_utils import KafkaConfigManager


@dataclass
class PriorityConsumerConfig:
    """Configuration for a priority consumer."""
    priority_level: int
    topics: List[str]
    group_id: str
    is_base_consumer: bool = False


class PriorityConsumer:
    """
    Individual consumer for a specific priority level.
    Handles consumption from priority-specific topics (e.g., orders_5, orders_7).
    """
    
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        config: PriorityConsumerConfig,
        **kwargs: Any
    ):
        """
        Initialize a priority consumer.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            config: Priority consumer configuration
            **kwargs: Additional arguments for KafkaConsumer
        """
        self.priority_level = config.priority_level
        self.topics = config.topics
        self.group_id = config.group_id
        self.is_base_consumer = config.is_base_consumer
        
        # Initialize the underlying Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=config.group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if isinstance(v, bytes) else v,
            **kwargs
        )
        print(f"Consumer initialized for priority {self.priority_level}")
        # Subscribe to topics
        self.consumer.subscribe(config.topics)
        
        # Message queue for this priority level
        self.message_queue = deque()
        self.queue_lock = threading.Lock()
        
        # Statistics
        self.messages_consumed = 0
        self.last_poll_time = None
        self.is_paused = False
        
        print(f"✓ PriorityConsumer initialized for priority {self.priority_level}")
        print(f"  Topics: {config.topics}")
        print(f"  Group ID: {config.group_id}")
        print(f"  Is Base Consumer: {config.is_base_consumer}")
    
    def poll(self, timeout_ms: int = 1000,max_records: Optional[int] = None,) -> List[Any]:
        """
        Poll for messages from this priority consumer.
        
        Args:
            timeout_ms: Timeout in milliseconds
            
        Returns:
            List of messages from this priority level
        """
        if self.is_paused:
            return []
        
        try:
            # Poll from Kafka
            raw_records = self.consumer.poll(timeout_ms=timeout_ms)
            messages = []
            
            # Process raw records
            for topic_partition, message_list in raw_records.items():
                for message in message_list:
                    # Add priority level to message if not present
                    if isinstance(message.value, dict) and 'priority' not in message.value:
                        message.value['priority'] = self.priority_level
                    messages.append(message)
            
            # Add messages to queue
            with self.queue_lock:
                self.message_queue.extend(messages)
                self.messages_consumed += len(messages)
            
            self.last_poll_time = datetime.now()
            return messages
            
        except KafkaError as e:
            print(f"Error polling priority {self.priority_level}: {e}")
            return []
    
    def get_messages_from_queue(self,max_records: Optional[int] = None) -> List[Any]:
        """
        Get messages from the internal queue.
        
        Args:
            max_messages: Maximum number of messages to return
            
        Returns:
            List of messages from the queue
        """
        with self.queue_lock:
            if max_records is None:
                messages = list(self.message_queue)
                self.message_queue.clear()
            else:
                messages = []
                for _ in range(min(max_records, len(self.message_queue))):
                    if self.message_queue:
                        messages.append(self.message_queue.popleft())
            
            return messages
    
    def has_messages(self) -> bool:
        """Check if this consumer has messages in its queue."""
        with self.queue_lock:
            return len(self.message_queue) > 0
    
    def queue_size(self) -> int:
        """Get the number of messages in the queue."""
        with self.queue_lock:
            return len(self.message_queue)
    
    def pause(self):
        """Pause this consumer."""
        self.is_paused = True
        print(f"⏸️ Paused priority {self.priority_level} consumer")
    
    def resume(self):
        """Resume this consumer."""
        self.is_paused = False
        print(f"▶️ Resumed priority {self.priority_level} consumer")
    
    def close(self):
        """Close this consumer."""
        self.consumer.close()
        print(f"✓ Priority {self.priority_level} consumer closed")


class PriorityQueueManager:
    """
    Manages priority queues and message distribution logic.
    """
    
    def __init__(self, max_priority: int):
        """
        Initialize the priority queue manager.
        
        Args:
            max_priority: Maximum priority level supported
        """
        self.max_priority =max_priority
        self.priority_consumers: Dict[int, PriorityConsumer] = {}
        self.base_consumer: Optional[PriorityConsumer] = None
        
        # Statistics
        self.total_messages_served = 0
        self.priority_stats = defaultdict(int)
        
        print(f"✓ PriorityQueueManager initialized with max priority {max_priority}")
    
    def add_consumer(self, consumer: PriorityConsumer):
        """
        Add a consumer to the priority queue manager.
        
        Args:
            consumer: Priority consumer to add
        """
        if consumer.is_base_consumer:
            self.base_consumer = consumer
            print(f"✓ Added base consumer")
        else:
            self.priority_consumers[consumer.priority_level] = consumer
            print(f"✓ Added priority {consumer.priority_level} consumer")
    
    def get_highest_priority_with_messages(self) -> Optional[int]:
        """
        Get the highest priority level that has messages.
        
        Returns:
            Highest priority level with messages, or None if no messages
        """
        # Check priority consumers first (highest to lowest)
        for priority in range(self.max_priority, -1, -1):
            if priority in self.priority_consumers:
                if self.priority_consumers[priority].has_messages():
                    return priority
        
        # Check base consumer last
        if self.base_consumer and self.base_consumer.has_messages():
            return -1  # Use -1 to represent base consumer
        
        return None
    
    def get_messages_from_highest_priority(self, max_messages: Optional[int] = None) -> List[Any]:
        """
        Get messages from the highest priority consumer that has messages.
        
        Args:
            max_messages: Maximum number of messages to return
            
        Returns:
            List of messages from highest priority queue
        """
        highest_priority = self.get_highest_priority_with_messages()
        if highest_priority is None:
            return []
        
        if highest_priority == -1:
            # Base consumer
            messages = self.base_consumer.get_messages_from_queue(max_messages)
        else:
            # Priority consumer
            messages = self.priority_consumers[highest_priority].get_messages_from_queue(max_messages)
        
        # Update statistics
        self.total_messages_served += len(messages)
        self.priority_stats[highest_priority] += len(messages)
        
        return messages
    
    def should_pause_lower_priorities(self) -> bool:
        """
        Check if lower priority consumers should be paused.
        
        Returns:
            True if any higher priority consumer has messages
        """
        highest_priority = self.get_highest_priority_with_messages()
        if highest_priority is None or highest_priority == -1:
            return False
        
        # If we have messages in priority consumers (not base), pause lower priorities
        return highest_priority > 0
    
    def manage_consumer_pausing(self):
        """
        Manage pausing/resuming of consumers based on priority queue status.
        """
        should_pause = self.should_pause_lower_priorities()
        
        if should_pause:
            # Pause base consumer and lower priority consumers
            if self.base_consumer and not self.base_consumer.is_paused:
                self.base_consumer.pause()
            
            highest_priority = self.get_highest_priority_with_messages()
            if highest_priority is not None:
                for priority in range(highest_priority):
                    if priority in self.priority_consumers and not self.priority_consumers[priority].is_paused:
                        self.priority_consumers[priority].pause()
        else:
            # Resume all consumers
            if self.base_consumer and self.base_consumer.is_paused:
                self.base_consumer.resume()
            
            for consumer in self.priority_consumers.values():
                if consumer.is_paused:
                    consumer.resume()
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get the current status of all priority queues.
        
        Returns:
            Dictionary with queue status information
        """
        status = {
            'total_messages_served': self.total_messages_served,
            'priority_stats': dict(self.priority_stats),
            'queue_sizes': {},
            'paused_consumers': []
        }
        
        # Base consumer status
        if self.base_consumer:
            status['queue_sizes']['base'] = self.base_consumer.queue_size()
            if self.base_consumer.is_paused:
                status['paused_consumers'].append('base')
        
        # Priority consumer status
        for priority, consumer in self.priority_consumers.items():
            status['queue_sizes'][f'priority_{priority}'] = consumer.queue_size()
            if consumer.is_paused:
                status['paused_consumers'].append(f'priority_{priority}')
        
        return status
    
    def close_all_consumers(self):
        """Close all consumers managed by this queue manager."""
        if self.base_consumer:
            self.base_consumer.close()
        
        for consumer in self.priority_consumers.values():
            consumer.close()
        
        print("✓ All consumers closed")


class PriorityConsumerManager:
    """
    Main manager class that orchestrates multiple priority consumers.
    Implements the priority boost mode with separate consumers for each priority level.
    """
    
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        base_topics: Union[str, List[str]],
        group_id: str,
        user_id: Optional[str] = None,
        auto_offset_reset: str = 'latest',
        **kwargs: Any
    ):
        """
        Initialize the Priority Consumer Manager.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            base_topics: Base topic(s) to consume from
            group_id: Consumer group ID
            user_id: User ID for S3 config manager
            auto_offset_reset: Offset reset policy for priority consumers ('earliest', 'latest', 'none')
            **kwargs: Additional arguments for KafkaConsumer
        """
        self.bootstrap_servers = bootstrap_servers
        self.base_topics = [base_topics] if isinstance(base_topics, str) else base_topics
        self.group_id = group_id
        self.user_id = user_id
        self.auto_offset_reset = auto_offset_reset
        self.kwargs = kwargs
        
        # Initialize configuration management
        self.s3_config_manager = None
        self.kafka_utils_manager = None
        self.priority_queue_manager = None
        
        # Consumer management
        self.consumers: Dict[int, PriorityConsumer] = {}
        self.base_consumer: Optional[PriorityConsumer] = None
        self.priority_boost_enabled = False
        self.max_priority = 10
        self.priority_boost_configs = []
        
        # Polling state
        self._last_poll_time = datetime.now().timestamp()
        self._polling_active = False
        
        print("🚀 Initializing PriorityConsumerManager...")
        self._initialize_configuration()
        self._initialize_consumers()
        print("✅ PriorityConsumerManager initialized successfully")
    
    def _initialize_configuration(self):
        """Initialize S3 and Kafka configuration managers."""
        try:
            self.s3_config_manager = S3ConfigManager(user_id=self.user_id)
            self.max_priority = self.s3_config_manager.get_max_priority()
            self.priority_boost_configs = self.s3_config_manager.get_priority_boost()
            
            if self.priority_boost_configs:
                self.priority_boost_enabled = True
                print(f"✓ Priority boost enabled with {len(self.priority_boost_configs)} configurations")
            else:
                print("ℹ️ No priority boost configuration found, using standard mode")
                
        except Exception as e:
            print(f"Warning: Could not initialize S3ConfigManager: {str(e)}")
        
        try:
            self.kafka_utils_manager = KafkaConfigManager(
                bootstrap_servers=self.bootstrap_servers,
                user_id=self.user_id
            )
            print("✓ KafkaConfigManager initialized")
        except Exception as e:
            print(f"Warning: Could not initialize KafkaConfigManager: {str(e)}")
    
    def _initialize_consumers(self):
        """Initialize all priority consumers based on configuration."""
        if not self.priority_boost_enabled:
            # Standard mode - create single base consumer
            self._create_base_consumer()
            return
        
        # Priority boost mode - ensure priority topics exist before creating consumers
        if self.kafka_utils_manager:
            print("🔧 Ensuring priority topics exist...")
            if self.kafka_utils_manager.check_and_create_priority_topics():
                print("✅ Priority topics verified/created successfully")
            else:
                print("⚠️ Warning: Could not verify/create priority topics")
        
        # Priority boost mode - create multiple consumers
        self.priority_queue_manager = PriorityQueueManager(self.max_priority)
        
        # Create base consumer for original topics
        self._create_base_consumer()
        
        # Create priority consumers for each priority level
        # Collect all topics that need priority boost
        priority_boost_topics = {}
        for boost_config in self.priority_boost_configs:
            topic_name = boost_config.get('topic_name')
            min_priority = boost_config.get('priority_boost_min_value', 0)
            
            if topic_name in self.base_topics:
                priority_boost_topics[topic_name] = min_priority
        
        # Create one consumer per priority level, subscribing to all relevant topics
        self._create_priority_consumers_by_level(priority_boost_topics)
    
    def _create_base_consumer(self):
        """Create the base consumer for original topics."""
        config = PriorityConsumerConfig(
            priority_level=0,  # Base consumer uses priority 0
            topics=self.base_topics.copy(),
            group_id=f"{self.group_id}_base",
            is_base_consumer=True
        )
        
        self.base_consumer = PriorityConsumer(
            bootstrap_servers=self.bootstrap_servers,
            config=config,
            **self.kwargs
        )
        
        if self.priority_queue_manager:
            self.priority_queue_manager.add_consumer(self.base_consumer)
        
        print(f"✓ Base consumer created for topics: {self.base_topics}")
    
    def _create_priority_consumers_by_level(self, priority_boost_topics: Dict[str, int]):
        """
        Create one consumer per priority level, subscribing to all relevant topics.
        
        Args:
            priority_boost_topics: Dict mapping topic names to their minimum priority
        """
        # Group topics by priority level
        topics_by_priority = {}
        
        for topic_name, min_priority in priority_boost_topics.items():
            for priority in range(min_priority, self.max_priority + 1):
                priority_topic = f"{topic_name}_{priority}"
                
                if priority not in topics_by_priority:
                    topics_by_priority[priority] = []
                topics_by_priority[priority].append(priority_topic)
        
        # Create one consumer per priority level
        for priority, topics in topics_by_priority.items():
            config = PriorityConsumerConfig(
                priority_level=priority,
                topics=topics,
                group_id=f"{self.group_id}_priority_{priority}",
                is_base_consumer=False
            )
            
            # Create kwargs for priority consumer with specified auto_offset_reset
            priority_kwargs = self.kwargs.copy()
            priority_kwargs['auto_offset_reset'] = self.auto_offset_reset
            
            consumer = PriorityConsumer(
                bootstrap_servers=self.bootstrap_servers,
                config=config,
                **priority_kwargs
            )
            
            self.consumers[priority] = consumer
            
            if self.priority_queue_manager:
                self.priority_queue_manager.add_consumer(consumer)
            
            print(f"✓ Priority {priority} consumer created and subscribed to {len(topics)} topics: {topics}")
    
    def poll(self, timeout_ms: int = 1000, max_records: Optional[int] = None) -> List[Any]:
        """
        Poll for messages using priority-aware logic.
        
        Args:
            timeout_ms: Timeout in milliseconds
            max_records: Maximum number of records to return
            
        Returns:
            List of messages from highest priority queue
        """
        if not self.priority_boost_enabled:
            # Standard mode - use base consumer
            if self.base_consumer:
                return self.base_consumer.poll(timeout_ms)
            return []
        
        # Priority boost mode - use priority queue manager
        if not self.priority_queue_manager:
            return []
        
        self._poll_from_active_consumers(timeout_ms)
        # Check if we have messages in priority queues
        messages = self.priority_queue_manager.get_messages_from_highest_priority(max_records)
        print(f"Messages from highest priority is priority", self.priority_queue_manager.get_highest_priority_with_messages())

          # Manage consumer pausing based on queue status
        self.priority_queue_manager.manage_consumer_pausing()
        
        self._last_poll_time = datetime.now().timestamp()
        return messages

    
    def _poll_from_active_consumers(self, timeout_ms: int):
        """Poll from all active consumers to fill priority queues."""
        # Poll from priority consumers first (highest priority first)
        active_priority_consumers = [
            consumer for consumer in self.consumers.values()
            if not consumer.is_paused
        ]
        
        # Sort by priority (highest first)
        active_priority_consumers.sort(key=lambda c: c.priority_level, reverse=True)
        
        if active_priority_consumers:
            # Give priority consumers most of the timeout
            priority_timeout = int(timeout_ms * 0.8)  # 80% for priority consumers
            per_consumer_timeout = priority_timeout // len(active_priority_consumers)
            
            for consumer in active_priority_consumers:
                consumer.poll(per_consumer_timeout)
        
        # Poll from base consumer last (lowest priority)
        if self.base_consumer and not self.base_consumer.is_paused:
            base_timeout = int(timeout_ms * 0.2)  # 20% for base consumer
            self.base_consumer.poll(base_timeout)
    
    async def poll_async(self, timeout_ms: int = 1000, max_records: Optional[int] = None) -> List[Any]:
        """
        Async version of poll for non-blocking operation.
        
        Args:
            timeout_ms: Timeout in milliseconds
            max_records: Maximum number of records to return
            
        Returns:
            List of messages from highest priority queue
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.poll, timeout_ms, max_records)
    
    def get_config_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current configuration and status.
        
        Returns:
            Dictionary with configuration summary
        """
        summary = {
            'priority_boost_enabled': self.priority_boost_enabled,
            'max_priority': self.max_priority,
            'base_topics': self.base_topics,
            'group_id': self.group_id,
            'user_id': self.user_id,
            'consumers_count': len(self.consumers),
            'has_base_consumer': self.base_consumer is not None,
            'last_poll_time': self._last_poll_time
        }
        
        if self.priority_queue_manager:
            summary['queue_status'] = self.priority_queue_manager.get_queue_status()
        
        if self.s3_config_manager:
            summary['s3_config'] = self.s3_config_manager.get_config_summary()
        
        return summary
    
    def refresh_config(self):
        """Refresh configuration from S3 and reinitialize if needed."""
        if self.s3_config_manager:
            try:
                self.s3_config_manager.force_refresh()
                new_max_priority = self.s3_config_manager.get_max_priority()
                new_boost_configs = self.s3_config_manager.get_priority_boost()
                
                # Check if configuration changed significantly
                if (new_max_priority != self.max_priority or 
                    new_boost_configs != self.priority_boost_configs):
                    
                    print("🔄 Configuration changed, reinitializing consumers...")
                    self._close_all_consumers()
                    self.max_priority = new_max_priority
                    self.priority_boost_configs = new_boost_configs
                    self.priority_boost_enabled = bool(new_boost_configs)
                    
                    # Ensure priority topics exist before reinitializing consumers
                    if self.kafka_utils_manager and self.priority_boost_enabled:
                        print("🔧 Ensuring priority topics exist for new configuration...")
                        if self.kafka_utils_manager.check_and_create_priority_topics():
                            print("✅ Priority topics verified/created successfully")
                        else:
                            print("⚠️ Warning: Could not verify/create priority topics")
                    
                    self._initialize_consumers()
                    print("✅ Consumers reinitialized with new configuration")
                else:
                    print("✓ Configuration refreshed (no changes detected)")
                    
            except Exception as e:
                print(f"Error refreshing configuration: {str(e)}")
    
    def _close_all_consumers(self):
        """Close all consumers."""
        if self.base_consumer:
            self.base_consumer.close()
            self.base_consumer = None
        
        for consumer in self.consumers.values():
            consumer.close()
        self.consumers.clear()
        
        if self.priority_queue_manager:
            self.priority_queue_manager.close_all_consumers()
            self.priority_queue_manager = None
    
    def close(self):
        """Close all consumers and cleanup resources."""
        print("🔄 Closing PriorityConsumerManager...")
        self._close_all_consumers()
        print("✅ PriorityConsumerManager closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
