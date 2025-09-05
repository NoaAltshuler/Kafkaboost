from kafka import KafkaConsumer
from typing import Any, Optional, Union, List, Dict, Tuple
import json
from queue import PriorityQueue
from dataclasses import dataclass
from datetime import datetime
import time
from collections import defaultdict
from kafkaboost.kafka_utils import KafkaConfigManager
from .s3_config_manager import S3ConfigManager
from .priority_consumer_manager import PriorityConsumerManager


class KafkaboostConsumer(KafkaConsumer):
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        topics: Union[str, List[str]],
        group_id: Optional[str] = None,
        number_of_messages: Optional[int] = None,
        config_file: Optional[str] = None,  
        user_id: Optional[str] = None,
        **kwargs: Any
    ):
        """
        Initialize the KafkaboostConsumer with priority support.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            topics: Topic(s) to consume from
            group_id: Consumer group ID
            config_file: Path to config file (optional if using S3)
            user_id: User ID for S3 config manager (optional)
            **kwargs: Additional arguments to pass to KafkaConsumer
        """
        print("Initializing KafkaboostConsumer...")
        # Convert single topic to list
        topics_list = [topics] if isinstance(topics, str) else topics
        
        # Initialize parent KafkaConsumer
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if isinstance(v, bytes) else v,
            **kwargs
        )
        print("KafkaboostConsumer initialized")
        
        # Initialize configuration management
        self.user_id = user_id
        self.s3_config_manager = None
        self.boost_config = {}
        self.priority_consumer_manager = None
        self.priority_boost_enabled = False
        
        # Initialize S3 config manager
        try:
            self.s3_config_manager = S3ConfigManager(user_id=user_id)
            print("✓ Consumer initialized with S3ConfigManager")
        except Exception as e:
            print(f"Warning: Could not initialize S3ConfigManager: {str(e)}")

        # Initialize Kafka utils manager
        try:
            self.kafka_utils_manager = KafkaConfigManager(
                bootstrap_servers=bootstrap_servers,
                user_id=user_id
            )
            if self.kafka_utils_manager:
                boost_topics = self.kafka_utils_manager.find_matching_topics(topics_list)
                print(f"Found matching topics: {boost_topics}")
        except Exception as e:
                print(f"Warning: Could not initialize KafkaConfigManager: {str(e)}")
        
        # Check if priority boost mode should be enabled
        self._initialize_priority_boost_mode(bootstrap_servers, topics_list, group_id, kwargs)
        
        # Initialize iterator-related variables
        self._iterator = None
        self._consumer_timeout = float('inf')
        self._last_poll_time = datetime.now().timestamp()

        # Load priority settings from config
        if self.s3_config_manager:
            self.max_priority = self.s3_config_manager.get_max_priority()

    def _initialize_priority_boost_mode(self, bootstrap_servers, topics_list, group_id, kwargs):
        """
        Initialize priority boost mode if configuration supports it.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            topics_list: List of topics to consume from
            group_id: Consumer group ID
            kwargs: Additional arguments for consumers
        """
        if not self.s3_config_manager or not self.user_id:
            print("ℹ️ Priority boost mode not available (no S3 config or user_id)")
            return
        
        try:
            # Check if priority boost configuration exists
            priority_boost_configs = self.s3_config_manager.get_priority_boost()
            
            if priority_boost_configs:
                # Check if any of our topics have priority boost configuration
                base_topics = [topics_list] if isinstance(topics_list, str) else topics_list
                has_priority_boost_topics = any(
                    config.get('topic_name') in base_topics 
                    for config in priority_boost_configs
                )
                
                if has_priority_boost_topics:
                    print("🚀 Priority boost mode detected - initializing PriorityConsumerManager")
                    
                    # Initialize PriorityConsumerManager
                    # Extract auto_offset_reset from kwargs to avoid duplication
                    auto_offset_reset = kwargs.pop('auto_offset_reset', 'latest')
                    
                    self.priority_consumer_manager = PriorityConsumerManager(
                        bootstrap_servers=bootstrap_servers,
                        base_topics=base_topics,
                        group_id=group_id,
                        user_id=self.user_id,
                        auto_offset_reset=auto_offset_reset,
                        **kwargs
                    )
                    
                    self.priority_boost_enabled = True
                    print("✅ Priority boost mode enabled")
                    return
            
            print("ℹ️ No priority boost configuration found for topics, using standard mode")
            
        except Exception as e:
            print(f"Warning: Could not initialize priority boost mode: {str(e)}")
            print("ℹ️ Falling back to standard mode")

    def _process_priority_messages(self, records: Dict) -> List:
        print("Processing priority messages...")

        queues = [[] for _ in range(self.max_priority + 1)]

        for tp, messages in records.items():
            for message in messages:
                priority = message.value.get("priority", 0)
                queues[priority].append(message)

        sorted_messages = []
        for queue in reversed(queues):
            sorted_messages.extend(queue)

        return sorted_messages
    
    def poll(
        self,
        timeout_ms: int = 1000,
        max_records: Optional[int] = None,
        **kwargs: Any
    ) -> List[Any]:
        """
        Poll for new messages and return them sorted by priority.
        
        Args:
            timeout_ms: Time to wait for messages
            max_records: Maximum number of records to return
            **kwargs: Additional arguments to pass to KafkaConsumer.poll()
            
        Returns:
            List of messages sorted by priority and timestamp
        """
        # Use PriorityConsumerManager if priority boost is enabled
        if self.priority_boost_enabled and self.priority_consumer_manager:
            print("Polling for messages using priority boost mode...")
            return self.priority_consumer_manager.poll(timeout_ms, max_records)
        
        # Standard mode - use original implementation
        print("Polling for messages in standard priority order...")
        raw_records = super().poll(
            timeout_ms=timeout_ms,
            max_records=max_records,
            **kwargs
        )
        
        # Sort messages by priority
        sorted_messages = self._process_priority_messages(raw_records)
        
        # Update last poll time
        self._last_poll_time = datetime.now().timestamp()
        
        return sorted_messages

    async def poll_async(
        self,
        timeout_ms: int = 1000,
        max_records: Optional[int] = None,
        **kwargs: Any
    ) -> List[Any]:
        """
        Async version of poll for non-blocking operation.
        
        Args:
            timeout_ms: Time to wait for messages
            max_records: Maximum number of records to return
            **kwargs: Additional arguments
            
        Returns:
            List of messages sorted by priority and timestamp
        """
        # Use PriorityConsumerManager if priority boost is enabled
        if self.priority_boost_enabled and self.priority_consumer_manager:
            print("Async polling for messages using priority boost mode...")
            return await self.priority_consumer_manager.poll_async(timeout_ms, max_records)
        
        # Standard mode - run sync poll in thread pool
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.poll, timeout_ms, max_records, **kwargs)

    def _message_generator_v2(self):
        """Generator that yields messages in priority order."""
        timeout_ms = 1000 * max(0, self._consumer_timeout - time.time())
        
        # Use our poll method instead of super().poll() to get priority-aware messages
        messages = self.poll(timeout_ms=timeout_ms)
        
        # Yield messages in priority order
        for message in messages:
            if self._closed:
                break
            yield message

    def __iter__(self):
        """Return an iterator that yields messages in priority order."""
        return self

    def __next__(self):
        """Get the next message in priority order."""
        if self._closed:
            raise StopIteration('KafkaConsumer closed')
        
        self._set_consumer_timeout()
        
        while time.time() < self._consumer_timeout:
            if not self._iterator:
                self._iterator = self._message_generator_v2()
            try:
                return next(self._iterator)
            except StopIteration:
                self._iterator = None
                
        raise StopIteration()

    def _set_consumer_timeout(self):
        """Set the consumer timeout based on configuration."""
        if hasattr(self, 'consumer_timeout_ms') and self.consumer_timeout_ms >= 0:
            self._consumer_timeout = time.time() + (
                self.consumer_timeout_ms / 1000.0)

    def refresh_config(self):
        """Refresh configuration from S3."""
        if self.s3_config_manager:
            try:
                self.boost_config = self.s3_config_manager.get_full_config_for_consumer()
                self.max_priority = self.s3_config_manager.get_max_priority()
                print("✓ Configuration refreshed from S3")
                
                # Refresh PriorityConsumerManager if it exists
                if self.priority_consumer_manager:
                    self.priority_consumer_manager.refresh_config()
                    
            except Exception as e:
                print(f"Warning: Failed to refresh config from S3: {str(e)}")

    def get_config_summary(self) -> dict:
        """
        Get a summary of the current configuration.
        
        Returns:
            Dictionary with configuration summary
        """
        summary = {
            'priority_boost_enabled': self.priority_boost_enabled,
            'has_priority_consumer_manager': self.priority_consumer_manager is not None
        }
        
        if self.priority_consumer_manager:
            # Get summary from PriorityConsumerManager
            priority_summary = self.priority_consumer_manager.get_config_summary()
            summary.update(priority_summary)
        elif self.s3_config_manager:
            # Get summary from S3 config manager
            s3_summary = self.s3_config_manager.get_config_summary()
            summary.update(s3_summary)
        else:
            # Fallback summary
            summary.update({
                'config_source': 'none',
                'max_priority': getattr(self, 'max_priority', 10),
                'topics_count': len(self.boost_config.get('Topics_priority', [])),
                'rules_count': len(self.boost_config.get('Rule_Base_priority', [])),
                'boost_configs_count': len(self.boost_config.get('Priority_boost', []))
            })
        
        return summary

    def close(self) -> None:
        """Close the consumer."""
        # Close PriorityConsumerManager if it exists
        if self.priority_consumer_manager:
            self.priority_consumer_manager.close()
            self.priority_consumer_manager = None
        
        # Close the base consumer
        super().close()