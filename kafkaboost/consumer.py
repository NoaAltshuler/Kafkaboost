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
        self.topics_list = [topics] if isinstance(topics, str) else topics
        
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
        self.kafka_utils_manager = None
        
        try:
            self.s3_config_manager = S3ConfigManager(user_id=user_id)
            print("✓ Consumer initialized with S3ConfigManager")
        except Exception as e:
            print(f"Warning: Could not initialize S3ConfigManager: {str(e)}")

        if self.s3_config_manager and self.s3_config_manager.is_priority_boost_configured():
            try:
                # Initialize KafkaConfigManager for topic matching
                self.kafka_utils_manager = KafkaConfigManager(bootstrap_servers, user_id=user_id)
                if self.kafka_utils_manager.connect():
                    print("✓ KafkaConfigManager initialized for priority boost")
                else:
                    print("Warning: Could not connect to Kafka for topic matching")
            except Exception as e:
                print(f"Warning: Could not initialize KafkaConfigManager: {str(e)}")
        
        # After initializing managers, just call subscribe()
        self.subscribe()
    
        
       
        # Initialize iterator-related variables
        self._iterator = None
        self._consumer_timeout = float('inf')
        self._last_poll_time = datetime.now().timestamp()

    def subscribe(self, topics=None, pattern=None, listener=None):
        """
        Override the subscribe function of Kafka class.
        This method handles priority boost configuration and subscription.
        """
        # Prefer passed topics when provided, fall back to self.topics_list
        if topics is None:
            topics = self.topics_list
            
        if self.s3_config_manager and self.s3_config_manager.is_priority_boost_configured():
            self.boost_config = topics
            if self.kafka_utils_manager:
                self.boost_topics = self.kafka_utils_manager.find_matching_topics(topics)
                print(f"Found matching topics: {self.boost_topics}")
                # Subscribe to the boost topics - extract topic names from the dictionary
                if self.boost_topics and any(self.boost_topics.values()):
                    # Extract all unique topic names from the boost_topics dictionary
                    all_topic_names = set()
                    for topic_list in self.boost_topics.values():
                        if isinstance(topic_list, list):
                            all_topic_names.update(topic_list)
                        elif isinstance(topic_list, str):
                            all_topic_names.add(topic_list)
                    
                    topic_names_list = list(all_topic_names)
                    if topic_names_list:  # Only subscribe if we have actual topics
                        print(f"Subscribing to topics: {topic_names_list}")
                        super().subscribe(topics=topic_names_list)
                        
                        # Create priority queues after subscription
                        self.create_priority_boost_queues()
                    else:
                        # Fallback to original topics if no boost topics found
                        print("No boost topics found, falling back to original topics")
                        super().subscribe(topics=topics)
                else:
                    # No boost topics or empty results, use original topics
                    print("No boost topics configured, using original topics")
                    super().subscribe(topics=topics)
            else:
                print("Warning: KafkaConfigManager not available for topic matching")
                super().subscribe(topics=topics)
        else:
            # Standard subscription
            super().subscribe(topics=topics, pattern=pattern, listener=listener)

    def create_priority_boost_queues(self):
        """
        Create priority boost queues.
        """
        # Get max priority from config
        max_priority = self.s3_config_manager.get_max_priority()
        
        # Create priority queues from 0 to max_priority (not just from min_priority)
        self.priority_queues = {}
        for priority in range(0, max_priority + 1):
            self.priority_queues[priority] = []
        
        # Keep priority structures consistent
        self.max_priority = max_priority
        self.priority_levels = max_priority
        
        print(f"Created priority queues for priorities 0 to {max_priority}")
        return self.priority_queues


    def _process_priority_messages(self, records: Dict) -> List:
        print("Processing priority messages...")

        queues = [[] for _ in range(self.s3_config_manager.get_max_priority() + 1)]

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
        if self.s3_config_manager and self.s3_config_manager.is_priority_boost_configured():
            # Use priority boost logic
            try:
                # Get topics organized by priority
                topics_by_priority = self.get_topics_by_priority_from_config()
                if topics_by_priority:
                    # For synchronous usage, we need to run the async function
                    import asyncio
                    try:
                        # Try to get the current event loop
                        loop = asyncio.get_running_loop()
                        # If we're in async context, don't return []; use standard poll to avoid data loss
                        print("Warning: poll() called in async context. Using standard poll to avoid data loss.")
                        return self._standard_poll(timeout_ms, max_records, **kwargs)
                    except RuntimeError:
                        # No event loop running, create a new one
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            print("Debug: Running poll_topics_async with new event loop")
                            messages = loop.run_until_complete(
                                self.poll_topics_async(topics_by_priority, timeout_ms, max_records, **kwargs)
                            )
                            print(f"Debug: poll_topics_async returned {len(messages)} messages")
                            return messages
                        except Exception as async_error:
                            print(f"Debug: Error in poll_topics_async: {str(async_error)}")
                            return self._standard_poll(timeout_ms, max_records, **kwargs)
                        finally:
                            loop.close()
                else:
                    print("No priority topics configured, falling back to standard poll")
                    return self._standard_poll(timeout_ms, max_records, **kwargs)
            except Exception as e:
                print(f"Error in priority boost poll: {str(e)}, falling back to standard poll")
                return self._standard_poll(timeout_ms, max_records, **kwargs)
        else:
            # Standard priority processing
            return self._standard_poll(timeout_ms, max_records, **kwargs)

    def _standard_poll(self, timeout_ms: int, max_records: Optional[int], **kwargs) -> List[Any]:
        """Standard poll implementation with priority message processing."""
        print("Polling for messages in priority order...")
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

 
    async def poll_topics_async(self, topics_by_priority: Dict[int, List[str]], timeout_ms: int = 1000, max_records: Optional[int] = None, **kwargs):
        """
        Async, priority-aware poll across per-priority topic lists.
        Strategy:
          - Subscribe once to the union of all topics.
          - Keep per-priority internal queues.
          - If queues have data: serve highest non-empty.
          - If empty: resume only highest priorities first, pause lower ones, poll, enqueue, serve.
          - If still empty everywhere: resume all and do a discovery poll.

        Args:
            topics_by_priority: Dictionary mapping priority (int) to list of topic names
            timeout_ms: Time to wait for messages from each topic list
            max_records: Maximum number of records to return
            **kwargs: Additional arguments to pass to KafkaConsumer.poll()
            
        Returns:
            List of messages (highest priority first).
        """
        import asyncio
        
        # Ensure priority queues exist
        if not hasattr(self, 'priority_queues') or not self.priority_queues:
            self.create_priority_boost_queues()
        
        # ---- 1) Ensure single subscription to union of topics
        # Build union list (dedup)
        union_topics: List[str] = []
        seen = set()
        for _, tlist in sorted(topics_by_priority.items()):
            for t in tlist:
                if t not in seen:
                    seen.add(t)
                    union_topics.append(t)

        # If not already subscribed to exactly this set, (re)subscribe ONCE
        current = set(self.subscription()) if hasattr(self, "subscription") else set()
        if set(union_topics) != current:
            # This will replace previous subscription (correct)
            super().subscribe(union_topics)

        # ---- 2) Build/refresh topic->priority mapping
        if not hasattr(self, "topic_priority"):
            self.topic_priority = {}
        for p, tlist in topics_by_priority.items():
            for t in tlist:
                self.topic_priority[t] = p

        # ---- 3) Ensure per-priority structures exist
        max_p = max(topics_by_priority.keys()) if topics_by_priority else 0
        if not hasattr(self, "priority_levels"):
            self.priority_levels = max_p
        else:
            self.priority_levels = max(self.priority_levels, max_p)

        # Use existing priority_queues if available, otherwise create new ones
        if not hasattr(self, "priority_queues"):
            self.priority_queues = {i: [] for i in range(self.priority_levels + 1)}
        else:
            for i in range(self.priority_levels + 1):
                self.priority_queues.setdefault(i, [])

        if not hasattr(self, "priority_partitions"):
            self.priority_partitions = {i: set() for i in range(self.priority_levels + 1)}

        # Map assigned partitions to priorities (best-effort; do nothing if none assigned yet)
        # TODO: Future improvement: add a listener with on_partitions_assigned/revoked to refresh 
        # priority_partitions, then your pause/resume is always accurate during rebalances
        try:
            assigned = self.assignment()  # set[TopicPartition]
            # reset and rebuild partition buckets
            self.priority_partitions = {i: set() for i in range(self.priority_levels + 1)}
            for tp in assigned:
                # Ensure tp is a TopicPartition object and has a topic attribute
                if hasattr(tp, 'topic') and tp.topic:
                    pr = self.topic_priority.get(tp.topic, 0)
                    self.priority_partitions[pr].add(tp)
        except Exception as e:
            print(f"Warning: Could not map partitions to priorities: {str(e)}")
            pass

        # Helper: enqueue records by topic priority
        def _enqueue_by_priority(record_map) -> int:
            count = 0
            for tp, msgs in record_map.items():
                # tp is a TopicPartition object, so we need to access its topic attribute
                topic_name = tp.topic if hasattr(tp, 'topic') else str(tp)
                pr = self.topic_priority.get(topic_name, 0)
                bucket = self.priority_queues.setdefault(pr, [])
                for m in msgs:
                    bucket.append(m)
                    count += 1
            return count

        # Helper: pause/resume sets of partitions based on active priorities
        # TEMPORARILY DISABLED due to TopicPartition compatibility issues
        def _apply_pause_resume(active_priorities: List[int]) -> None:
            # TODO: Re-enable pause/resume once TopicPartition compatibility is resolved
            # For now, skip pause/resume to avoid errors while maintaining priority functionality
            print(f"Debug: Pause/resume temporarily disabled - active priorities: {active_priorities}")
            pass

        # Helper: serve from highest non-empty queues
        def _serve_from_queues(limit: Optional[int]) -> List[Any]:
            out: List[Any] = []
            for pr in range(self.priority_levels, -1, -1):
                q = self.priority_queues.get(pr, [])
                while q and (limit is None or len(out) < limit):
                    out.append(q.pop(0))
                if limit is not None and len(out) >= limit:
                    break
            return out

        # ---- 4) If we already have buffered messages, serve them first
        buffered = _serve_from_queues(max_records)
        if buffered:
            return buffered

        # ---- 5) Nothing buffered → try priorities from highest to lowest, ONE AT A TIME
        loop = asyncio.get_running_loop()
        for pr in range(self.priority_levels, -1, -1):
            print(f"Debug: Polling priority level {pr} only")
            
            # Only poll from the current priority level, not from pr..max
            # This ensures we get messages in strict priority order
            _apply_pause_resume([pr])  # Only activate current priority level

            # do a blocking poll in a worker thread so we don't block the event loop
            record_map = await loop.run_in_executor(
                None,
                lambda: super(type(self), self).poll(timeout_ms=timeout_ms, max_records=max_records, **kwargs)
            )
            _enqueue_by_priority(record_map)

            # Check if we got messages at the current priority level only
            current_priority_messages = self.priority_queues.get(pr, [])
            if current_priority_messages:
                print(f"Debug: Found {len(current_priority_messages)} messages at priority {pr}")
                return _serve_from_queues(max_records)
            else:
                print(f"Debug: No messages at priority {pr}, moving to next level")

        # ---- 6) Still empty → discovery poll with everything resumed
        print("Debug: No messages found at any priority level, doing discovery poll")
        _apply_pause_resume(active_priorities=[])  # empty list => resume all
        record_map = await loop.run_in_executor(
            None,
            lambda: super(type(self), self).poll(timeout_ms=timeout_ms, max_records=max_records, **kwargs)
        )
        _enqueue_by_priority(record_map)
        return _serve_from_queues(max_records)

    def get_topics_by_priority_from_config(self) -> Dict[int, List[str]]:
        """
        Helper method to get topics organized by priority from S3 config.
        This creates the input format needed for poll_topics_async.
        
        Returns:
            Dictionary mapping priority to list of topics
        """
        if not self.s3_config_manager:
            return {}
        
        topics_by_priority = {}
        
        # First, get the minimum boost priority across all boost configurations
        priority_boost = self.s3_config_manager.get_priority_boost()
        min_boost_priority = float('inf')
        if priority_boost:
            min_boost_priority = min(
                boost_config.get('priority_boost_min_value', 0) 
                for boost_config in priority_boost
            )
        else:
            min_boost_priority = 0
        
        # Get topics priority from config - these are only polled from base priority
        topics_priority = self.s3_config_manager.get_topics_priority()
        for topic_config in topics_priority:
            topic_name = topic_config.get('topic', '')
            if topic_name:
                # Non-boost topics are only polled from base priority (priority < min_boost_priority)
                base_priority = 0  # Base priority is always 0
                if base_priority not in topics_by_priority:
                    topics_by_priority[base_priority] = []
                topics_by_priority[base_priority].append(topic_name)
        
        # Get priority boost topics - these create priority-specific topic names
        for boost_config in priority_boost:
            base_topic_name = boost_config.get('topic_name', '')
            min_priority = boost_config.get('priority_boost_min_value', 0)
            max_priority = self.s3_config_manager.get_max_priority()
            
            if base_topic_name:
                # For each priority level from min_priority to max_priority,
                # create the topic name that would be created by check_and_create_priority_topics()
                for priority in range(min_priority, max_priority + 1):
                    if priority not in topics_by_priority:
                        topics_by_priority[priority] = []
                    
                    # Add the priority-specific topic name (e.g., test_topic_5, test_topic_6, etc.)
                    priority_topic_name = f"{base_topic_name}_{priority}"
                    topics_by_priority[priority].append(priority_topic_name)
                    
                    # Also add the base topic to the min_priority level for low-priority messages
                    # Note: This means the base topic (e.g., test_topic) will be polled at both 
                    # priority 0 (if min_priority > 0) and at min_priority level. This allows
                    # low-priority traffic on the base topic and high-priority traffic on suffixed topics.
                    if priority == min_priority:
                        topics_by_priority[priority].append(base_topic_name)
        
        return topics_by_priority

    def _message_generator_v2(self):
        """Generator that yields messages in priority order."""
        timeout_ms = 1000 * max(0, self._consumer_timeout - time.time())
        record_map = super().poll(timeout_ms=timeout_ms, update_offsets=False)
        
        # Sort messages by priority
        sorted_messages = self._process_priority_messages(record_map)
        
        # Yield messages in priority order
        for message in sorted_messages:
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
            except Exception as e:
                print(f"Warning: Failed to refresh config from S3: {str(e)}")

    def get_config_summary(self) -> dict:
        """
        Get a summary of the current configuration.
        
        Returns:
            Dictionary with configuration summary
        """
        if self.s3_config_manager:
            return self.s3_config_manager.get_config_summary()
  
    def close(self) -> None:
        """Close the consumer."""
        super().close()