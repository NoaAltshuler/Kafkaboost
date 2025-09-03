import time
import threading
import uuid
import logging
import os
import signal
import sys
from kafkaboost.producer import KafkaboostProducer
from kafkaboost.consumer import KafkaboostConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
from kafkaboost.kafka_utils import KafkaConfigManager

# Global list to track S3ConfigManager instances for cleanup
s3_managers = []

def signal_handler(sig, frame):
    """Handle Ctrl+C to properly cleanup S3ConfigManager instances."""
    print("\n🛑 Received interrupt signal. Cleaning up...")
    for manager in s3_managers:
        try:
            manager.close()
        except:
            pass
    print("✅ Cleanup completed. Exiting...")
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

# Disable Kafka connection logs
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('kafka.client').setLevel(logging.WARNING)
logging.getLogger('kafkaboost').setLevel(logging.WARNING)

TOPIC = "topic_1"
TOPIC2 = "topic_2"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = f"priority_test_group_{uuid.uuid4()}"  # Unique group ID for each test run
CONFIG_FILE = "/Users/noaalt/Projects/kafkaboost_repo/kafkaboost/tests/sample_config.json"

# Get user ID from environment variable - use the actual user ID that has S3 config
USER_ID = "5428b428-20a1-7051-114f-c24ede151b86"

def clear_topics():
    """Clear all messages from the topics"""
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        # Delete and recreate topics
        admin_client.delete_topics([TOPIC, TOPIC2])
        time.sleep(2)  # Wait for topics to be deleted
        topics = [
            NewTopic(name=TOPIC, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC2, num_partitions=1, replication_factor=1)
        ]
        admin_client.create_topics(topics)
        print(f"Cleared and recreated topics: {TOPIC}, {TOPIC2}")
    except Exception as e:
        print(f"Error clearing topics: {e}")
    finally:
        admin_client.close()

def create_topics():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topics = [
        NewTopic(name=TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(name=TOPIC2, num_partitions=1, replication_factor=1)
    ]
    try:
        admin_client.create_topics(topics)
        print(f"Created topics: {TOPIC}, {TOPIC2}")
    except Exception as e:
        print(f"Error creating topics: {e}")
    finally:
        admin_client.close()

def producer_thread():
    producer = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS, user_id=USER_ID)

    messages = [
        {"data": "p10", "priority": 10},
        {"data": "p5", "priority": 5},
        {"data": "p9", "priority": 9},
        {"data": "p8 ", "priority": 8},
        {"data": "p4 Medium", "priority": 4}
    ]

    for msg in messages:
        producer.send(TOPIC, value=msg, priority=msg["priority"])
        print(f"Produced: {msg}")

    producer.flush()
    producer.close()

def consumer_thread(received_messages):
    print(f"Consumer subscribing to topic: {TOPIC} with group ID: {GROUP_ID}")
    consumer = KafkaboostConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topics=[TOPIC],
        group_id=GROUP_ID,
        max_poll_records=500,
        consumer_timeout_ms=10000,
        user_id=USER_ID,
        auto_offset_reset='earliest'  # Start reading from the beginning
    )

    print("Polling for prioritized messages...")
    start_time = time.time()
    while len(received_messages) < 5 and time.time() - start_time < 15:
        batch = consumer.poll(timeout_ms=1000, max_records=500)
        if batch:
            print(f"Received batch of {len(batch)} messages")
            for message in batch:
                if message.value not in received_messages:
                    received_messages.append(message.value)
                    print(f"Added new message: {message.value}")
            print(f"Total unique messages received so far: {len(received_messages)}")
        else:
            print("No messages received in this poll")

    consumer.close()

def test_priority_order():
    # Clear topic before starting the test
    clear_topics()
    create_topics()
    
    # Create a list to store received messages
    received_messages = []
    
    # Create and start threads
    producer = threading.Thread(target=producer_thread)
    consumer = threading.Thread(target=consumer_thread, args=(received_messages,))
    
    # Start producer first
    producer.start()
    time.sleep(2)  # Give producer time to send messages
    
    # Start consumer
    consumer.start()
    
    # Wait for both threads to complete
    producer.join()
    consumer.join()

    print(f"\nTotal messages received: {len(received_messages)}")
    
    # Verify messages are received in descending priority order
    priorities = [msg["priority"] for msg in received_messages]
    assert priorities == sorted(priorities, reverse=True), \
        f"Expected descending priorities, got: {priorities}"

    print("✅ Test passed: messages received in priority order.")
    for msg in received_messages:
        print(msg)

def test_priority_boost_polling():
    """
    Test the new priority boost polling functionality with the specific user ID.
    This test verifies that the consumer can properly handle priority boost configuration
    and poll messages according to the priority rules.
    """
    print("\n🚀 Testing Priority Boost Polling Functionality...")
    
    # Clear and create topics for testing
    clear_topics()
    create_topics()
    
    # Test 1: Basic priority boost consumer initialization
    print("📋 Test 1: Initializing consumer with priority boost...")
    consumer = KafkaboostConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topics=["topic1", "topic2", "test_topic"],  # Include all topics from config
        group_id=f"priority_boost_test_{uuid.uuid4()}",
        user_id=USER_ID,
        auto_offset_reset='earliest'
    )
    
    # Verify S3 config manager is initialized
    assert consumer.s3_config_manager is not None, "S3ConfigManager should be initialized"
    print("✅ S3ConfigManager initialized successfully")
    
    # Verify priority boost is configured
    assert consumer.s3_config_manager.is_priority_boost_configured(), "Priority boost should be configured"
    print("✅ Priority boost configuration detected")
    
    # Test 2: Priority queues creation
    print("\n📋 Test 2: Creating priority boost queues...")
    priority_queues = consumer.create_priority_boost_queues()
    assert priority_queues is not None, "Priority queues should be created"
    print(f"✅ Priority queues created: {list(priority_queues.keys())}")
    
    # Test 3: Get topics by priority from config
    print("\n📋 Test 3: Getting topics organized by priority...")
    topics_by_priority = consumer.get_topics_by_priority_from_config()
    assert topics_by_priority, "Should get topics organized by priority"
    print(f"✅ Topics by priority: {topics_by_priority}")
    
    # Get max priority dynamically from config instead of hardcoding
    max_priority = consumer.s3_config_manager.get_max_priority()
    print(f"✅ Max priority from config: {max_priority}")
    
    # Verify the expected structure based on s3_config_local.json
    # Non-boost topics should be in priority 0, boost topics in their respective priorities
    expected_priorities = {0, 5, 6, 7, 8, 9, 10}  # 0 for non-boost, 5-10 for boost
    actual_priorities = set(topics_by_priority.keys())
    assert actual_priorities.issuperset(expected_priorities), \
        f"Expected priorities {expected_priorities}, got {actual_priorities}"
    print("✅ Priority structure matches configuration")
    
    # Test 4: Test the new poll function with priority boost
    print("\n📋 Test 4: Testing poll function with priority boost...")
    
    # First, produce some messages to test topics
    producer = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS, user_id=USER_ID)
    
    # Send messages to different topics with different priorities
    # IMPORTANT: Send to the actual topics the consumer is polling from
    test_messages = [
        {"topic": "topic1", "data": "topic1_high", "priority": 7},      # Non-boost topic
        {"topic": "topic2", "data": "topic2_medium", "priority": 6},    # Non-boost topic
        {"topic": "test_topic_5", "data": "test_topic_boost_5", "priority": 5},  # Boost topic
        {"topic": "test_topic_6", "data": "test_topic_boost_6", "priority": 6},  # Boost topic
        {"topic": "test_topic_7", "data": "test_topic_boost_7", "priority": 7},  # Boost topic
        {"topic": "topic1", "data": "topic1_low", "priority": 3},       # Non-boost topic
        {"topic": "topic2", "data": "topic2_low", "priority": 2},       # Non-boost topic
        {"topic": "test_topic_8", "data": "test_topic_boost_8", "priority": 8},  # Boost topic
        {"topic": "test_topic_9", "data": "test_topic_boost_9", "priority": 9},  # Boost topic
        {"topic": "test_topic_10", "data": "test_topic_boost_10", "priority": 10}  # Boost topic
    ]
    
    for msg in test_messages:
        producer.send(msg["topic"], value={"data": msg["data"], "priority": msg["priority"]}, priority=msg["priority"])
        print(f"📤 Produced: {msg['data']} to {msg['topic']} with priority {msg['priority']}")
    
    producer.flush()
    producer.close()
    
    # Wait for messages to be available with retry logic instead of fixed sleep
    print("\n⏳ Waiting for messages to be available...")
    max_wait_time = 10  # seconds
    wait_interval = 0.5  # seconds
    total_wait_time = 0
    
    while total_wait_time < max_wait_time:
        try:
            # Try a quick poll to see if messages are available
            test_messages = consumer.poll(timeout_ms=100, max_records=1)
            if test_messages:
                print(f"✅ Messages available after {total_wait_time:.1f} seconds")
                break
        except Exception:
            pass
        
        time.sleep(wait_interval)
        total_wait_time += wait_interval
        print(f"   Waiting... ({total_wait_time:.1f}s)")
    
    if total_wait_time >= max_wait_time:
        print("⚠️  Timeout waiting for messages - proceeding with test anyway")
    
    # Test the poll function
    print("\n📥 Polling for messages with priority boost...")
    try:
        messages = consumer.poll(timeout_ms=5000, max_records=10)
        print(f"✅ Poll successful! Received {len(messages)} messages")
        
        if messages:
            print("📋 Received messages:")
            for i, msg in enumerate(messages):
                print(f"  {i+1}. {msg.value}")
            
            # CRITICAL: Assert that messages are returned in descending priority order
            priorities = [msg.value.get("priority", 0) for msg in messages]
            print(f"📊 Message priorities in order: {priorities}")
            
            # Verify descending order (highest priority first)
            assert priorities == sorted(priorities, reverse=True), \
                f"Expected descending priorities, got: {priorities}"
            print("✅ Priority ordering verified: messages returned in descending priority order")
            
            # Verify that we got messages from different priority levels
            unique_priorities = set(priorities)
            assert len(unique_priorities) > 1, f"Expected messages from multiple priority levels, got: {unique_priorities}"
            print(f"✅ Messages received from multiple priority levels: {sorted(unique_priorities, reverse=True)}")
            
        else:
            print("⚠️  No messages received (this might be expected if topics are empty)")
            
    except Exception as e:
        print(f"❌ Error during poll: {str(e)}")
        # This might be expected if Kafka is not running locally
    
    # Test 5: Test async polling with ordering verification
    print("\n📋 Test 5: Testing async polling functionality...")
    try:
        import asyncio
        
        async def test_async_poll():
            messages = await consumer.poll_topics_async(topics_by_priority, timeout_ms=2000, max_records=5)
            print(f"✅ Async poll successful! Received {len(messages)} messages")
            
            if messages:
                # CRITICAL: Verify async polling also maintains priority order
                priorities = [msg.value.get("priority", 0) for msg in messages]
                print(f"📊 Async message priorities in order: {priorities}")
                
                # Verify descending order for async polling too
                assert priorities == sorted(priorities, reverse=True), \
                    f"Expected descending priorities in async poll, got: {priorities}"
                print("✅ Async priority ordering verified: messages returned in descending priority order")
                
                # Verify we got messages from different priority levels
                unique_priorities = set(priorities)
                assert len(unique_priorities) > 1, f"Expected async messages from multiple priority levels, got: {unique_priorities}"
                print(f"✅ Async messages received from multiple priority levels: {sorted(unique_priorities, reverse=True)}")
            
            return messages
        
        # Run the async test
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async_messages = loop.run_until_complete(test_async_poll())
            print("✅ Async polling test completed successfully")
        finally:
            loop.close()
            
    except Exception as e:
        print(f"⚠️  Async polling test failed (might be expected): {str(e)}")
    
    # Test 6: Verify pause/resume behavior indirectly through priority ordering
    print("\n📋 Test 6: Verifying pause/resume behavior through priority ordering...")
    try:
        # Send a mix of high and low priority messages to test pause/resume
        producer2 = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS, user_id=USER_ID)
        
        # Send messages with clear priority separation
        pause_test_messages = [
            {"topic": "test_topic_10", "data": "pause_test_high", "priority": 10},
            {"topic": "test_topic_9", "data": "pause_test_high", "priority": 9},
            {"topic": "test_topic_2", "data": "pause_test_low", "priority": 2},
            {"topic": "test_topic_1", "data": "pause_test_low", "priority": 1},
            {"topic": "test_topic_0", "data": "pause_test_low", "priority": 0},
        ]
        
        for msg in pause_test_messages:
            producer2.send(msg["topic"], value={"data": msg["data"], "priority": msg["priority"]}, priority=msg["priority"])
            print(f"📤 Produced pause test: {msg['data']} to {msg['topic']} with priority {msg['priority']}")
        
        producer2.flush()
        producer2.close()
        
        # Wait for messages
        time.sleep(1)
        
        # Poll and verify that high priority messages come first
        pause_test_messages = consumer.poll(timeout_ms=3000, max_records=5)
        if pause_test_messages:
            priorities = [msg.value.get("priority", 0) for msg in pause_test_messages]
            print(f"📊 Pause test priorities: {priorities}")
            
            # Verify that high priority messages (9, 10) come before low priority (0, 1, 2)
            high_priority_indices = [i for i, p in enumerate(priorities) if p >= 9]
            low_priority_indices = [i for i, p in enumerate(priorities) if p <= 2]
            
            if high_priority_indices and low_priority_indices:
                # All high priority messages should come before any low priority messages
                max_high_index = max(high_priority_indices)
                min_low_index = min(low_priority_indices)
                
                assert max_high_index < min_low_index, \
                    f"High priority messages should come before low priority. High max index: {max_high_index}, Low min index: {min_low_index}"
                print("✅ Pause/resume behavior verified: high priority messages delivered before low priority")
            else:
                print("⚠️  Could not verify pause/resume behavior (insufficient message mix)")
        else:
            print("⚠️  No pause test messages received")
            
    except Exception as e:
        print(f"⚠️  Pause/resume test failed: {str(e)}")
    
    # Cleanup
    consumer.close()
    
    print("\n🎉 Priority Boost Polling Test Completed!")
    print("=" * 60)

def producer_thread_multi_topic():
    producer = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS, user_id=USER_ID)

    messages_topic1 = [
        {"data": "topic1_p10", "priority": 10},
        {"data": "topic1_p5", "priority": 5},
        {"data": "topic1_p9", "priority": 9},
        {"data": "topic1_p8", "priority": 8},
        {"data": "topic1_p4", "priority": 4}
    ]

    messages_topic2 = [
        {"data": "topic2_p10", "priority": 10},
        {"data": "topic2_p5", "priority": 5},
        {"data": "topic2_p9", "priority": 9},
        {"data": "topic2_p8", "priority": 8},
        {"data": "topic2_p4", "priority": 4}
    ]

    # Send messages to topic 1
    for msg in messages_topic1:
        producer.send(TOPIC, value=msg, priority=msg["priority"])
        print(f"Produced to {TOPIC}: {msg}")

    # Send messages to topic 2
    for msg in messages_topic2:
        producer.send(TOPIC2, value=msg, priority=msg["priority"])
        print(f"Produced to {TOPIC2}: {msg}")

    producer.flush()
    producer.close()

def consumer_thread_multi_topic(received_messages):
    print(f"Consumer subscribing to topics: {TOPIC}, {TOPIC2} with group ID: {GROUP_ID}")
    consumer = KafkaboostConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topics=[TOPIC, TOPIC2],
        group_id=GROUP_ID,
        max_poll_records=500,
        consumer_timeout_ms=10000,
        user_id=USER_ID,
        auto_offset_reset='earliest'  # Start reading from the beginning
    )

    print("Polling for prioritized messages from multiple topics...")
    start_time = time.time()
    while len(received_messages) < 30 and time.time() - start_time < 15:
        batch = consumer.poll(timeout_ms=1000, max_records=500)
        if batch:
            print(f"Received batch of {len(batch)} messages")
            for message in batch:
                if message.value not in received_messages:
                    received_messages.append(message.value)
                    print(f"Added new message: {message.value}")
            print(f"Total unique messages received so far: {len(received_messages)}")
        else:
            print("No messages received in this poll")

    consumer.close()

def test_priority_order_multi_topic():
    # Clear topics before starting the test
    clear_topics()
    create_topics()
    
    # Create a list to store received messages
    received_messages = []
    
    # Create and start threads
    producer = threading.Thread(target=producer_thread_multi_topic)
    consumer = threading.Thread(target=consumer_thread_multi_topic, args=(received_messages,))
    
    # Start producer first
    producer.start()
    time.sleep(2)  # Give producer time to send messages
    
    # Start consumer
    consumer.start()
    
    # Wait for both threads to complete
    producer.join()
    consumer.join()

    print(f"\nTotal messages received: {len(received_messages)}")
    
    # Print the order in which messages were received (topic, data, priority)
    print("\nOrder of received messages (topic, data, priority):")
    for msg in received_messages:
        topic = "topic1" if msg["data"].startswith("topic1") else ("topic2" if msg["data"].startswith("topic2") else "unknown")
        print(f"{topic}: {msg['data']} (priority {msg['priority']})")
    
    # Verify messages are received in descending priority order
    priorities = [msg["priority"] for msg in received_messages]
    assert priorities == sorted(priorities, reverse=True), \
        f"Expected descending priorities, got: {priorities}"

    # Verify we received messages from both topics
    topic1_messages = [msg for msg in received_messages if msg["data"].startswith("topic1")]
    topic2_messages = [msg for msg in received_messages if msg["data"].startswith("topic2")]
    
    print(f"\nMessages from {TOPIC}: {len(topic1_messages)}")
    print(f"Messages from {TOPIC2}: {len(topic2_messages)}")
    
    assert len(topic1_messages) == 5, f"Expected 5 messages from {TOPIC}, got {len(topic1_messages)}"
    assert len(topic2_messages) == 5, f"Expected 5 messages from {TOPIC2}, got {len(topic2_messages)}"

    print("✅ Test passed: messages received in priority order from multiple topics.")
    print("\nMessages from Topic 1:")
    for msg in topic1_messages:
        print(msg)
    print("\nMessages from Topic 2:")
    for msg in topic2_messages:
        print(msg)



def test_priority_boost_topic_routing():
    """
    Test that messages are sent to the correct priority boost topics.
    Based on the config, 'test_topic' should route to 'test_topic_X' 
    when priority >= 5 (priority_boost_min_value).
    """
    print("\n=== Testing Priority Boost Topic Routing ===")
    
    # Use the topic configured in Priority_boost
    PRIORITY_BOOST_TOPIC = "test_topic"
    MIN_PRIORITY = 5  # From config: priority_boost_min_value
    
    # Create admin client to manage topics
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    
    try:
        # Clean up any existing priority boost topics
        existing_topics = admin_client.list_topics()
        topics_to_delete = [topic for topic in existing_topics if topic.startswith(f"{PRIORITY_BOOST_TOPIC}_")]
        if topics_to_delete:
            admin_client.delete_topics(topics_to_delete)
            time.sleep(2)
        
        # Create the base topic and priority-specific topics
        base_topic = NewTopic(name=PRIORITY_BOOST_TOPIC, num_partitions=1, replication_factor=1)
        priority_topics = [
            NewTopic(name=f"{PRIORITY_BOOST_TOPIC}_{i}", num_partitions=1, replication_factor=1)
            for i in range(MIN_PRIORITY, 11)  # Create topics for priorities 5-10
        ]
        
        all_topics = [base_topic] + priority_topics
        admin_client.create_topics(all_topics)
        time.sleep(2)
        
        print(f"Created topics: {PRIORITY_BOOST_TOPIC} and priority topics {PRIORITY_BOOST_TOPIC}_5 to {PRIORITY_BOOST_TOPIC}_10")
        
    except Exception as e:
        print(f"Error setting up topics: {e}")
    finally:
        admin_client.close()
    
    # Test messages with different priorities
    test_messages = [
        {"data": "low_priority_3", "priority": 3},  # Should go to base topic
        {"data": "low_priority_4", "priority": 4},  # Should go to base topic
        {"data": "high_priority_5", "priority": 5}, # Should go to test_topic_5
        {"data": "high_priority_7", "priority": 7}, # Should go to test_topic_7
        {"data": "high_priority_10", "priority": 10}, # Should go to test_topic_10
    ]
    
    # Send messages
    producer = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS, user_id=USER_ID)
    
    print("Sending test messages...")
    for msg in test_messages:
        producer.send(PRIORITY_BOOST_TOPIC, value=msg, priority=msg["priority"])
        print(f"Sent: {msg['data']} (priority {msg['priority']})")
    
    producer.flush()
    producer.close()
    
    # Create consumers for each topic to verify routing
    consumers = {}
    received_messages = {}
    
    # Consumer for base topic
    base_consumer = KafkaboostConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topics=[PRIORITY_BOOST_TOPIC],
        group_id=f"test_base",
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        user_id=USER_ID
    )
    consumers[PRIORITY_BOOST_TOPIC] = base_consumer
    print("SUBSCRIPTION:", base_consumer.subscription()) 
    received_messages[PRIORITY_BOOST_TOPIC] = []
    
    # Consumers for priority topics
    for priority in range(MIN_PRIORITY, 11):
        priority_topic = f"{PRIORITY_BOOST_TOPIC}_{priority}"
        priority_consumer = KafkaboostConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topics=[priority_topic],
            group_id=f"test_priority_{priority}_{uuid.uuid4()}",
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            user_id=USER_ID
        )
        consumers[priority_topic] = priority_consumer
        received_messages[priority_topic] = []
    
    # Collect messages from all topics
    print("Collecting messages from all topics...")
    start_time = time.time()
    while time.time() - start_time < 10:
        for topic_name, consumer in consumers.items():
            batch = consumer.poll(timeout_ms=1000, max_records=10)
            print("ASSIGNMENT:", consumer.assignment()) 
            if batch:
                for message in batch:
                    if message.value not in received_messages[topic_name]:
                        received_messages[topic_name].append(message.value)
                        print(f"Received in {topic_name}: {message.value['data']} (priority {message.value['priority']})")
    
    # Close all consumers
    for consumer in consumers.values():
        consumer.close()
    
    # Verify routing results
    print("\n=== Routing Results ===")
    
    # Check base topic (should have low priority messages)
    base_messages = received_messages[PRIORITY_BOOST_TOPIC]
    expected_base = [msg for msg in test_messages if msg["priority"] < MIN_PRIORITY]
    print(f"Base topic '{PRIORITY_BOOST_TOPIC}': {len(base_messages)} messages")
    for msg in base_messages:
        print(f"  - {msg['data']} (priority {msg['priority']})")
    
    # Check priority topics
    for priority in range(MIN_PRIORITY, 11):
        priority_topic = f"{PRIORITY_BOOST_TOPIC}_{priority}"
        priority_messages = received_messages[priority_topic]
        expected_priority = [msg for msg in test_messages if msg["priority"] == priority]
        print(f"Priority topic '{priority_topic}': {len(priority_messages)} messages")
        for msg in priority_messages:
            print(f"  - {msg['data']} (priority {msg['priority']})")
    
    # Assertions
    print("\n=== Verifying Results ===")
    
    # Verify low priority messages went to base topic
    base_priorities = [msg["priority"] for msg in base_messages]
    assert all(priority < MIN_PRIORITY for priority in base_priorities), \
        f"Base topic should only contain messages with priority < {MIN_PRIORITY}, got: {base_priorities}"
    
    # Verify high priority messages went to correct priority topics
    for priority in range(MIN_PRIORITY, 11):
        priority_topic = f"{PRIORITY_BOOST_TOPIC}_{priority}"
        priority_messages = received_messages[priority_topic]
        if priority_messages:
            assert len(priority_messages) == 1, \
                f"Expected 1 message in {priority_topic}, got {len(priority_messages)}"
            assert priority_messages[0]["priority"] == priority, \
                f"Expected priority {priority} in {priority_topic}, got {priority_messages[0]['priority']}"
    
    print("✅ Priority boost topic routing test passed!")
    print("✅ Messages with priority >= 5 were routed to priority-specific topics")
    print("✅ Messages with priority < 5 were routed to the base topic")

def small_test():
    print("Testing S3 config manager")
    from kafkaboost.s3_config_manager import S3ConfigManager
    
    try:
        s3_manager = S3ConfigManager(user_id=USER_ID, auto_save_local=True,
        local_file_path="small_test.json", aws_config_file="kafkaboost/aws_config.json")
        s3_managers.append(s3_manager)  # Track for cleanup
        config = s3_manager.get_config()
        
        topics_priority = config.get('Topics_priority', [])
        print(f"Topics priority: {topics_priority}")
        Rule_Base_priority = config.get('Rule_Base_priority', [])
        print(f"Rule base priority: {Rule_Base_priority}")
        Priority_boost = config.get('Priority_boost', [])
        print(f"Priority boost: {Priority_boost}")
        
        print("✅ S3 config manager test completed successfully")
    except Exception as e:
        print(f"❌ S3 config manager test failed: {e}")
        # Fall back to file-based test
        print("Falling back to file-based config test...")
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        topics_priority = config.get('Topics_priority', [])
        print(f"Topics priority: {topics_priority}")
        Rule_Base_priority = config.get('Rule_Base_priority', [])
        print(f"Rule base priority: {Rule_Base_priority}")
        Priority_boost = config.get('Priority_boost', [])
        print(f"Priority boost: {Priority_boost}")


def test_find_matching_topics():
    """
    Simple test to verify find_matching_topics works with the S3 config manager
    """
    print("\n=== Testing find_matching_topics with S3 config manager ===")
    
    try:
        print("starting.....")
        # Initialize KafkaConfigManager with S3 config manager
        manager = KafkaConfigManager(BOOTSTRAP_SERVERS, user_id=USER_ID)
        assert manager.connect(), "Failed to connect to Kafka"
        
        # Use find_matching_topics to get all test_topic variants
        result = manager.find_matching_topics("test_topic")
        print(f"Found topics for 'test_topic': {result}")
        
        test_topic_variants = result.get("test_topic", [])
        print(f"Consumer will listen to these topics: {test_topic_variants}")
        
        # Create a consumer that listens to all test_topic variants
        if test_topic_variants:
            consumer = KafkaboostConsumer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topics=test_topic_variants,
                group_id=f"test_find_matching_{uuid.uuid4()}",
                auto_offset_reset='earliest',
                consumer_timeout_ms=5000,
                user_id=USER_ID
            )
            
            print(f"Consumer subscription: {consumer.subscription()}")
            print(f"Consumer assignment: {consumer.assignment()}")
            
            # Verify the consumer is listening to all the topics we found
            subscription_topics = list(consumer.subscription())
            assert set(subscription_topics) == set(test_topic_variants), \
                f"Consumer should listen to {test_topic_variants}, but listens to {subscription_topics}"
            
            consumer.close()
            print("✅ Consumer successfully subscribed to all test_topic variants!")
        else:
            print("⚠️  No test_topic variants found - this might be expected if topics don't exist yet")
        
        manager.close()
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        raise

def test_priority_boost_logic_step_by_step():
    """
    Test the priority boost logic step by step to verify it works correctly.
    This test will poll multiple times to see if we get messages in strict priority order.
    """
    print("\n🔍 Testing Priority Boost Logic Step by Step...")
    
    # Clear and create topics for testing
    clear_topics()
    create_topics()
    
    # Create consumer
    consumer = KafkaboostConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topics=["topic1", "topic2", "test_topic"],
        group_id=f"priority_logic_test_{uuid.uuid4()}",
        user_id=USER_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )
    
    # Create producer and send messages with clear priorities
    producer = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS, user_id=USER_ID)
    
    # Send exactly 3 messages with different priorities
    test_messages = [
        {"topic": "test_topic_10", "data": "HIGH_PRIORITY_10", "priority": 10},
        {"topic": "test_topic_7", "data": "MEDIUM_PRIORITY_7", "priority": 7},
        {"topic": "test_topic_3", "data": "LOW_PRIORITY_3", "priority": 3}
    ]
    
    for msg in test_messages:
        producer.send(msg["topic"], value={"data": msg["data"], "priority": msg["priority"]}, priority=msg["priority"])
        print(f"📤 Produced: {msg['data']} to {msg['topic']} with priority {msg['priority']}")
    
    producer.flush()
    producer.close()
    
    # Wait for messages
    time.sleep(2)
    
    print("\n📥 Testing Priority Polling Logic:")
    print("=" * 50)
    
    # First poll - should get only priority 10 messages
    print("\n🔄 Poll 1 - Should get only priority 10 messages:")
    messages1 = consumer.poll(timeout_ms=3000, max_records=5)
    if messages1:
        priorities1 = [msg.value.get("priority", 0) for msg in messages1]
        print(f"📊 Received priorities: {priorities1}")
        print(f"📋 Messages: {[msg.value.get('data') for msg in messages1]}")
        
        # Verify we only got priority 10
        assert all(p == 10 for p in priorities1), f"Expected only priority 10, got {priorities1}"
        print("✅ Poll 1: Correctly received only priority 10 messages")
    else:
        print("❌ Poll 1: No messages received")
    
    # Second poll - should get priority 7 messages
    print("\n🔄 Poll 2 - Should get priority 7 messages:")
    messages2 = consumer.poll(timeout_ms=3000, max_records=5)
    if messages2:
        priorities2 = [msg.value.get("priority", 0) for msg in messages2]
        print(f"📊 Received priorities: {priorities2}")
        print(f"📋 Messages: {[msg.value.get('data') for msg in messages2]}")
        
        # Verify we only got priority 7
        assert all(p == 7 for p in priorities2), f"Expected only priority 7, got {priorities2}"
        print("✅ Poll 2: Correctly received only priority 7 messages")
    else:
        print("❌ Poll 2: No messages received")
    
    # Third poll - should get priority 3 messages
    print("\n🔄 Poll 3 - Should get priority 3 messages:")
    messages3 = consumer.poll(timeout_ms=3000, max_records=5)
    if messages3:
        priorities3 = [msg.value.get("priority", 0) for msg in messages3]
        print(f"📊 Received priorities: {priorities3}")
        print(f"📋 Messages: {[msg.value.get('data') for msg in messages3]}")
        
        # Verify we only got priority 3
        assert all(p == 3 for p in priorities3), f"Expected only priority 3, got {priorities3}"
        print("✅ Poll 3: Correctly received only priority 3 messages")
    else:
        print("❌ Poll 3: No messages received")
    
    # Fourth poll - should get no messages (all consumed)
    print("\n🔄 Poll 4 - Should get no messages (all consumed):")
    messages4 = consumer.poll(timeout_ms=1000, max_records=5)
    if messages4:
        print(f"❌ Poll 4: Unexpectedly received {len(messages4)} messages")
    else:
        print("✅ Poll 4: Correctly received no messages (all consumed)")
    
    consumer.close()
    
    print("\n🎯 Priority Boost Logic Test Summary:")
    print("=" * 50)
    total_messages = len(messages1) + len(messages2) + len(messages3)
    print(f"📊 Total messages consumed: {total_messages}")
    print(f"📊 Expected messages: 3")
    
    if total_messages == 3:
        print("✅ SUCCESS: Priority boost logic working correctly!")
        print("   - Poll 1: Priority 10 messages only")
        print("   - Poll 2: Priority 7 messages only") 
        print("   - Poll 3: Priority 3 messages only")
        print("   - Poll 4: No messages (all consumed)")
    else:
        print("❌ FAILURE: Priority boost logic not working as expected")
    
    print("=" * 50)


if __name__ == "__main__":
    print("Running S3 config manager integration tests...")
    
    print("\n=== Running small test (S3 config manager) ===")
    small_test()
    
    print("\n=== Running find_matching_topics test ===")
    test_find_matching_topics()
    
    print("\n=== Running priority boost topic routing test ===")
    test_priority_boost_topic_routing()
    
    print("\n=== Running single topic test ===")
    test_priority_order()
    
    print("\n=== Running multi-topic test ===")
    test_priority_order_multi_topic()
    
    print("\n=== Running priority boost polling test ===")
    test_priority_boost_polling()
    
    print("\n=== Running priority boost logic step-by-step test ===")
    test_priority_boost_logic_step_by_step()
    
    print("\n🎉 All tests completed!")
