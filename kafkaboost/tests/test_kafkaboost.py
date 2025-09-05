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
    
    print("\n🎉 All tests completed!")
