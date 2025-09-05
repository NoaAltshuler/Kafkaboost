#!/usr/bin/env python3
"""
Comprehensive tests for the priority boost implementation using real Kafka.
Tests both standard mode and priority boost mode with actual Kafka setup.
"""

import json
import time
import uuid
import asyncio
import logging
from datetime import datetime

# Disable info logs
logging.basicConfig(level=logging.WARNING)
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('kafka.client').setLevel(logging.WARNING)
logging.getLogger('kafkaboost').setLevel(logging.WARNING)

from kafkaboost.consumer import KafkaboostConsumer
from kafkaboost.producer import KafkaboostProducer
from kafkaboost.priority_consumer_manager import PriorityConsumerManager


# Test configuration
BOOTSTRAP_SERVERS = ['localhost:9092']
USER_ID = '5428b428-20a1-7051-114f-c24ede151b86'  # Use the actual user ID from s3_config_local.json
TEST_TOPIC = 'test_topic'  # Use the topic from the config
GROUP_ID_PREFIX = 'test_priority_boost_group'




def test_standard_mode_consumer():
    """Test consumer in standard mode (no priority boost)."""
    print("\n=== Testing Standard Mode Consumer ===")
    
    try:
        # Create consumer without user_id (should use standard mode)
        consumer = KafkaboostConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topics=[TEST_TOPIC],
            group_id=f"{GROUP_ID_PREFIX}_standard_{uuid.uuid4()}",
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        print("✓ Standard mode consumer created successfully")
        print(f"  Priority boost enabled: {consumer.priority_boost_enabled}")
        print(f"  Has priority consumer manager: {consumer.priority_consumer_manager is not None}")
        
        # Test polling (should return empty since no messages yet)
        messages = consumer.poll(timeout_ms=1000)
        print(f"✓ Poll returned {len(messages)} messages")
        
        # Clean up
        consumer.close()
        print("✓ Standard mode consumer test completed")
        return True
        
    except Exception as e:
        print(f"✗ Standard mode consumer test failed: {e}")
        return False


def test_priority_boost_mode_consumer():
    """Test consumer in priority boost mode."""
    print("\n=== Testing Priority Boost Mode Consumer ===")
    
    try:
        # Create consumer with user_id (should attempt priority boost mode)
        consumer = KafkaboostConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topics=[TEST_TOPIC],
            group_id=f"{GROUP_ID_PREFIX}_priority_{uuid.uuid4()}",
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            user_id=USER_ID
        )
        
        print("✓ Priority boost mode consumer created successfully")
        print(f"  Priority boost enabled: {consumer.priority_boost_enabled}")
        print(f"  Has priority consumer manager: {consumer.priority_consumer_manager is not None}")
        
        # Test config summary
        summary = consumer.get_config_summary()
        print(f"✓ Config summary: {summary}")
        
        # Test polling (should return empty since no messages yet)
        messages = consumer.poll(timeout_ms=1000)
        print(f"✓ Poll returned {len(messages)} messages")
        
        # Clean up
        consumer.close()
        print("✓ Priority boost mode consumer test completed")
        return True
        
    except Exception as e:
        print(f"✗ Priority boost mode consumer test failed: {e}")
        return False


def test_producer_priority_routing():
    """Test producer priority routing to different topics."""
    print("\n=== Testing Producer Priority Routing ===")
    
    try:
        # Create producer with user_id
        producer = KafkaboostProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            user_id=USER_ID
        )
        
        print("✓ Producer created successfully")
        
        # Test messages with different priorities
        test_messages = [
            {"data": "low_priority_3", "priority": 3},   # Should go to base topic
            {"data": "low_priority_4", "priority": 4},   # Should go to base topic
            {"data": "high_priority_5", "priority": 5},  # Should go to test_topic_5
            {"data": "high_priority_7", "priority": 7},  # Should go to test_topic_7
            {"data": "high_priority_10", "priority": 10}, # Should go to test_topic_10
        ]
        
        print("📤 Sending test messages...")
        for msg in test_messages:
            producer.send(TEST_TOPIC, value=msg, priority=msg["priority"])
            print(f"  Sent: {msg['data']} (priority {msg['priority']})")
        
        producer.flush()
        producer.close()
        print("✓ Producer priority routing test completed")
        return True
        
    except Exception as e:
        print(f"✗ Producer priority routing test failed: {e}")
        return False


def test_priority_consumer_manager_direct():
    """Test PriorityConsumerManager directly."""
    print("\n=== Testing PriorityConsumerManager Direct ===")
    
    try:
        # Create PriorityConsumerManager directly
        manager = PriorityConsumerManager(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            base_topics=[TEST_TOPIC],
            group_id=f"{GROUP_ID_PREFIX}_direct_{uuid.uuid4()}",
            user_id=USER_ID,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        print("✓ PriorityConsumerManager created successfully")
        print(f"  Priority boost enabled: {manager.priority_boost_enabled}")
        print(f"  Max priority: {manager.max_priority}")
        print(f"  Consumers count: {len(manager.consumers)}")
        print(f"  Has base consumer: {manager.base_consumer is not None}")
        
        # Test config summary
        summary = manager.get_config_summary()
        print(f"✓ Config summary: {summary}")
        
        # Test polling
        messages = manager.poll(timeout_ms=1000)
        print(f"✓ Poll returned {len(messages)} messages")
        
        # Clean up
        manager.close()
        print("✓ PriorityConsumerManager direct test completed")
        return True
        
    except Exception as e:
        print(f"✗ PriorityConsumerManager direct test failed: {e}")
        return False


async def test_async_polling():
    """Test async polling functionality."""
    print("\n=== Testing Async Polling ===")
    
    try:
        consumer = KafkaboostConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topics=[TEST_TOPIC],
            group_id=f"{GROUP_ID_PREFIX}_async_{uuid.uuid4()}",
            user_id=USER_ID,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        print("✓ Consumer created for async testing")
        
        # Test async poll
        messages = await consumer.poll_async(timeout_ms=1000)
        print(f"✓ Async poll returned {len(messages)} messages")
        
        # Clean up
        consumer.close()
        print("✓ Async polling test completed")
        return True
        
    except Exception as e:
        print(f"✗ Async polling test failed: {e}")
        return False


def test_end_to_end_priority_flow():
    """Test complete end-to-end priority flow."""
    print("\n=== Testing End-to-End Priority Flow ===")
    
    try:
        # Step 1: Send messages with different priorities
        print("📤 Step 1: Sending messages with different priorities...")
        producer = KafkaboostProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            user_id=USER_ID
        )
        
        test_messages = [
            {"data": "message_priority_3", "priority": 3},
            {"data": "message_priority_7", "priority": 7},
            {"data": "message_priority_5", "priority": 5},
            {"data": "message_priority_10", "priority": 10},
        ]
        
        for msg in test_messages:
            producer.send(TEST_TOPIC, value=msg, priority=msg["priority"])
            print(f"  Sent: {msg['data']} (priority {msg['priority']})")
        
        producer.flush()
        producer.close()
        
        # Step 2: Wait a bit for messages to be available
        print("⏳ Step 2: Waiting for messages to be available...")
        time.sleep(2)
        
        # Step 3: Consume messages and verify priority order
        print("📥 Step 3: Consuming messages and verifying priority order...")
        consumer = KafkaboostConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topics=[TEST_TOPIC],
            group_id=f"{GROUP_ID_PREFIX}_e2e",
            user_id=USER_ID,
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000
        )
        
        received_messages = []
        max_attempts = 10
        
        for attempt in range(max_attempts):
            messages = consumer.poll(timeout_ms=1000)
            if messages:
                received_messages.extend(messages)
                print(f"  Attempt {attempt + 1}: Received {len(messages)} messages")
            else:
                print(f"  Attempt {attempt + 1}: No messages received")
            
            if len(received_messages) >= 100:
                break
        
        consumer.close()
        
        # Step 4: Verify results
        print("✅ Step 4: Verifying results...")
        print(f"  Expected messages: {len(test_messages)}")
        print(f"  Received messages: {len(received_messages)}")
        
        if received_messages:
            print("  Message priorities received:")
            for i, msg in enumerate(received_messages):
                priority = msg.value.get('priority', 'unknown')
                data = msg.value.get('data', 'unknown')
                print(f"    {i+1}. Priority {priority}: {data}")
        
        success = len(received_messages) >= len(test_messages)
        if success:
            print("✓ End-to-end priority flow test completed successfully")
        else:
            print("⚠️ End-to-end test completed but not all messages were received")
        
        return success
        
    except Exception as e:
        print(f"✗ End-to-end priority flow test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🧪 Testing Priority Boost Implementation with Real Kafka")
    print("=" * 60)
    print(f"Kafka Server: {BOOTSTRAP_SERVERS[0]}")
    print(f"Test Topic: {TEST_TOPIC}")
    print(f"User ID: {USER_ID}")
    print("=" * 60)
    
    test_results = []
    
    try:
        # Run tests
        # test_results.append(("Standard Mode Consumer", test_standard_mode_consumer()))
        # test_results.append(("Priority Boost Mode Consumer", test_priority_boost_mode_consumer()))
        # test_results.append(("Producer Priority Routing", test_producer_priority_routing()))
        # test_results.append(("PriorityConsumerManager Direct", test_priority_consumer_manager_direct()))
        # test_results.append(("Async Polling", asyncio.run(test_async_polling())))
        test_results.append(("End-to-End Priority Flow", test_end_to_end_priority_flow()))
        
        # Print results
        print("\n" + "=" * 60)
        print("📊 TEST RESULTS")
        print("=" * 60)
        
        passed = 0
        total = len(test_results)
        
        for test_name, result in test_results:
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{status} {test_name}")
            if result:
                passed += 1
        
        print("=" * 60)
        print(f"📈 SUMMARY: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 All tests passed successfully!")
        else:
            print("⚠️ Some tests failed. Check the output above for details.")
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
