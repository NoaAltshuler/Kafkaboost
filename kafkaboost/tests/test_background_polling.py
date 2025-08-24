#!/usr/bin/env python3
"""
Test script to demonstrate background polling functionality of S3ConfigManager
"""

import time
import json
from kafkaboost.aws_config import AWSConfig
from kafkaboost.s3_config_manager import S3ConfigManager

def on_config_change(new_config):
    """Callback function called when configuration changes."""
    print(f"\n🔄 CONFIG CHANGED! New config received:")
    print(f"   User ID: {new_config.get('user_id', 'N/A')}")
    print(f"   Max Priority: {new_config.get('max_priority', 'N/A')}")
    print(f"   Hash Code: {new_config.get('hash_code', 'N/A')}")
    print(f"   Topics: {len(new_config.get('Topics_priority', []))} topics")
    print(f"   Rules: {len(new_config.get('Rule_Base_priority', []))} rules")

def main():
    print("=== Testing S3ConfigManager Background Polling ===")
    
    # Load AWS configuration
    print("\n1. Loading AWS configuration...")
    aws_config = AWSConfig.from_file('aws_config.json')
    print("✅ AWS configuration loaded")
    
    # Create S3ConfigManager with background polling
    print("\n2. Creating S3ConfigManager with background polling...")
    user_id = "5428b428-20a1-7051-114f-c24ede151b86"
    
    s3_manager = S3ConfigManager(
        user_id=user_id,
        auto_save_local=True,
        local_file_path="polling_test_config.json",
        poll_interval=30,  # Poll every 30 seconds for testing (instead of 5 minutes)
        enable_background_polling=True,
        on_config_change=on_config_change
    )
    
    print("✅ S3ConfigManager created with background polling")
    print(f"   User ID: {s3_manager.get_user_id()}")
    print(f"   Polling interval: {s3_manager.get_poll_interval()} seconds")
    print(f"   Polling active: {s3_manager.is_polling_active()}")
    
    # Get initial config
    print("\n3. Getting initial configuration...")
    initial_config = s3_manager.get_config()
    print(f"✅ Initial config retrieved: {len(initial_config)} keys")
    print(f"   Max Priority: {initial_config.get('max_priority', 'N/A')}")
    print(f"   Hash Code: {initial_config.get('hash_code', 'N/A')}")
    
    # Monitor polling for a few minutes
    print(f"\n4. Monitoring background polling for 2 minutes...")
    print("   (Polling every 30 seconds, checking for changes)")
    print("   Press Ctrl+C to stop early")
    
    try:
        for i in range(12):  # Monitor for 2 minutes (12 * 10 seconds)
            time.sleep(10)
            current_config = s3_manager.get_config()
            print(f"   [{i+1}/12] Polling active: {s3_manager.is_polling_active()}, "
                  f"Last update: {s3_manager._last_update.strftime('%H:%M:%S') if s3_manager._last_update else 'Never'}")
            
    except KeyboardInterrupt:
        print("\n⏹️  Stopping monitoring...")
    
    # Test force refresh
    print("\n5. Testing force refresh...")
    s3_manager.force_refresh()
    
    # Test changing poll interval
    print("\n6. Testing poll interval change...")
    s3_manager.set_poll_interval(60)  # Change to 1 minute
    print(f"   New polling interval: {s3_manager.get_poll_interval()} seconds")
    
    # Final status
    print("\n7. Final status:")
    print(f"   Polling active: {s3_manager.is_polling_active()}")
    print(f"   Poll interval: {s3_manager.get_poll_interval()} seconds")
    print(f"   Local file: {s3_manager.get_local_file_path()}")
    
    print("\n✅ Background polling test completed!")
    print("   The S3ConfigManager will continue polling in the background")
    print("   until the object is destroyed or the program exits.")

if __name__ == "__main__":
    main()
