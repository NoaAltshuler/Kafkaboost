#!/usr/bin/env python3
"""
Simple test to demonstrate S3ConfigManager functionality
"""

from kafkaboost.aws_config import AWSConfig
from kafkaboost.s3_config_manager import S3ConfigManager

def main():
    print("=== Simple S3ConfigManager Test ===")
    
    # Step 1: Load AWS configuration to set environment variables
    print("\n1. Loading AWS configuration...")
    try:
        aws_config = AWSConfig.from_file('aws_config.json')
        print("✅ AWS configuration loaded successfully")
        print(f"   S3 Bucket: {aws_config.get_s3_bucket()}")
        print(f"   Region: {aws_config.get_region()}")
    except Exception as e:
        print(f"❌ Failed to load AWS configuration: {str(e)}")
        return
    
    # Test 2: Create S3ConfigManager with user ID
    print(f"\n2. Creating S3ConfigManager with user ID...")
    user_id = "5428b428-20a1-7051-114f-c24ede151b86"
    
    s3_manager = S3ConfigManager(
        user_id=user_id,
        auto_save_local=True,
        local_file_path="test_user_config.json"
    )
    
    print(f"✅ S3ConfigManager created successfully!")
    print(f"   User ID: {s3_manager.get_user_id()}")
    print(f"   S3 Key: {s3_manager.get_s3_key()}")
    print(f"   Local file: {s3_manager.get_local_file_path()}")
    
    # Test 3: Try to get config (this will attempt to fetch from S3)
    print(f"\n3. Attempting to get config from S3...")
    try:
        config = s3_manager.get_config()
        print(f"✅ Config retrieved: {len(config)} keys")
        print(f"   Config content: {config}")
        
        # Check if local file was created
        import os
        if os.path.exists(s3_manager.get_local_file_path()):
            print(f"✅ Local file created: {s3_manager.get_local_file_path()}")
            
            # Read and show the file content
            with open(s3_manager.get_local_file_path(), 'r') as f:
                content = f.read()
            print(f"📄 File content: {content}")
        else:
            print(f"❌ Local file not created")
            
    except Exception as e:
        print(f"❌ Error getting config: {str(e)}")
    
    # Test 4: Test without user ID
    print(f"\n4. Testing without user ID...")
    s3_manager_default = S3ConfigManager(
        auto_save_local=True,
        local_file_path="test_default_config.json"
    )
    
    print(f"✅ Default S3ConfigManager created!")
    print(f"   User ID: {s3_manager_default.get_user_id()}")
    print(f"   S3 Key: {s3_manager_default.get_s3_key()}")
    
    try:
        config_default = s3_manager_default.get_config()
        print(f"✅ Default config retrieved: {len(config_default)} keys")
    except Exception as e:
        print(f"❌ Error getting default config: {str(e)}")

if __name__ == "__main__":
    main()
