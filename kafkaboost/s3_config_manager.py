import os
import json
import time
import threading
import boto3
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, NoCredentialsError


class S3ConfigManager:
    """
    S3-based configuration manager that extends the base ConfigManager
    to fetch configuration from AWS S3 bucket.
    """
    AWSConfig = "kafkaboost/aws_config.json"
    def __init__(self, user_id: Optional[str] = None, auto_save_local: bool = True, local_file_path: str = "s3_config_local.json", 
                 poll_interval: int = 5000, enable_background_polling: bool = True, on_config_change: Optional[Callable] = None,
                 aws_config_file: Optional[str] = None):
        # Load AWS credentials from config file first
        if aws_config_file:
            self._load_aws_credentials_from_file(aws_config_file)
        else:
            self._load_aws_credentials_from_file(self.AWSConfig)
        
        # Initialize S3-specific attributes after loading credentials
        self._s3_client = None
        self._bucket_name = os.getenv('S3_CONFIG_BUCKET')
        self._aws_region = os.getenv('AWS_REGION', 'us-east-1')
        
        # User-specific configuration
        self._user_id = user_id
        if user_id:
            # For user-specific config, we'll find the most recent file dynamically
            self._config_key = None  # Will be set when fetching
        else:
            # Use default config key
            self._config_key = os.getenv('S3_CONFIG_KEY', 'config.json')
        
        # Initialize config attributes
        self._config: Dict[str, Any] = {}
        self._last_update: Optional[datetime] = None
        self._update_interval = timedelta(minutes=5)
        
        # Local file saving options
        self._auto_save_local = auto_save_local
        self._local_file_path = local_file_path
        
        # Background polling configuration
        self._poll_interval = poll_interval  # seconds
        self._enable_background_polling = enable_background_polling
        self._on_config_change = on_config_change
        self._polling_thread = None
        self._stop_polling = threading.Event()
        self._config_hash = None  # To detect changes
        
        # Initialize S3 client
        self._initialize_s3_client()
        
        # Start background polling if enabled
        if self._enable_background_polling and user_id:
            self._start_background_polling()
    
    def _load_aws_credentials_from_file(self, aws_config_file: str):
        """Load AWS credentials from the AWS config file."""
        try:
            from kafkaboost.aws_config import AWSConfig
            aws_config = AWSConfig.from_file(aws_config_file)
            print(f"✅ AWS credentials loaded from {aws_config_file}")
        except Exception as e:
            print(f"Warning: Could not load AWS credentials from {aws_config_file}: {str(e)}")
        
    def _initialize_s3_client(self):
        """Initialize the S3 client with credentials."""
        try:
            # Try to use AWS credentials from environment variables or IAM role
            self._s3_client = boto3.client(
                's3',
                region_name=self._aws_region
            )
            print(f"S3 client initialized for region: {self._aws_region}")
        except NoCredentialsError:
            print("Warning: No AWS credentials found. Please configure AWS credentials.")
            raise RuntimeError("AWS credentials not configured")
    
    def _fetch_config(self) -> None:
        """Fetch the configuration from S3 bucket."""
        if not self._bucket_name:
            print("Warning: S3_CONFIG_BUCKET not set, using default config")
            self._config = {}
            self._last_update = datetime.now()
            # Auto-save even when using default config
            if self._auto_save_local:
                self.save_config_to_file(self._local_file_path)
            return
        
        # For user-specific config, find the most recent file
        if self._user_id and not self._config_key:
            self._config_key = self._find_latest_user_file()
            if not self._config_key:
                print(f"Warning: No files found for user {self._user_id}, using default config")
                self._config = {}
                self._last_update = datetime.now()
                if self._auto_save_local:
                    self.save_config_to_file(self._local_file_path)
                return
            
        try:
            print(f"Fetching config from S3: s3://{self._bucket_name}/{self._config_key}")
            
            response = self._s3_client.get_object(
                Bucket=self._bucket_name,
                Key=self._config_key
            )
            
            config_data = response['Body'].read().decode('utf-8')
            self._config = json.loads(config_data)
            self._last_update = datetime.now()
            print(f"Config updated successfully from S3 at {self._last_update}")
            
            # Auto-save to local file if enabled
            if self._auto_save_local:
                self.save_config_to_file(self._local_file_path)
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"Error: S3 bucket '{self._bucket_name}' does not exist")
            elif error_code == 'NoSuchKey':
                print(f"Error: Config file '{self._config_key}' not found in bucket")
            else:
                print(f"Error fetching config from S3: {str(e)}")
            
            # Fall back to default config if S3 fails
            print("Falling back to default config...")
            self._config = {}
            self._last_update = datetime.now()
            # Auto-save even when falling back to default config
            if self._auto_save_local:
                self.save_config_to_file(self._local_file_path)
            
        except Exception as e:
            print(f"Unexpected error fetching config from S3: {str(e)}")
            # Fall back to default config
            self._config = {}
            self._last_update = datetime.now()
            # Auto-save even when falling back to default config
            if self._auto_save_local:
                self.save_config_to_file(self._local_file_path)
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get the current configuration, fetching from S3 if necessary.
        
        Returns:
            The current configuration dictionary
        """
        if (self._last_update is None or 
            datetime.now() - self._last_update >= self._update_interval):
            self._fetch_config()
        return self._config.copy()
    
    def save_config_to_file(self, file_path: str = "retrieved_s3_config.json") -> bool:
        """
        Save the current configuration to a local file.
        
        Args:
            file_path: Path where to save the configuration file
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            with open(file_path, 'w') as f:
                json.dump(self._config, f, indent=2)
            print(f"Configuration saved to: {file_path}")
            return True
        except Exception as e:
            print(f"Error saving configuration to file: {str(e)}")
            return False
    
    def download_config_file(self, local_path: str = "downloaded_s3_config.json") -> bool:
        """
        Download the configuration file directly from S3 to a local file.
        
        Args:
            local_path: Local path where to save the downloaded file
            
        Returns:
            True if downloaded successfully, False otherwise
        """
        if not self._bucket_name:
            print("Error: S3_CONFIG_BUCKET not set")
            return False
            
        try:
            print(f"Downloading config file from S3: s3://{self._bucket_name}/{self._config_key}")
            
            response = self._s3_client.get_object(
                Bucket=self._bucket_name,
                Key=self._config_key
            )
            
            # Save the raw file content
            with open(local_path, 'wb') as f:
                f.write(response['Body'].read())
            
            print(f"Config file downloaded to: {local_path}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"Error: S3 bucket '{self._bucket_name}' does not exist")
            elif error_code == 'NoSuchKey':
                print(f"Error: Config file '{self._config_key}' not found in bucket")
            else:
                print(f"Error downloading config from S3: {str(e)}")
            return False
            
        except Exception as e:
            print(f"Unexpected error downloading config from S3: {str(e)}")
            return False
    
    def upload_config(self, config_data: Dict[str, Any]) -> bool:
        """
        Upload configuration to S3 bucket.
        
        Args:
            config_data: Configuration dictionary to upload
            
        Returns:
            True if upload successful, False otherwise
        """
        if not self._bucket_name:
            print("Error: S3_CONFIG_BUCKET not set")
            return False
            
        try:
            config_json = json.dumps(config_data, indent=2)
            
            self._s3_client.put_object(
                Bucket=self._bucket_name,
                Key=self._config_key,
                Body=config_json,
                ContentType='application/json'
            )
            
            print(f"Config uploaded successfully to S3: s3://{self._bucket_name}/{self._config_key}")
            return True
            
        except Exception as e:
            print(f"Error uploading config to S3: {str(e)}")
            return False
    
    def list_config_versions(self) -> list:
        """
        List all versions of the config file in S3.
        
        Returns:
            List of config file versions
        """
        if not self._bucket_name:
            return []
            
        try:
            response = self._s3_client.list_object_versions(
                Bucket=self._bucket_name,
                Prefix=self._config_key
            )
            
            versions = []
            if 'Versions' in response:
                for version in response['Versions']:
                    versions.append({
                        'version_id': version['VersionId'],
                        'last_modified': version['LastModified'],
                        'size': version['Size']
                    })
            
            return versions
            
        except Exception as e:
            print(f"Error listing config versions: {str(e)}")
            return []
    
    def get_local_file_path(self) -> str:
        """
        Get the path where the configuration is saved locally.
        
        Returns:
            Path to the local configuration file
        """
        return self._local_file_path
    
    def is_auto_save_enabled(self) -> bool:
        """
        Check if auto-save to local file is enabled.
        
        Returns:
            True if auto-save is enabled, False otherwise
        """
        return self._auto_save_local
    
    def _find_latest_user_file(self) -> Optional[str]:
        """
        Find the most recent configuration file for the user.
        
        Returns:
            The S3 key of the most recent file, or None if not found
        """
        if not self._user_id or not self._bucket_name:
            return None
            
        try:
            # List all objects in the bucket
            response = self._s3_client.list_objects_v2(Bucket=self._bucket_name, MaxKeys=1000)
            
            if 'Contents' in response:
                # Find files for this specific user
                user_files = []
                for obj in response['Contents']:
                   #print(f"Checking object: {obj['Key']}")
                    if self._user_id in obj['Key']:
                        user_files.append(obj)
                
                if user_files:
                    # Get the most recent file
                    latest_file = max(user_files, key=lambda x: x['LastModified'])
                    print(f"✓ Using latest file for user {self._user_id}: {latest_file['Key']}")
                    return latest_file['Key']
            
            return None
            
        except Exception as e:
            print(f"Error finding latest user file: {str(e)}")
            return None
    
    def get_user_id(self) -> Optional[str]:
        """
        Get the user ID for this config manager.
        
        Returns:
            The user ID if set, None otherwise
        """
        return self._user_id
    
    def get_s3_key(self) -> str:
        """
        Get the S3 key being used for this config manager.
        
        Returns:
            The S3 key path
        """
        return self._config_key
    
    def _start_background_polling(self):
        """Start background polling thread."""
        if self._polling_thread and self._polling_thread.is_alive():
            return  # Already running
            
        self._stop_polling.clear()
        self._polling_thread = threading.Thread(target=self._polling_worker, daemon=True)
        self._polling_thread.start()
        print(f"✅ Background polling started for user {self._user_id} (every {self._poll_interval} seconds)")
    
    def _stop_background_polling(self):
        """Stop background polling thread."""
        if self._polling_thread and self._polling_thread.is_alive():
            self._stop_polling.set()
            self._polling_thread.join(timeout=5)
            print("✅ Background polling stopped")
    
    def _polling_worker(self):
        """Background worker that polls for config changes."""
        while not self._stop_polling.is_set():
            try:
                # Fetch latest config
                self._fetch_config()
                
                # Check if config has changed
                current_hash = hash(str(self._config))
                if self._config_hash is not None and current_hash != self._config_hash:
                    print(f"🔄 Configuration changed for user {self._user_id}")
                    if self._on_config_change:
                        try:
                            self._on_config_change(self._config.copy())
                        except Exception as e:
                            print(f"Error in config change callback: {str(e)}")
                
                self._config_hash = current_hash
                
                # Wait for next poll interval with shorter checks for shutdown
                for _ in range(self._poll_interval):
                    if self._stop_polling.is_set():
                        break
                    time.sleep(1)
                
            except Exception as e:
                print(f"Error in background polling: {str(e)}")
                # Wait a bit before retrying, but check for shutdown more frequently
                for _ in range(60):
                    if self._stop_polling.is_set():
                        break
                    time.sleep(1)
    
    def is_polling_active(self) -> bool:
        """
        Check if background polling is active.
        
        Returns:
            True if polling is active, False otherwise
        """
        return self._polling_thread is not None and self._polling_thread.is_alive()
    
    def get_poll_interval(self) -> int:
        """
        Get the current polling interval in seconds.
        
        Returns:
            Polling interval in seconds
        """
        return self._poll_interval
    
    def set_poll_interval(self, interval: int):
        """
        Set a new polling interval.
        
        Args:
            interval: New polling interval in seconds
        """
        self._poll_interval = interval
        print(f"Polling interval updated to {interval} seconds")
    
    def force_refresh(self):
        """
        Force an immediate config refresh.
        """
        print("🔄 Forcing immediate config refresh...")
        self._fetch_config()
        print("✅ Config refresh completed")
    
    # Configuration retrieval functions for kafka_utils, consumer, and producer
    
    def get_max_priority(self) -> int:
        """
        Get the maximum priority value from config.
        
        Returns:
            Maximum priority value (default: 10)
        """
        config = self.get_config()
        return config.get('max_priority', 10)
    
    def get_default_priority(self) -> int:
        """
        Get the default priority value from config.
        
        Returns:
            Default priority value (default: 0)
        """
        config = self.get_config()
        return config.get('default_priority', 0)
    
    def get_topics_priority(self) -> list:
        """
        Get the topics priority configuration.
        
        Returns:
            List of topic priority configurations
        """
        config = self.get_config()
        return config.get('Topics_priority', [])
    
    def get_rule_base_priority(self) -> list:
        """
        Get the rule-based priority configuration.
        
        Returns:
            List of rule-based priority configurations
        """
        config = self.get_config()
        return config.get('Rule_Base_priority', [])
    
    def get_priority_boost(self) -> list:
        """
        Get the priority boost configuration.
        
        Returns:
            List of priority boost configurations
        """
        config = self.get_config()
        return config.get('Priority_boost', [])
    
    def get_user_id(self) -> Optional[str]:
        """
        Get the user ID from config.
        
        Returns:
            User ID if present, None otherwise
        """
        config = self.get_config()
        return config.get('user_id')
    
    def get_topic_priority_by_name(self, topic_name: str) -> Optional[int]:
        """
        Get priority for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Priority value for the topic, None if not found
        """
        topics_priority = self.get_topics_priority()
        for topic_config in topics_priority:
            if topic_config.get('topic') == topic_name:
                return topic_config.get('priority')
        return None
    
    def get_role_priority(self, role_name: str) -> Optional[int]:
        """
        Get priority for a specific role.
        
        Args:
            role_name: Name of the role
            
        Returns:
            Priority value for the role, None if not found
        """
        rule_base_priority = self.get_rule_base_priority()
        for role_config in rule_base_priority:
            if role_config.get('role_name') == role_name:
                return role_config.get('priority')
        return None
    
    def get_priority_boost_config(self, topic_name: str) -> Optional[dict]:
        """
        Get priority boost configuration for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Priority boost configuration for the topic, None if not found
        """
        priority_boost = self.get_priority_boost()
        for boost_config in priority_boost:
            if boost_config.get('topic_name') == topic_name:
                return boost_config
        return None
    
    def get_priority_boost_min_value(self, topic_name: str) -> int:
        """
        Get the minimum priority value for boost for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Minimum priority value for boost (default: 0)
        """
        boost_config = self.get_priority_boost_config(topic_name)
        if boost_config:
            return boost_config.get('priority_boost_min_value', 0)
        return 0
    
    def get_full_config_for_kafka_utils(self) -> dict:
        """
        Get the complete configuration needed by kafka_utils.
        
        Returns:
            Complete configuration dictionary
        """
        return self.get_config()
    
    def get_full_config_for_consumer(self) -> dict:
        """
        Get the complete configuration needed by consumer.
        
        Returns:
            Complete configuration dictionary
        """
        return self.get_config()
    
    def get_full_config_for_producer(self) -> dict:
        """
        Get the complete configuration needed by producer.
        
        Returns:
            Complete configuration dictionary
        """
        return self.get_config()
    
    def is_config_valid(self) -> bool:
        """
        Check if the current configuration is valid.
        
        Returns:
            True if config is valid, False otherwise
        """
        try:
            config = self.get_config()
            # Basic validation - check if required fields exist
            required_fields = ['max_priority', 'default_priority']
            for field in required_fields:
                if field not in config:
                    return False
            return True
        except Exception:
            return False
    
    def get_config_summary(self) -> dict:
        """
        Get a summary of the current configuration.
        
        Returns:
            Dictionary with configuration summary
        """
        config = self.get_config()
        return {
            'user_id': config.get('user_id'),
            'max_priority': config.get('max_priority'),
            'default_priority': config.get('default_priority'),
            'topics_count': len(config.get('Topics_priority', [])),
            'rules_count': len(config.get('Rule_Base_priority', [])),
            'boost_configs_count': len(config.get('Priority_boost', [])),
            'last_updated': self._last_update.isoformat() if self._last_update else None,
            'is_valid': self.is_config_valid()
        }

    def __del__(self):
        """Cleanup when object is destroyed."""
        try:
            self._stop_background_polling()
        except:
            pass
    
    def close(self):
        """Explicitly close and cleanup resources."""
        self._stop_background_polling()
