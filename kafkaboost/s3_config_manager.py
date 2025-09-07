import os
import json
import time
import threading
import requests
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta




class S3ConfigManager:
   """
   S3-based configuration manager that extends the base ConfigManager
   to fetch configuration from AWS S3 bucket via Lambda URL.
   """
   LAMBDA_CONFIG_URL = "https://fmr23ddnqhd6p3j3t6nekipqy40dgrrn.lambda-url.us-east-1.on.aws/config"
   def __init__(self, user_id: Optional[str] = None, auto_save_local: bool = True, local_file_path: str = "s3_config_local.json",
                poll_interval: int = 5000, enable_background_polling: bool = True, on_config_change: Optional[Callable] = None):
       # HTTP client configuration for Lambda URL
       print("in init func congif manger")
       self._lambda_url = self.LAMBDA_CONFIG_URL
       self._http_timeout = 30  # HTTP request timeout in seconds
      
       # User-specific configuration
       self._user_id = user_id
      
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


      
       # Start background polling if enabled
       if self._enable_background_polling and user_id:
           self._start_background_polling()
  


  
   def _fetch_config(self) -> None:
       """Fetch the configuration from Lambda URL."""
       if self._user_id:
           self._fetch_config_from_lambda()
       else:
           print("Warning: No user_id provided, using default config")
           self._config = {}
           self._last_update = datetime.now()
           if self._auto_save_local:
               self.save_config_to_file(self._local_file_path)
  
   def _fetch_config_from_lambda(self) -> None:
       """Fetch the configuration from Lambda URL."""
       if not self._user_id:
           print("Warning: No user_id provided, using default config")
           self._config = {}
           self._last_update = datetime.now()
           if self._auto_save_local:
               self.save_config_to_file(self._local_file_path)
           return
      
       try:
           # Construct the Lambda URL with user_id
           url = f"{self._lambda_url}/{self._user_id}"
           print(f"Fetching config from Lambda URL: {url}")
          
           # Make HTTP request to Lambda URL
           response = requests.get(url, timeout=self._http_timeout)
           response.raise_for_status()  # Raise exception for HTTP errors
          
           # Parse the JSON response
           response_data = response.json()
          
           # Handle nested config structure from Lambda
           if 'config' in response_data:
               self._config = response_data['config']
           else:
               self._config = response_data
              
           self._last_update = datetime.now()
           print(f"Config updated successfully from Lambda URL at {self._last_update}")
          
           # Auto-save to local file if enabled
           if self._auto_save_local:
               self.save_config_to_file(self._local_file_path)
          
       except requests.exceptions.RequestException as e:
           print(f"Error fetching config from Lambda URL: {str(e)}")
           # Fall back to default config
           print("Falling back to default config...")
           self._config = {}
           self._last_update = datetime.now()
           if self._auto_save_local:
               self.save_config_to_file(self._local_file_path)
          
       except json.JSONDecodeError as e:
           print(f"Error parsing JSON response from Lambda URL: {str(e)}")
           # Fall back to default config
           print("Falling back to default config...")
           self._config = {}
           self._last_update = datetime.now()
           if self._auto_save_local:
               self.save_config_to_file(self._local_file_path)
          
       except Exception as e:
           print(f"Unexpected error fetching config from Lambda URL: {str(e)}")
           # Fall back to default config
           self._config = {}
           self._last_update = datetime.now()
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
  
  
   def set_lambda_url(self, url: str) -> None:
       """
       Set the Lambda URL for configuration fetching.
      
       Args:
           url: The Lambda URL endpoint
       """
       self._lambda_url = url
       print(f"Lambda URL set to: {url}")
  
  
   def set_http_timeout(self, timeout: int) -> None:
       """
       Set the HTTP request timeout for Lambda URL requests.
      
       Args:
           timeout: Timeout in seconds
       """
       self._http_timeout = timeout
       print(f"HTTP timeout set to: {timeout} seconds")
  
   def get_user_id(self) -> Optional[str]:
       """
       Get the user ID for this config manager.
      
       Returns:
           The user ID if set, None otherwise
       """
       return self._user_id
  
  
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



