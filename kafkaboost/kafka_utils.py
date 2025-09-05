from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import json
from typing import Dict, List, Optional, Union
import logging
from .s3_config_manager import S3ConfigManager

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class KafkaConfigManager:
    def __init__(self, bootstrap_servers: str, user_id: Optional[str] = None):
        """
        Initialize the KafkaConfigManager.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            user_id: User ID for S3 config manager (optional)
            use_s3_config: Whether to use S3 config manager (default: True)
        """
        self.bootstrap_servers = bootstrap_servers
        self.user_id = user_id
        self.admin_client = None
        
        # Initialize config manager
        try:
            self.config_manager = S3ConfigManager(user_id=user_id)
            logger.info("✓ KafkaConfigManager initialized with S3ConfigManager")
        except Exception as e:
            logger.warning(f"Failed to initialize S3ConfigManager: {str(e)}")


    def connect(self) -> bool:
        """Establish connection to Kafka admin client."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
            logger.info("Successfully connected to Kafka admin client")
            return True
        except NoBrokersAvailable:
            logger.error(f"Could not connect to Kafka brokers at {self.bootstrap_servers}")
            return False
            
    def get_topic_config(self, topics: Union[str, List[str]]) -> Dict[str, Optional[Dict]]:
        """
        Get configuration for one or more topics.

        Args:
            topics: Single topic name or list of topic names

        Returns:
            Dictionary mapping topic names to their configurations
        """
        if not self.admin_client:
            if not self.connect():
                return {}

        topic_list = [topics] if isinstance(topics, str) else topics

        try:
            config_resources = [
                ConfigResource(ConfigResourceType.TOPIC, topic)
                for topic in topic_list
            ]
            # Get configurations
            configs_response_list = self.admin_client.describe_configs(config_resources)

            result = {}
            for response in configs_response_list:
                for resource in response.resources:
                    error_code, error_message, resource_type, resource_name, config_entries = resource

                    if error_code != 0:
                        logger.error(f"Error from broker for topic '{resource_name}': {error_message}")
                        result[resource_name] = None
                        continue

                    # Unpack tuple-style entries: (config_name, config_value, ...)
                    topic_config = {
                        entry[0]: entry[1]
                        for entry in config_entries
                    }
                    result[resource_name] = topic_config

            # Ensure all topics are present
            for topic in topic_list:
                if topic not in result:
                    result[topic] = None

            return result

        except Exception as e:
            logger.error(f"Error getting topic configs: {str(e)}")
            return {topic: None for topic in topic_list}
            
    def get_all_topic_configs(self) -> Dict[str, Optional[Dict]]:
        """
        Get configurations for all topics in the cluster.
        
        Returns:
            Dictionary mapping all topic names to their configurations
        """
        if not self.admin_client:
            if not self.connect():
                return {}
                
        try:
            # Get list of all topics
            topics = self.admin_client.list_topics()
            return self.get_topic_config(topics)
        except Exception as e:
            logger.error(f"Error getting all topic configs: {str(e)}")
            return {}
            
    def check_and_create_priority_topics(self) -> bool:
        if not self.admin_client:
            if not self.connect():
                return False

        try:
            priority_config = self.config_manager.get_priority_boost()
            if not priority_config:
                logger.warning("No priority configuration found in config")
                return False

            self.max_priority = self.config_manager.get_max_priority()
        except Exception as e:
            logger.error(f"Error accessing config manager: {str(e)}")
            return False

        existing_topics = self._get_existing_topics()

        for topic_entry in priority_config:
            topic_name = topic_entry.get("topic_name")
            min_priority = topic_entry.get("priority_boost_min_value", 0)
            num_partitions = topic_entry.get("number_of_partitions", 1)
            if not topic_name:
                continue

            for i in range(min_priority, self.max_priority + 1):
                full_topic = f"{topic_name}_{i}"
                if full_topic not in existing_topics:
                    try:
                        new_topic = NewTopic(
                            name=full_topic,
                            num_partitions=num_partitions,
                            replication_factor=1
                        )
                        self.admin_client.create_topics([new_topic])
                        logger.info(f"Created topic: {full_topic} with {num_partitions} partitions")
                    except TopicAlreadyExistsError:
                        logger.info(f"Topic {full_topic} already exists")
                    except Exception as e:
                        logger.error(f"Error creating topic {full_topic}: {str(e)}")
                        return False

        return True

    def _get_existing_topics(self) -> List[str]:
        """Get list of existing topics."""
        try:
            return self.admin_client.list_topics()
        except Exception as e:
            logger.error(f"Error listing topics: {str(e)}")
            return []
            
    def find_matching_topics(self, base_topics: Union[str, List[str]]) -> Dict[str, List[str]]:
        """
        Find all topics that match the given base topic names, including priority variants.
        
        Args:
            base_topics: Single base topic name or list of base topic names
            
        Returns:
            Dictionary mapping base topic names to lists of matching topics (including priority variants)
            
        Example:
            Input: "test_topic"
            If Kafka has topics: ["test_topic", "test_topic_7", "test_topic_8", "topic1"]
            Output: {"test_topic": ["test_topic", "test_topic_7", "test_topic_8"]}
        """
        if not self.admin_client:
            if not self.connect():
                return {}
                
        # Normalize input to list
        base_topic_list = [base_topics] if isinstance(base_topics, str) else base_topics
        
        try:
            # Get all existing topics
            all_topics = self._get_existing_topics()
            
            result = {}
            for base_topic in base_topic_list:
                matching_topics = []
                
                # Find exact matches and priority variants
                for topic in all_topics:
                    # Exact match
                    if topic == base_topic:
                        matching_topics.append(topic)
                    # Priority variant match (base_topic_priority_number)
                    elif topic.startswith(f"{base_topic}_"):
                        # Check if the suffix is a number (priority)
                        suffix = topic[len(f"{base_topic}_"):]
                        if suffix.isdigit():
                            matching_topics.append(topic)
                
                result[base_topic] = matching_topics
                
            return result
            
        except Exception as e:
            logger.error(f"Error finding matching topics: {str(e)}")
            return {base_topic: [] for base_topic in base_topic_list}
    
    def get_config_summary(self) -> dict:
        """
        Get a summary of the current configuration.
        
        Returns:
            Dictionary with configuration summary
        """
        if self.config_manager:
            return self.config_manager.get_config_summary()
        else:
            return {
                'config_source': 'none',
                'max_priority': 10,
                'default_priority': 0,
                'topics_count': 0,
                'rules_count': 0,
                'boost_configs_count': 0
            }
            
    def close(self):
        """Close the admin client connection."""
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None 