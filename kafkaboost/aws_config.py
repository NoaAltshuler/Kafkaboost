import os
import json
from typing import Dict, Any, Optional

class AWSConfig:
    """
    AWS configuration helper to manage credentials and settings.
    """
    
    def __init__(self, config_data: Optional[Dict[str, Any]] = None):
        """
        Initialize AWS configuration.
        
        Args:
            config_data: AWS configuration dictionary (optional)
        """
        self.config = config_data or {}
        self._setup_environment()
    
    def _setup_environment(self):
        """Set up environment variables for AWS configuration."""
        # Set AWS region
        if 'aws_project_region' in self.config:
            os.environ['AWS_REGION'] = self.config['aws_project_region']
            os.environ['AWS_DEFAULT_REGION'] = self.config['aws_project_region']
        
        # Set S3 bucket for config
        if 'aws_user_files_s3_bucket' in self.config:
            os.environ['S3_CONFIG_BUCKET'] = self.config['aws_user_files_s3_bucket']
        
        # Set S3 bucket region
        if 'aws_user_files_s3_bucket_region' in self.config:
            os.environ['S3_BUCKET_REGION'] = self.config['aws_user_files_s3_bucket_region']
        
        # Set Cognito Identity Pool ID for S3 path construction
        if 'aws_cognito_identity_pool_id' in self.config:
            os.environ['AWS_COGNITO_IDENTITY_POOL_ID'] = self.config['aws_cognito_identity_pool_id']
    
    @classmethod
    def from_file(cls, file_path: str) -> 'AWSConfig':
        """
        Create AWSConfig from a JSON file.
        
        Args:
            file_path: Path to the AWS configuration JSON file
            
        Returns:
            AWSConfig instance
        """
        with open(file_path, 'r') as f:
            config_data = json.load(f)
        return cls(config_data)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'AWSConfig':
        """
        Create AWSConfig from a dictionary.
        
        Args:
            config_dict: AWS configuration dictionary
            
        Returns:
            AWSConfig instance
        """
        return cls(config_dict)
    
    def get_s3_bucket(self) -> Optional[str]:
        """Get the S3 bucket name."""
        return self.config.get('aws_user_files_s3_bucket')
    
    def get_region(self) -> Optional[str]:
        """Get the AWS region."""
        return self.config.get('aws_project_region')
    
    def get_cognito_identity_pool_id(self) -> Optional[str]:
        """Get the Cognito Identity Pool ID."""
        return self.config.get('aws_cognito_identity_pool_id')
    
    def get_user_pools_id(self) -> Optional[str]:
        """Get the Cognito User Pools ID."""
        return self.config.get('aws_user_pools_id')
    
    def get_user_pools_web_client_id(self) -> Optional[str]:
        """Get the Cognito User Pools Web Client ID."""
        return self.config.get('aws_user_pools_web_client_id')
    
    def validate_config(self) -> bool:
        """
        Validate that required AWS configuration is present.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        required_fields = [
            'aws_project_region',
            'aws_user_files_s3_bucket',
            'aws_cognito_identity_pool_id'
        ]
        
        missing_fields = [field for field in required_fields if field not in self.config]
        
        if missing_fields:
            print(f"Missing required AWS configuration fields: {missing_fields}")
            return False
        
        return True
    
    def print_config_summary(self):
        """Print a summary of the AWS configuration."""
        print("AWS Configuration Summary:")
        print(f"  Region: {self.get_region()}")
        print(f"  S3 Bucket: {self.get_s3_bucket()}")
        print(f"  Cognito Identity Pool: {self.get_cognito_identity_pool_id()}")
        print(f"  User Pools ID: {self.get_user_pools_id()}")
        print(f"  User Pools Web Client ID: {self.get_user_pools_web_client_id()}")
