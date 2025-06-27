"""
View Update Configuration for Environment-Based Strategy Management

This module provides configuration management for view update strategies,
allowing different behaviors in development vs. production environments.

Part of ENHANCEMENT-029: Hash-Based View Update Management
"""

import os
from typing import Optional


class ViewUpdateConfig:
    """Configuration class for view update behavior"""

    def __init__(self):
        self.environment = os.environ.get('DAGSTER_ENVIRONMENT', 'development')
        self.force_update_in_dev = os.environ.get('FORCE_VIEW_UPDATES_DEV', 'true').lower() == 'true'
        self.hash_tracking_enabled = os.environ.get('VIEW_HASH_TRACKING', 'true').lower() == 'true'
        self.backup_views_before_update = os.environ.get('BACKUP_VIEWS', 'false').lower() == 'true'

    def should_use_hash_detection(self) -> bool:
        """Determine if hash-based detection should be used"""
        if self.environment == 'development' and self.force_update_in_dev:
            return False  # Always update in dev if configured
        return self.hash_tracking_enabled

    def get_update_strategy(self) -> str:
        """Get the update strategy for current environment"""
        if not self.hash_tracking_enabled:
            return 'create_if_not_exists'
        elif self.environment == 'development' and self.force_update_in_dev:
            return 'always_replace'
        else:
            return 'hash_based'


def get_view_update_strategy(environment: Optional[str] = None) -> str:
    """
    Get view update strategy based on environment

    Args:
        environment: Optional environment override

    Returns:
        Update strategy string ('hash_based', 'always_replace', 'create_if_not_exists')
    """
    config = ViewUpdateConfig()

    if environment:
        config.environment = environment

    return config.get_update_strategy()


def get_view_update_config() -> ViewUpdateConfig:
    """
    Get the current view update configuration

    Returns:
        ViewUpdateConfig instance with current settings
    """
    return ViewUpdateConfig()