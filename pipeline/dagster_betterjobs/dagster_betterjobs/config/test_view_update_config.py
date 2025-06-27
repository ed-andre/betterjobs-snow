"""
Unit tests for View Update Configuration

Tests the environment-based configuration system for view update strategies.

Part of ENHANCEMENT-029: Hash-Based View Update Management
"""

import pytest
from unittest.mock import patch
import os

from .view_update_config import ViewUpdateConfig, get_view_update_strategy, get_view_update_config


class TestViewUpdateConfig:
    """Test ViewUpdateConfig class"""

    @patch.dict(os.environ, {}, clear=True)
    def test_default_configuration(self):
        """Test default configuration values"""
        config = ViewUpdateConfig()

        assert config.environment == 'development'
        assert config.force_update_in_dev is True
        assert config.hash_tracking_enabled is True
        assert config.backup_views_before_update is False

    @patch.dict(os.environ, {
        'DAGSTER_ENVIRONMENT': 'production',
        'FORCE_VIEW_UPDATES_DEV': 'false',
        'VIEW_HASH_TRACKING': 'false',
        'BACKUP_VIEWS': 'true'
    })
    def test_environment_override(self):
        """Test configuration from environment variables"""
        config = ViewUpdateConfig()

        assert config.environment == 'production'
        assert config.force_update_in_dev is False
        assert config.hash_tracking_enabled is False
        assert config.backup_views_before_update is True

    @patch.dict(os.environ, {'DAGSTER_ENVIRONMENT': 'development', 'FORCE_VIEW_UPDATES_DEV': 'true'})
    def test_should_use_hash_detection_dev_force_update(self):
        """Test hash detection disabled when force update in dev"""
        config = ViewUpdateConfig()

        assert config.should_use_hash_detection() is False

    @patch.dict(os.environ, {'DAGSTER_ENVIRONMENT': 'production', 'VIEW_HASH_TRACKING': 'true'})
    def test_should_use_hash_detection_production(self):
        """Test hash detection enabled in production"""
        config = ViewUpdateConfig()

        assert config.should_use_hash_detection() is True

    @patch.dict(os.environ, {'VIEW_HASH_TRACKING': 'false'})
    def test_should_use_hash_detection_disabled(self):
        """Test hash detection disabled when tracking disabled"""
        config = ViewUpdateConfig()

        assert config.should_use_hash_detection() is False


class TestUpdateStrategies:
    """Test update strategy determination"""

    @patch.dict(os.environ, {'VIEW_HASH_TRACKING': 'false'})
    def test_create_if_not_exists_strategy(self):
        """Test create_if_not_exists strategy when hash tracking disabled"""
        config = ViewUpdateConfig()

        assert config.get_update_strategy() == 'create_if_not_exists'

    @patch.dict(os.environ, {
        'DAGSTER_ENVIRONMENT': 'development',
        'FORCE_VIEW_UPDATES_DEV': 'true',
        'VIEW_HASH_TRACKING': 'true'
    })
    def test_always_replace_strategy_dev(self):
        """Test always_replace strategy in development"""
        config = ViewUpdateConfig()

        assert config.get_update_strategy() == 'always_replace'

    @patch.dict(os.environ, {
        'DAGSTER_ENVIRONMENT': 'production',
        'VIEW_HASH_TRACKING': 'true'
    })
    def test_hash_based_strategy_production(self):
        """Test hash_based strategy in production"""
        config = ViewUpdateConfig()

        assert config.get_update_strategy() == 'hash_based'

    @patch.dict(os.environ, {
        'DAGSTER_ENVIRONMENT': 'staging',
        'VIEW_HASH_TRACKING': 'true'
    })
    def test_hash_based_strategy_staging(self):
        """Test hash_based strategy in staging (non-dev environment)"""
        config = ViewUpdateConfig()

        assert config.get_update_strategy() == 'hash_based'


class TestUtilityFunctions:
    """Test utility functions"""

    @patch.dict(os.environ, {'DAGSTER_ENVIRONMENT': 'production'})
    def test_get_view_update_strategy_function(self):
        """Test get_view_update_strategy utility function"""
        strategy = get_view_update_strategy()

        assert strategy == 'hash_based'

    @patch.dict(os.environ, {'DAGSTER_ENVIRONMENT': 'development'})
    def test_get_view_update_strategy_with_override(self):
        """Test get_view_update_strategy with environment override"""
        strategy = get_view_update_strategy(environment='production')

        # Should use production strategy despite dev environment
        assert strategy == 'hash_based'

    def test_get_view_update_config_function(self):
        """Test get_view_update_config utility function"""
        config = get_view_update_config()

        assert isinstance(config, ViewUpdateConfig)


class TestBooleanParsing:
    """Test boolean parsing from environment variables"""

    @patch.dict(os.environ, {'FORCE_VIEW_UPDATES_DEV': 'TRUE'})
    def test_boolean_parsing_uppercase_true(self):
        """Test boolean parsing with uppercase TRUE"""
        config = ViewUpdateConfig()

        assert config.force_update_in_dev is True

    @patch.dict(os.environ, {'FORCE_VIEW_UPDATES_DEV': 'False'})
    def test_boolean_parsing_mixed_case_false(self):
        """Test boolean parsing with mixed case False"""
        config = ViewUpdateConfig()

        assert config.force_update_in_dev is False

    @patch.dict(os.environ, {'VIEW_HASH_TRACKING': '1'})
    def test_boolean_parsing_numeric_string(self):
        """Test boolean parsing with numeric string (should be False)"""
        config = ViewUpdateConfig()

        assert config.hash_tracking_enabled is False

    @patch.dict(os.environ, {'BACKUP_VIEWS': 'yes'})
    def test_boolean_parsing_yes_string(self):
        """Test boolean parsing with 'yes' string (should be False)"""
        config = ViewUpdateConfig()

        assert config.backup_views_before_update is False


class TestComplexScenarios:
    """Test complex configuration scenarios"""

    @patch.dict(os.environ, {
        'DAGSTER_ENVIRONMENT': 'development',
        'FORCE_VIEW_UPDATES_DEV': 'false',
        'VIEW_HASH_TRACKING': 'true'
    })
    def test_dev_with_hash_tracking_no_force(self):
        """Test development with hash tracking but no force update"""
        config = ViewUpdateConfig()

        assert config.should_use_hash_detection() is True
        assert config.get_update_strategy() == 'hash_based'

    @patch.dict(os.environ, {
        'DAGSTER_ENVIRONMENT': 'production',
        'FORCE_VIEW_UPDATES_DEV': 'true',  # Should be ignored in production
        'VIEW_HASH_TRACKING': 'true'
    })
    def test_production_ignores_dev_settings(self):
        """Test that production ignores dev-specific settings"""
        config = ViewUpdateConfig()

        assert config.should_use_hash_detection() is True
        assert config.get_update_strategy() == 'hash_based'

    @patch.dict(os.environ, {}, clear=True)
    def test_all_defaults_workflow(self):
        """Test complete workflow with all default settings"""
        config = ViewUpdateConfig()

        # Defaults: development, force_update=true, hash_tracking=true
        assert config.environment == 'development'
        assert config.force_update_in_dev is True
        assert config.hash_tracking_enabled is True
        assert config.should_use_hash_detection() is False  # Force update overrides
        assert config.get_update_strategy() == 'always_replace'


if __name__ == "__main__":
    pytest.main([__file__])