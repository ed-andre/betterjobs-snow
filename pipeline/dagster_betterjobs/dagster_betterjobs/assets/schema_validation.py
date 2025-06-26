"""
Schema Drift Detection Asset

Validates all database views for schema drift issues and provides detailed monitoring.
This replaces the sensor-based approach with a more appropriate asset + schedule pattern.
"""

from dagster import asset, AssetExecutionContext, AssetMaterialization
from dagster_betterjobs.resources import SnowflakeResource
from typing import Dict, List, Any
from datetime import datetime
from dagster_betterjobs.assets.snowflake_setup import views_setup


@asset(
    name="schema_drift_validation",
    description="Validate all database views for schema drift and development errors",
    group_name="0_infrastructure_setup",
    deps=[views_setup],
    kinds={"snowflake", "monitoring", "data_quality"}
)
def schema_drift_validation(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Perform comprehensive schema drift validation on all database views.

    Detection Strategy:
    1. Discover all views in RAW, STAGE, and ANALYTICS schemas
    2. Execute validation query (SELECT * LIMIT 1) for each view
    3. Categorize any schema-related errors
    4. Generate detailed monitoring metrics and alerts
    5. Store validation results for historical tracking

    Returns:
        Dict with validation results, metrics, and any detected issues
    """

    # Discover all database views
    views = get_all_database_views(snowflake, context)
    context.log.info(f"Starting validation of {len(views)} database views")

    # Validate each view and collect issues
    validation_results = validate_database_views(views, snowflake, context)

    # Process results and generate metrics
    results_summary = process_validation_results(validation_results, context)

    # Log detailed materialization with metadata
    context.log_event(
        AssetMaterialization(
            asset_key="schema_drift_validation",
            metadata={
                "total_views_validated": len(views),
                "validation_timestamp": datetime.now().isoformat(),
                "drift_issues_detected": results_summary["total_issues"],
                "issues_by_type": results_summary["issues_by_type"],
                "issues_by_schema": results_summary["issues_by_schema"],
                "success_rate_percent": results_summary["success_rate"],
                "failed_views": results_summary["failed_views"][:10],  # Limit for metadata
                "execution_duration_seconds": results_summary["execution_duration"]
            }
        )
    )

    # Alert if issues detected
    if results_summary["total_issues"] > 0:
        alert_message = format_validation_alert(results_summary)
        context.log.error(f"Schema drift detected:\n{alert_message}")

        # Fail the asset to ensure immediate visibility of schema issues
        # This prevents downstream assets from running with broken schemas
        raise Exception(
            f"Schema drift validation failed: {results_summary['total_issues']} issues detected. "
            f"Issues by type: {results_summary['issues_by_type']}. "
            f"Issues by schema: {results_summary['issues_by_schema']}. "
            f"Failed views: {', '.join(results_summary['failed_views'][:5])}{'...' if len(results_summary['failed_views']) > 5 else ''}. "
            f"Check logs for detailed error information."
        )
    else:
        context.log.info("✅ Schema validation completed successfully - no drift detected")

    return results_summary


def get_all_database_views(snowflake: SnowflakeResource, context: AssetExecutionContext) -> List[tuple]:
    """Discover all views in target schemas"""

    with snowflake.get_connection() as conn:
        views_query = """
        SELECT
            table_schema,
            table_name,
            table_schema || '.' || table_name as full_view_name,
            created as view_created_date,
            last_altered as view_last_modified
        FROM information_schema.views
        WHERE table_schema IN ('RAW', 'STAGE', 'ANALYTICS')
        ORDER BY table_schema, table_name
        """

        cursor = conn.cursor()
        cursor.execute(views_query)
        views = cursor.fetchall()

        context.log.info(f"Discovered views by schema:")
        for schema in ['RAW', 'STAGE', 'ANALYTICS']:
            schema_views = [v for v in views if v[0] == schema]
            context.log.info(f"  - {schema}: {len(schema_views)} views")

        return views


def validate_database_views(views: List[tuple], snowflake: SnowflakeResource, context: AssetExecutionContext) -> List[Dict[str, Any]]:
    """Validate each view and return detailed results"""

    validation_results = []
    start_time = datetime.now()

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        for view in views:
            schema_name, view_name, full_name, created_date, modified_date = view

            view_result = {
                'view_name': full_name,
                'schema': schema_name,
                'table_name': view_name,
                'status': 'success',
                'error_message': None,
                'drift_type': None,
                'validation_timestamp': datetime.now().isoformat(),
                'created_date': str(created_date) if created_date else None,
                'modified_date': str(modified_date) if modified_date else None
            }

            try:
                # Validate view with simple query
                validation_query = f"SELECT * FROM {full_name} LIMIT 1"
                context.log.debug(f"Validating {full_name}")

                cursor.execute(validation_query)
                cursor.fetchall()  # Consume results

                context.log.debug(f"✅ {full_name} - validation successful")

            except Exception as e:
                error_message = str(e)
                context.log.info(f"❌ {full_name} - validation failed: {error_message}")

                # Categorize the error
                drift_type = categorize_schema_error(error_message.lower(), full_name)

                view_result.update({
                    'status': 'failed',
                    'error_message': error_message,
                    'drift_type': drift_type
                })

                if drift_type:
                    context.log.warning(f"🚨 Schema drift detected in {full_name}: {drift_type}")
                else:
                    context.log.info(f"🔍 Non-schema error in {full_name} (ignored): {error_message[:100]}...")

            validation_results.append(view_result)

    execution_time = (datetime.now() - start_time).total_seconds()
    context.log.info(f"View validation completed in {execution_time:.2f} seconds")

    return validation_results


def process_validation_results(validation_results: List[Dict[str, Any]], context: AssetExecutionContext) -> Dict[str, Any]:
    """Process validation results and generate summary metrics"""

    # Calculate summary statistics
    total_views = len(validation_results)
    failed_views = [r for r in validation_results if r['status'] == 'failed']
    schema_drift_issues = [r for r in failed_views if r['drift_type'] is not None]

    # Group issues by type and schema
    issues_by_type = {}
    issues_by_schema = {}

    for issue in schema_drift_issues:
        drift_type = issue['drift_type']
        schema = issue['schema']

        issues_by_type[drift_type] = issues_by_type.get(drift_type, 0) + 1
        issues_by_schema[schema] = issues_by_schema.get(schema, 0) + 1

    success_rate = ((total_views - len(failed_views)) / total_views * 100) if total_views > 0 else 0

    summary = {
        'total_views': total_views,
        'successful_validations': total_views - len(failed_views),
        'total_failures': len(failed_views),
        'total_issues': len(schema_drift_issues),
        'success_rate': round(success_rate, 2),
        'issues_by_type': issues_by_type,
        'issues_by_schema': issues_by_schema,
        'failed_views': [{'view': r['view_name'], 'type': r['drift_type']} for r in schema_drift_issues],
        'detailed_results': validation_results,
        'execution_duration': 0  # Will be set by caller
    }

    # Log summary statistics
    context.log.info(f"📊 Validation Summary:")
    context.log.info(f"  - Total views: {total_views}")
    context.log.info(f"  - Success rate: {success_rate:.1f}%")
    context.log.info(f"  - Schema drift issues: {len(schema_drift_issues)}")

    if issues_by_type:
        context.log.info(f"  - Issues by type: {dict(issues_by_type)}")
    if issues_by_schema:
        context.log.info(f"  - Issues by schema: {dict(issues_by_schema)}")

    return summary


def categorize_schema_error(error_message: str, view_name: str) -> str:
    """Categorize error type to identify schema drift vs. other issues"""

    # Missing columns
    if any(keyword in error_message for keyword in [
        'column does not exist', 'column doesn\'t exist', 'unknown column',
        'missing column', 'ambiguous column', 'cannot resolve column'
    ]):
        return 'missing_column'

    # Missing tables/views/objects
    elif any(keyword in error_message for keyword in [
        'table does not exist', 'table doesn\'t exist', 'view does not exist',
        'view doesn\'t exist', 'object does not exist', 'object doesn\'t exist',
        'does not exist or not authorized', 'table or view does not exist'
    ]):
        return 'missing_table'

    # Invalid identifiers and schema references
    elif any(keyword in error_message for keyword in [
        'invalid identifier', 'cannot resolve'
    ]):
        return 'schema_reference_error'

    # Join/relationship issues
    elif any(keyword in error_message for keyword in [
        'join', 'foreign key', 'reference', 'constraint'
    ]):
        return 'relationship_error'

    # Data type compatibility issues
    elif any(keyword in error_message for keyword in [
        'data type', 'cannot convert', 'type mismatch', 'cast'
    ]):
        return 'data_type_error'

    # Not a schema drift issue (data quality, permissions, etc.)
    else:
        return None  # Don't report non-schema issues


def format_validation_alert(results_summary: Dict[str, Any]) -> str:
    """Format validation results into structured alert message"""

    alert_parts = [
        f"Schema drift detected in {results_summary['total_issues']} views:",
        f"Success rate: {results_summary['success_rate']}% ({results_summary['successful_validations']}/{results_summary['total_views']})"
    ]

    if results_summary['issues_by_type']:
        alert_parts.append("\nIssues by type:")
        for issue_type, count in results_summary['issues_by_type'].items():
            alert_parts.append(f"  - {issue_type}: {count} views")

    if results_summary['issues_by_schema']:
        alert_parts.append("\nIssues by schema:")
        for schema, count in results_summary['issues_by_schema'].items():
            alert_parts.append(f"  - {schema}: {count} views")

    alert_parts.append("\nAffected views:")
    for failed_view in results_summary['failed_views'][:10]:  # Limit to 10
        alert_parts.append(f"  - {failed_view['view']}: {failed_view['type']}")

    if len(results_summary['failed_views']) > 10:
        remaining = len(results_summary['failed_views']) - 10
        alert_parts.append(f"  ... and {remaining} more")

    return "\n".join(alert_parts)