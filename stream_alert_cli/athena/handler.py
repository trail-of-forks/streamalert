"""
Copyright 2017-present, Airbnb Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from stream_alert.athena_partition_refresh.main import AthenaRefresher
from stream_alert.classifier.clients import FirehoseClient
from stream_alert.shared.alert import Alert
from stream_alert.shared.athena import AthenaClient
from stream_alert.shared.logger import get_logger
from stream_alert_cli.athena import helpers
from stream_alert_cli.helpers import continue_prompt, record_to_schema

LOGGER = get_logger(__name__)

CREATE_TABLE_STATEMENT = ('CREATE EXTERNAL TABLE {table_name} ({schema}) '
                          'PARTITIONED BY (dt string) '
                          'STORED AS PARQUET '
                          'LOCATION \'s3://{bucket}/{table_name}/\'')

MAX_QUERY_LENGTH = 262144


def athena_handler(options, config):
    """Main Athena handler

    Args:
        options (argparse.Namespace): The parsed args passed from the CLI
        config (CLIConfig): Loaded StreamAlert config

    Returns:
        bool: False if errors occurred, True otherwise
    """
    if options.subcommand == 'rebuild-partitions':
        return rebuild_partitions(
            options.table_name,
            options.bucket,
            config)

    elif options.subcommand == 'drop-all-tables':
        return drop_all_tables(config)

    elif options.subcommand == 'create-table':
        return create_table(
            options.table_name,
            options.bucket,
            config,
            options.schema_override)


def get_athena_client(config):
    """Get an athena client using the current config settings

    Args:
        config (CLIConfig): Loaded StreamAlert config

    Returns:
        AthenaClient: instantiated client for performing athena actions
    """
    prefix = config['global']['account']['prefix']
    athena_config = config['lambda']['athena_partition_refresh_config']

    db_name = athena_config.get(
        'database_name',
        AthenaRefresher.STREAMALERT_DATABASE.format(prefix)
    )

    # Get the S3 bucket to store Athena query results
    results_bucket = athena_config.get(
        'results_bucket',
        's3://{}.streamalert.athena-results'.format(prefix)
    )

    return AthenaClient(
        db_name,
        results_bucket,
        'stream_alert_cli',
        region=config['global']['account']['region']
    )


def rebuild_partitions(table, bucket, config):
    """Rebuild an Athena table's partitions

    Steps:
      - Get the list of current partitions
      - Destroy existing table
      - Re-create tables
      - Re-create partitions

    Args:
        table (str): The name of the table being rebuilt
        bucket (str): The s3 bucket to be used as the location for Athena data
        table_type (str): The type of table being refreshed
            Types of 'data' and 'alert' are accepted, but only 'data' is implemented
        config (CLIConfig): Loaded StreamAlert config

    Returns:
        bool: False if errors occurred, True otherwise
    """
    sanitized_table_name = FirehoseClient.firehose_log_name(table)

    athena_client = get_athena_client(config)

    # Get the current set of partitions
    partitions = athena_client.get_table_partitions(sanitized_table_name)
    if not partitions:
        LOGGER.info('No partitions to rebuild for %s, nothing to do', sanitized_table_name)
        return False

    # Drop the table
    LOGGER.info('Dropping table %s', sanitized_table_name)
    if not athena_client.drop_table(sanitized_table_name):
        return False

    LOGGER.info('Creating table %s', sanitized_table_name)

    # Re-create the table with previous partitions
    if not create_table(table, bucket, config):
        return False

    new_partitions_statement = helpers.add_partition_statement(
        partitions, bucket, sanitized_table_name)

    # Make sure our new alter table statement is within the query API limits
    if len(new_partitions_statement) > MAX_QUERY_LENGTH:
        file_name = 'partitions_{}.txt'.format(sanitized_table_name)
        LOGGER.error(
            'Partition statement too large, writing to local file with name %s',
            file_name
        )
        with open(file_name, 'w') as partition_file:
            partition_file.write(new_partitions_statement)
        return False

    LOGGER.info('Creating %d new partitions for %s', len(partitions), sanitized_table_name)

    success = athena_client.run_query(query=new_partitions_statement)
    if not success:
        LOGGER.error('Error re-creating new partitions for %s', sanitized_table_name)
        return False

    LOGGER.info('Successfully rebuilt partitions for %s', sanitized_table_name)
    return True


def drop_all_tables(config):
    """Drop all 'streamalert' Athena tables

    Used when cleaning up an existing deployment

    Args:
        config (CLIConfig): Loaded StreamAlert config

    Returns:
        bool: False if errors occurred, True otherwise
    """
    if not continue_prompt(message='Are you sure you want to drop all Athena tables?'):
        return False

    athena_client = get_athena_client(config)

    if not athena_client.drop_all_tables():
        LOGGER.error('Failed to drop one or more tables from database: %s', athena_client.database)
        return False

    LOGGER.info('Successfully dropped all tables from database: %s', athena_client.database)
    return True


def _construct_create_table_statement(schema, table_name, bucket):
    """Convert a dictionary based Athena schema to a Hive DDL statement

    Args:
        schema (dict): The sanitized Athena schema
        table_name (str): The name of the Athena table to create
        bucket (str): The S3 bucket containing the data

    Returns:
        str: The Hive DDL CREATE TABLE expression
    """
    # Construct the main Athena Schema
    schema_statement = []
    for key_name in sorted(schema.keys()):
        key_type = schema[key_name]
        if isinstance(key_type, str):
            schema_statement.append('{0} {1}'.format(key_name, key_type))
        # Account for nested structs
        elif isinstance(key_type, dict):
            struct_schema = ', '.join(
                '{0}:{1}'.format(sub_key, key_type[sub_key])
                for sub_key in sorted(key_type.keys())
            )
            schema_statement.append('{0} struct<{1}>'.format(key_name, struct_schema))

    return CREATE_TABLE_STATEMENT.format(
        table_name=table_name,
        schema=', '.join(schema_statement),
        bucket=bucket)


def create_table(table, bucket, config, schema_override=None):
    """Create a 'streamalert' Athena table

    Args:
        table (str): The name of the table being rebuilt
        bucket (str): The s3 bucket to be used as the location for Athena data
        table_type (str): The type of table being refreshed
        config (CLIConfig): Loaded StreamAlert config
        schema_override (set): An optional set of key=value pairs to be used for
            overriding the configured column_name=value_type.

    Returns:
        bool: False if errors occurred, True otherwise
    """
    enabled_logs = FirehoseClient.load_enabled_log_sources(
        config['global']['infrastructure']['firehose'],
        config['logs']
    )

    # Convert special characters in schema name to underscores
    sanitized_table_name = FirehoseClient.firehose_log_name(table)

    # Check that the log type is enabled via Firehose
    if sanitized_table_name != 'alerts' and sanitized_table_name not in enabled_logs:
        LOGGER.error('Table name %s missing from configuration or '
                     'is not enabled.', sanitized_table_name)
        return False

    athena_client = get_athena_client(config)

    # Check if the table exists
    if athena_client.check_table_exists(sanitized_table_name):
        LOGGER.info('The \'%s\' table already exists.', sanitized_table_name)
        return False

    if table == 'alerts':
        # get a fake alert so we can get the keys needed and their types
        alert = Alert('temp_rule_name', {}, {})
        output = alert.output_dict()
        schema = record_to_schema(output)
        athena_schema = helpers.logs_schema_to_athena_schema(schema)

        query = _construct_create_table_statement(
            schema=athena_schema, table_name=table, bucket=bucket)

    else:  # all other tables are log types

        log_info = config['logs'][table.replace('_', ':', 1)]

        schema = dict(log_info['schema'])
        sanitized_schema = FirehoseClient.sanitize_keys(schema)

        athena_schema = helpers.logs_schema_to_athena_schema(sanitized_schema)

        # Add envelope keys to Athena Schema
        configuration_options = log_info.get('configuration')
        if configuration_options:
            envelope_keys = configuration_options.get('envelope_keys')
            if envelope_keys:
                sanitized_envelope_key_schema = FirehoseClient.sanitize_keys(envelope_keys)
                # Note: this key is wrapped in backticks to be Hive compliant
                athena_schema['`streamalert:envelope_keys`'] = helpers.logs_schema_to_athena_schema(
                    sanitized_envelope_key_schema)

        # Handle Schema overrides
        #   This is useful when an Athena schema needs to differ from the normal log schema
        if schema_override:
            for override in schema_override:
                column_name, column_type = override.split('=')
                # Columns are escaped to avoid Hive issues with special characters
                column_name = '`{}`'.format(column_name)
                if column_name in athena_schema:
                    athena_schema[column_name] = column_type
                    LOGGER.info('Applied schema override: %s:%s', column_name, column_type)
                else:
                    LOGGER.error(
                        'Schema override column %s not found in Athena Schema, skipping',
                        column_name
                    )

        query = _construct_create_table_statement(
            schema=athena_schema, table_name=sanitized_table_name, bucket=bucket)

    success = athena_client.run_query(query=query)
    if not success:
        LOGGER.error('The %s table could not be created', sanitized_table_name)
        return False

    # Update the CLI config
    if (table != 'alerts' and
            bucket not in config['lambda']['athena_partition_refresh_config']['buckets']):
        config['lambda']['athena_partition_refresh_config']['buckets'][bucket] = 'data'
        config.write()

    LOGGER.info('The %s table was successfully created!', sanitized_table_name)

    return True


def create_log_tables(config):
    if not config['global']['infrastructure'].get('firehose', {}).get('enabled'):
        return

    firehose_config = config['global']['infrastructure']['firehose']
    firehose_s3_bucket_suffix = firehose_config.get('s3_bucket_suffix', 'streamalert.data')
    firehose_s3_bucket_name = '{}.{}'.format(config['global']['account']['prefix'],
                                             firehose_s3_bucket_suffix)

    enabled_logs = FirehoseClient.load_enabled_log_sources(
        config['global']['infrastructure']['firehose'],
        config['logs']
    )

    for log_stream_name, _ in enabled_logs.iteritems():
        create_table(log_stream_name, firehose_s3_bucket_name, config)

    return True
