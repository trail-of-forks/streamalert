import boto3

from stream_alert.rule_processor.firehose import FirehoseClient
from stream_alert_cli.logger import LOGGER_CLI

def create_log_format_tables(config):
  enabled_logs = FirehoseClient.load_enabled_log_sources(
      config['global']['infrastructure']['firehose'],
      config['logs']
  )

  glue_client = boto3.client("glue")
  database_name = config['global']['infrastructure']['firehose']['catalog_name']
  table_prefix = config['global']['infrastructure']['firehose']['catalog_table_prefix']

  table_collection = glue_client.get_tables(DatabaseName=database_name)
  catalog_tables = [k['Name'] for k in table_collection['TableList']]

  for log_table in enabled_logs:
    if table_prefix+log_table in catalog_tables:
      LOGGER_CLI.info("%s table already exists, not recreating", log_table)
      continue

    LOGGER_CLI.info("creating %s table", log_table)
    log_info = config['logs'][log_table.replace('_', ':', 1)]

    schema = dict(log_info['schema'])
    sanitized_schema = FirehoseClient.sanitize_keys(schema)

    columns = [{'Name':key_name, 'Type':'string'} for key_name in sorted(sanitized_schema)]

    table_input = {
      'Name':table_prefix + log_table,
      'StorageDescriptor': {
        'Columns': columns
      }
    }
    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)

  return True

#    else: # all other tables are log types
#
#        log_info = config['logs'][table.replace('_', ':', 1)]
#
#        schema = dict(log_info['schema'])
#        sanitized_schema = FirehoseClient.sanitize_keys(schema)
#
#        athena_schema = helpers.logs_schema_to_athena_schema(sanitized_schema)
#
#        # Add envelope keys to Athena Schema
#        configuration_options = log_info.get('configuration')
#        if configuration_options:
#            envelope_keys = configuration_options.get('envelope_keys')
#            if envelope_keys:
#                sanitized_envelope_key_schema = FirehoseClient.sanitize_keys(envelope_keys)
#                # Note: this key is wrapped in backticks to be Hive compliant
#                athena_schema['`streamalert:envelope_keys`'] = helpers.logs_schema_to_athena_schema(
#                    sanitized_envelope_key_schema)
#
#        # Handle Schema overrides
#        #   This is useful when an Athena schema needs to differ from the normal log schema
#        if schema_override:
#            for override in schema_override:
#                column_name, column_type = override.split('=')
#                if not all([column_name, column_type]):
#                    LOGGER_CLI.error('Invalid schema override [%s], use column_name=type format',
#                                     override)
#
#                # Columns are escaped to avoid Hive issues with special characters
#                column_name = '`{}`'.format(column_name)
#                if column_name in athena_schema:
#                    athena_schema[column_name] = column_type
#                    LOGGER_CLI.info('Applied schema override: %s:%s', column_name, column_type)
#                else:
#                    LOGGER_CLI.error(
#                        'Schema override column %s not found in Athena Schema, skipping',
#                        column_name)
#
#        query = _construct_create_table_statement(
#            schema=athena_schema, table_name=sanitized_table_name, bucket=bucket)
#
#    success = athena_client.run_query(query=query)
#    if not success:
#        LOGGER_CLI.error('The %s table could not be created', sanitized_table_name)
#        return
#
#    # Update the CLI config
#    if (table != 'alerts' and
#            bucket not in config['lambda']['athena_partition_refresh_config']['buckets']):
#        config['lambda']['athena_partition_refresh_config']['buckets'][bucket] = 'data'
#        config.write()
#
#    LOGGER_CLI.info('The %s table was successfully created!', sanitized_table_name)



