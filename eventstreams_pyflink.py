from pyflink.table import Schema, TableDescriptor
from pyflink.java_gateway import get_gateway


KAFKA_CONNECTOR_OPTION_DEFAULTS = {
    "properties.bootstrap.servers": "kafka-jumbo1001.eqiad.wmnet:9092",
    "scan.startup.mode":  "latest-offset"
}


def flink_jvm():
    return get_gateway().jvm


def sanitize_name(name):
    jvm = flink_jvm()
    JsonSchemaConverter_j = jvm.org.wikimedia.flink.JsonSchemaConverter
    return JsonSchemaConverter_j.sanitizeFieldName(name)


def eventstream_flink_schema_builder_j(eventstream, kafka_watermark_column_name = None):
    jvm = flink_jvm()
    schema_converter_j = jvm.org.wikimedia.flink.JsonSchemaConverter()
    builder_j = schema_converter_j.getSchemaBuilder(eventstream.schema())
    if kafka_watermark_column_name:
        builder_j.columnByMetadata(
            kafka_watermark_column_name,
            "TIMESTAMP_LTZ(3) NOT NULL",
            "timestamp",
            True
        )
        builder_j.watermark(kafka_watermark_column_name, kafka_watermark_column_name)
    return builder_j


def eventstream_flink_schema_builder(eventstream, kafka_watermark_column_name = None):
    return Schema.Builder(eventstream_flink_schema_builder_j(eventstream, kafka_watermark_column_name))


def eventstream_table_descriptor_builder(eventstream, connector_options: dict = {}):
    td_builder = TableDescriptor \
        .for_connector("kafka") \
        .option("topic", ';'.join(eventstream.topics())) \
        .schema(eventstream_flink_schema_builder(eventstream, "kafka_timestamp").build()) \
        .format("json")
    for k, v in dict(KAFKA_CONNECTOR_OPTION_DEFAULTS, **connector_options).items():
        td_builder.option(k, str(v))
    return td_builder


def eventstream_flink_table(table_env, stream_name, connector_options = {}, table_name = None):
    jvm = flink_jvm()
    event_stream_factory = jvm.org.wikimedia.eventutilities.core.event.WikimediaDefaults.EVENT_STREAM_FACTORY
    eventstream = event_stream_factory.createEventStream(stream_name)
    table_builder = eventstream_table_descriptor_builder(eventstream, connector_options)
    table = table_env.from_descriptor(table_builder.build())
    table_name = table_name or sanitize_name(stream_name)
    table_env.register_table(table_name, table)
    return table
 
# example:
#table = eventstream_flink_table(st_env, "mediawiki.revision-create")

#result = st_env.sql_query(
#    """
#     SELECT TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE), database, COUNT(DISTINCT database)
#     FROM mediawiki_revision_create
#     GROUP BY TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE), database
#     """
#)


