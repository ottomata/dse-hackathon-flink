package org.wikimedia.flink;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.table.api.Schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class JsonSchemaConverter {
    private static final Logger log = LoggerFactory.getLogger(JsonSchemaConverter.class);

    private boolean shouldSanitizeFieldNames = false; //JsonSchemaConverterConfig.SANITIZE_FIELD_NAMES_DEFAULT;

    // If shouldSanitizeFieldNames anything in a field name matching this regex
    // will be replaced with sanitizeFieldReplacement.
    private static final Pattern sanitizeFieldPattern = Pattern.compile("(^[^A-Za-z_]|[^A-Za-z0-9_])");
    private static final String sanitizeFieldReplacement = "_";

    // JSONSchema field names used to convert the JSONSchema Flink DataTypes
    // (Copied from Flinks' JsonRowSchemaConverter
    // see https://spacetelescope.github.io/understanding-json-schema/UnderstandingJSONSchema.pdf
    private static final String TITLE = "title";
    private static final String DESCRIPTION = "description";
    private static final String PROPERTIES = "properties";
    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String TYPE = "type";
    private static final String FORMAT = "format";
    private static final String CONTENT_ENCODING = "contentEncoding";
    private static final String ITEMS = "items";
    private static final String ADDITIONAL_ITEMS = "additionalItems";
    private static final String REF = "$ref";
    private static final String ALL_OF = "allOf";
    private static final String ANY_OF = "anyOf";
    private static final String NOT = "not";
    private static final String ONE_OF = "oneOf";

    // from https://tools.ietf.org/html/draft-zyp-json-schema-03#page-14
    private static final String DISALLOW = "disallow";
    private static final String EXTENDS = "extends";

    private static final String TYPE_NULL = "null";
    private static final String TYPE_BOOLEAN = "boolean";
    private static final String TYPE_OBJECT = "object";
    private static final String TYPE_ARRAY = "array";
    private static final String TYPE_NUMBER = "number";
    private static final String TYPE_INTEGER = "integer";
    private static final String TYPE_STRING = "string";

    private static final String FORMAT_DATE = "date";
    private static final String FORMAT_TIME = "time";
    private static final String FORMAT_DATE_TIME = "date-time";

    private static final String CONTENT_ENCODING_BASE64 = "base64";

    public Schema.Builder getSchemaBuilder(ObjectNode jsonSchema) {
        if (jsonSchema.isNull())
            return null;

        String title = jsonSchema.get(TITLE).textValue();
        String sanitizedTitle = shouldSanitizeFieldNames ? sanitizeFieldName(title) : title;

        DataType rowDataType = toDataType(jsonSchema, sanitizedTitle);

        Schema.Builder builder = Schema.newBuilder();
        return builder.fromRowDataType(rowDataType);
    }

    /**
     * A DataTypes.Field is just a special DataType with a name and an optional description.
     * Useful for named DataType fields within a Row DataType.
     * @param jsonField
     * @param fieldName
     * @return
     */
    public DataTypes.Field toFieldDataType(ObjectNode jsonField, String fieldName) {
        String sanitizedFieldName = shouldSanitizeFieldNames ? sanitizeFieldName(fieldName) : fieldName;
        DataType dataType = toDataType(jsonField, sanitizedFieldName);

        DataTypes.Field fieldDataType;
        if (jsonField.hasNonNull(DESCRIPTION)) {
            fieldDataType =  DataTypes.FIELD(
                sanitizedFieldName,
                dataType,
                jsonField.get(DESCRIPTION).textValue()
            );
        } else {
            fieldDataType = DataTypes.FIELD(
                sanitizedFieldName,
                dataType
            );
        }

        return fieldDataType;
    }

    /**
     * Converts this JSONSchema property to a Flink DataType.
     *
     * @param jsonField
     * @param fieldName
     * @return
     */
    public DataType toDataType(ObjectNode jsonField, String fieldName) {
        if (jsonField.isNull())
            return null;

        if (fieldName == null && jsonField.hasNonNull(TITLE)) {
            fieldName = jsonField.get(TITLE).textValue();
        }

        JsonNode schemaTypeNode = jsonField.get(TYPE);
        if (schemaTypeNode == null || !schemaTypeNode.isTextual())
            throw new RuntimeException("Schema must contain 'type' field");


        DataType dataType;
        switch (schemaTypeNode.textValue()) {

            case "boolean":
                dataType = DataTypes.BOOLEAN();
                break;

            case "integer":
                dataType = DataTypes.INT();
                break;

            case "number":
                dataType = DataTypes.DOUBLE();
                break;

            case "string":
                dataType = DataTypes.STRING();
                break;

            case "array":
                JsonNode itemsSchema = jsonField.get(ITEMS);

                // Arrays must specify the type of their elements.
                if (itemsSchema == null || itemsSchema.isNull())
                    throw new RuntimeException(fieldName + " array schema did not specify the items type");

                // Arrays must only use a single type, not tuple validation.
                if (!itemsSchema.isObject() || !itemsSchema.has("type")) {
                    throw new RuntimeException(
                        fieldName + " array schema must specify the items type for field, e.g. \"items\": { \"type\": \"string\""
                    );
                }
                dataType = DataTypes.ARRAY(toDataType((ObjectNode)itemsSchema, null));
                break;

            case "object":
                JsonNode properties = jsonField.get(PROPERTIES);

                if (properties == null || !properties.isObject())
                    throw new RuntimeException(fieldName + " struct schema's \"properties\" is not an object.");

//                JsonNode requiredFieldList = jsonSchema.get(requiredField);
//                if (requiredFieldList != null && !requiredFieldList.isArray()) {
//                    throw new RuntimeException(fieldName + " struct schema's \"required\" is not an array.");
//                }

//                ArrayList<DataTypes.Field> rowFields = new ArrayList<>();
                DataTypes.Field[] rowFields = new DataTypes.Field[properties.size()];
                int fieldNum = 0;
                Iterator<Map.Entry<String,JsonNode>> fields = properties.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String subFieldName = field.getKey();

                    // TODO find a better way to do this than brute force checking the list every time?
                    // If we have a JSONSchema list of 'required' field names,
                    // check if any of these fields are required.  This is Draft 4+ JSONSChema.
//                    boolean subFieldRequired = arrayNodeContainsTextValue(
//                        (ArrayNode)requiredFieldList, subFieldName
//                    );

//                    String sanitizedSubFieldName = shouldSanitizeFieldNames ? sanitizeFieldName(subFieldName) : subFieldName;


                    DataTypes.Field fieldDataType = toFieldDataType((ObjectNode)field.getValue(), subFieldName);
                    rowFields[fieldNum] = fieldDataType;
                    fieldNum++;
                }
                dataType = DataTypes.ROW(rowFields);
                break;

            default:
                throw new RuntimeException(
                    "Unknown schema type " + schemaTypeNode.textValue() + "in field " + fieldName
                );
        }

        return dataType;
    }



    /**
     * Replaces characters in fieldName that are not suitable for
     * field names with underscores. This tries to conform with
     * allowed Avro (and Parquet) field names, which should
     * be in general a good rule for integration with
     * other downstream datastores too (e.g. SQL stores).
     *
     * From https://avro.apache.org/docs/1.8.0/spec.html#names
     *
     * The name portion of a fullname, record field names, and enum symbols must:
     *  start with [A-Za-z_]
     *  subsequently contain only [A-Za-z0-9_]
     *
     * @param fieldName
     * @return sanitized field name
     */
    public static String sanitizeFieldName(String fieldName) {
        if (fieldName == null)
            return fieldName;
        else {
            Matcher m = sanitizeFieldPattern.matcher(fieldName);
            String sanitizedFieldName = m.find() ? m.replaceAll(sanitizeFieldReplacement) : fieldName;

            if (sanitizedFieldName != fieldName)
                log.debug("Sanitized field name " + fieldName + " to " + sanitizedFieldName);

            return sanitizedFieldName;
        }
    }
}
