package org.apache.flink.formats.json.hermes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import junit.framework.TestCase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by zyxing on 2020/9/14
 */
public class CanalHermesJsonFormatFactoryTest extends TestCase {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final TableSchema SCHEMA = TableSchema.builder()
		.field("groupid", DataTypes.INT())
		.field("groupname", DataTypes.STRING())
		.field("groupdesc", DataTypes.STRING())
		.field("DataChange_LastTime", DataTypes.TIMESTAMP())
		.build();

	private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();

	final String insert = "{\"schemaName\": \"fltreportdb\", \"tableName\": \"presto_group\", \"eventType\": \"INSERT\", \"beforeColumnList\": [], \"afterColumnList\": [{\"name\": \"groupid\", \"value\": \"74\", \"isUpdated\": true, \"isKey\": true, \"isNull\": false}, {\"name\": \"groupname\", \"value\": \"test\", \"isUpdated\": true, \"isKey\": false, \"isNull\": false}, {\"name\": \"groupdesc\", \"value\": \"test\", \"isUpdated\": true, \"isKey\": false, \"isNull\": false}, {\"name\": \"DataChange_LastTime\", \"value\": \"2020-09-14 22:50:35\", \"isUpdated\": true, \"isKey\": false, \"isNull\": false}]}";

	final String update = "{\"schemaName\": \"fltreportdb\", \"tableName\": \"presto_group\", \"eventType\": \"UPDATE\", \"beforeColumnList\": [{\"name\": \"groupid\", \"value\": \"75\", \"isUpdated\": false, \"isKey\": true, \"isNull\": false}, {\"name\": \"groupname\", \"value\": \"test\", \"isUpdated\": false, \"isKey\": false, \"isNull\": false}, {\"name\": \"groupdesc\", \"value\": \"test\", \"isUpdated\": false, \"isKey\": false, \"isNull\": false}, {\"name\": \"DataChange_LastTime\", \"value\": \"2020-09-16 13:52:13\", \"isUpdated\": false, \"isKey\": false, \"isNull\": false}], \"afterColumnList\": [{\"name\": \"groupid\", \"value\": \"75\", \"isUpdated\": false, \"isKey\": true, \"isNull\": false}, {\"name\": \"groupname\", \"value\": \"test\", \"isUpdated\": false, \"isKey\": false, \"isNull\": false}, {\"name\": \"groupdesc\", \"value\": \"update\", \"isUpdated\": true, \"isKey\": false, \"isNull\": false}, {\"name\": \"DataChange_LastTime\", \"value\": \"2020-09-16 13:54:50\", \"isUpdated\": true, \"isKey\": false, \"isNull\": false}]}";

	final String delete = "{\"schemaName\": \"fltreportdb\", \"tableName\": \"presto_group\", \"eventType\": \"DELETE\", \"beforeColumnList\": [{\"name\": \"groupid\", \"value\": \"66\", \"isUpdated\": false, \"isKey\": true, \"isNull\": false}, {\"name\": \"groupname\", \"value\": \"test\", \"isUpdated\": false, \"isKey\": false, \"isNull\": false}, {\"name\": \"groupdesc\", \"value\": \"test\", \"isUpdated\": false, \"isKey\": false, \"isNull\": false}, {\"name\": \"DataChange_LastTime\", \"value\": \"2020-09-14 21:20:35\", \"isUpdated\": false, \"isKey\": false, \"isNull\": false}], \"afterColumnList\": []}\n";

	@Test
	public void testCreateDecodingFormat() throws IOException {
		final CanalHermesJsonDeserializationSchema self = new CanalHermesJsonDeserializationSchema(
			ROW_TYPE,
			new RowDataTypeInfo(ROW_TYPE),
			true,
			TimestampFormat.SQL, "");

//		final CanalJsonDeserializationSchema expectedDeser = new CanalJsonDeserializationSchema(
//			ROW_TYPE,
//			new RowDataTypeInfo(ROW_TYPE),
//			true,
//			TimestampFormat.ISO_8601);

//		final Map<String, String> options = getAllOptions();

//		final DynamicTableSource actualSource = createTableSource(options);

//		TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
//			(TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

//		DeserializationSchema<RowData> actualDeser = scanSourceMock.valueFormat
//			.createRuntimeDecoder(
//				ScanRuntimeProviderContext.INSTANCE,
//				SCHEMA.toRowDataType());
//
//		assertEquals(expectedDeser, actualDeser);

		self.deserialize(update.getBytes(), null);
	}

	private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
		Map<String, String> options = getAllOptions();
		optionModifier.accept(options);
		return options;
	}

	private Map<String, String> getAllOptions() {
		final Map<String, String> options = new HashMap<>();
		options.put("connector", TestDynamicTableFactory.IDENTIFIER);
		options.put("target", "MyTarget");
		options.put("buffer-size", "1000");

		options.put("format", "canal-hermes-json");
		options.put("canal-hermes-json.ignore-parse-errors", "true");
		options.put("canal-hermes-json.timestamp-format.standard", "ISO-8601");
		return options;
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(SCHEMA, options, "mock source"),
			new Configuration(),
			CanalHermesJsonFormatFactoryTest.class.getClassLoader());
	}

}
