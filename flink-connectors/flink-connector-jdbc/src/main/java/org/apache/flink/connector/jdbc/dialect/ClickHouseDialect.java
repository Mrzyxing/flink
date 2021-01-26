/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.dialect;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.flink.connector.jdbc.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;


/**
 * JDBC dialect for ClickHouse.
 */
public class ClickHouseDialect extends AbstractDialect {

	private static final long serialVersionUID = 1L;

	@Override
	public String dialectName() {
		return "ClickHouse";
	}

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:clickhouse:");
	}

	@Override
	public JdbcRowConverter getRowConverter(RowType rowType) {
		return new ClickHouseRowConverter(rowType);
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return "`" + identifier + "`";
	}

	@Override
	public Optional<String> getUpsertStatement(String tableName, String[] fieldNames,
		String[] uniqueKeyFields) {
		return Optional.of(getInsertIntoStatement(tableName, fieldNames));
	}

	@Override
	public String getRowExistsStatement(String tableName, String[] conditionFields) {
		return null;
	}

	@Override
	public String getUpdateStatement(String tableName, String[] fieldNames,
		String[] conditionFields) {
		return getInsertIntoStatement(tableName, fieldNames);
	}

	@Override
	public String getDeleteStatement(String tableName, String[] fieldNames) {
		return getInsertIntoStatement(tableName, fieldNames);
	}

	@Override
	public String getSelectFromStatement(String tableName, String[] selectFields,
		String[] conditionFields) {
		return null;
	}

	@Override
	public int maxDecimalPrecision() {
		return 0;
	}

	@Override
	public int minDecimalPrecision() {
		return 0;
	}

	@Override
	public int maxTimestampPrecision() {
		return 0;
	}

	@Override
	public int minTimestampPrecision() {
		return 0;
	}

	@Override
	public List<LogicalTypeRoot> unsupportedTypes() {
		// The data types used in Mysql are list at:
		// https://dev.mysql.com/doc/refman/8.0/en/data-types.html

		// TODO: We can't convert BINARY data type to
		//  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in LegacyTypeInfoDataTypeConverter.
		return Arrays.asList(
			LogicalTypeRoot.BINARY,
			LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
			LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
			LogicalTypeRoot.INTERVAL_YEAR_MONTH,
			LogicalTypeRoot.INTERVAL_DAY_TIME,
			LogicalTypeRoot.ARRAY,
			LogicalTypeRoot.MULTISET,
			LogicalTypeRoot.MAP,
			LogicalTypeRoot.ROW,
			LogicalTypeRoot.DISTINCT_TYPE,
			LogicalTypeRoot.STRUCTURED_TYPE,
			LogicalTypeRoot.NULL,
			LogicalTypeRoot.RAW,
			LogicalTypeRoot.SYMBOL,
			LogicalTypeRoot.UNRESOLVED
		);
	}
}
