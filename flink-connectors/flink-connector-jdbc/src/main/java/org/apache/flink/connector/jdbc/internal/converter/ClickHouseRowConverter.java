package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * Created by zyxing on 2020/12/22
 */
public class ClickHouseRowConverter extends AbstractJdbcRowConverter {

	private static final long serialVersionUID = 1L;

	public ClickHouseRowConverter(RowType rowType) {
		super(rowType);
	}

	@Override
	public String converterName() {
		return "ClickHouse";
	}

}
