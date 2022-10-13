package io.tidb.bigdata.flink.connector.sink.output;

/*
 * Copyright 2022 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

/** Wrapper class for column pruning. The array `indexes` stores the indexes of visible columns. */
public class ColumnPruningOutputRowData implements RowData {

  private final RowData rowData;
  private final int[] indexes;

  public ColumnPruningOutputRowData(RowData rowData, int[] indexes) {

    this.rowData = rowData;
    this.indexes = indexes;
  }

  @Override
  public int getArity() {
    return indexes.length;
  }

  @Override
  public RowKind getRowKind() {
    return rowData.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    rowData.setRowKind(kind);
  }

  @Override
  public boolean isNullAt(int pos) {
    return rowData.isNullAt(indexes[pos]);
  }

  @Override
  public boolean getBoolean(int pos) {
    return rowData.getBoolean(indexes[pos]);
  }

  @Override
  public byte getByte(int pos) {
    return rowData.getByte(indexes[pos]);
  }

  @Override
  public short getShort(int pos) {
    return rowData.getShort(indexes[pos]);
  }

  @Override
  public int getInt(int pos) {
    return rowData.getInt(indexes[pos]);
  }

  @Override
  public long getLong(int pos) {
    return rowData.getLong(indexes[pos]);
  }

  @Override
  public float getFloat(int pos) {
    return rowData.getFloat(indexes[pos]);
  }

  @Override
  public double getDouble(int pos) {
    return rowData.getDouble(indexes[pos]);
  }

  @Override
  public StringData getString(int pos) {
    return rowData.getString(indexes[pos]);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return rowData.getDecimal(indexes[pos], precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return rowData.getTimestamp(indexes[pos], precision);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    return rowData.getRawValue(indexes[pos]);
  }

  @Override
  public byte[] getBinary(int pos) {
    return rowData.getBinary(indexes[pos]);
  }

  @Override
  public ArrayData getArray(int pos) {
    return rowData.getArray(indexes[pos]);
  }

  @Override
  public MapData getMap(int pos) {
    return rowData.getMap(indexes[pos]);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    return rowData.getRow(indexes[pos], numFields);
  }
}
