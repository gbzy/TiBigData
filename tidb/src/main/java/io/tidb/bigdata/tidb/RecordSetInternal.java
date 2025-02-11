/*
 * Copyright 2020 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.tidb;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import io.tidb.bigdata.tidb.key.Base64KeyRange;
import io.tidb.bigdata.tidb.meta.TiDAGRequest;
import io.tidb.bigdata.tidb.operation.iterator.CoprocessorIterator;
import io.tidb.bigdata.tidb.row.Row;
import io.tidb.bigdata.tidb.types.DataType;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.tikv.common.meta.TiTimestamp;

public final class RecordSetInternal {

  private final List<ColumnHandleInternal> columnHandles;
  private final List<DataType> columnTypes;
  private final CoprocessorIterator<Row> iterator;

  public RecordSetInternal(
      ClientSession session,
      SplitInternal split,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp) {
    this(session, ImmutableList.of(split), columnHandles, expression, timestamp, Optional.empty());
  }

  public RecordSetInternal(
      ClientSession session,
      List<SplitInternal> splits,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp) {
    this(session, splits, columnHandles, expression, timestamp, Optional.empty());
  }

  public RecordSetInternal(
      ClientSession session,
      SplitInternal split,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp,
      Optional<Integer> limit) {
    this(session, ImmutableList.of(split), columnHandles, expression, timestamp, limit);
  }

  public RecordSetInternal(
      ClientSession session,
      List<SplitInternal> splits,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp,
      Optional<Integer> limit) {
    checkSplits(splits);
    this.columnHandles = requireNonNull(columnHandles, "ColumnHandles can not be null");
    this.columnTypes =
        columnHandles.stream().map(ColumnHandleInternal::getType).collect(toImmutableList());
    List<String> columns =
        columnHandles.stream().map(ColumnHandleInternal::getName).collect(toImmutableList());
    SplitInternal split = splits.get(0);
    TiDAGRequest.Builder request = session.request(split.getTable(), columns);
    limit.ifPresent(request::setLimit);
    expression.ifPresent(request::addFilter);
    request.setStartTs(split.getTimestamp());
    // snapshot read
    timestamp.ifPresent(request::setStartTs);
    List<Base64KeyRange> ranges =
        splits.stream()
            .map(
                splitInternal ->
                    new Base64KeyRange(splitInternal.getStartKey(), splitInternal.getEndKey()))
            .collect(Collectors.toList());
    this.iterator = session.iterate(request, ranges);
  }

  private void checkSplits(Collection<SplitInternal> splits) {
    Preconditions.checkArgument(
        splits != null && splits.size() >= 1, "Splits can not be empty or null");
    Preconditions.checkArgument(
        splits.stream().map(SplitInternal::getTimestamp).distinct().count() == 1,
        "Timestamp for splits must be equals");
    Preconditions.checkArgument(
        splits.stream().map(split -> split.getTable().getSchemaTableName()).distinct().count() == 1,
        "Table for splits must be equals");
  }

  public List<DataType> getColumnTypes() {
    return columnTypes;
  }

  public RecordCursorInternal cursor() {
    return new RecordCursorInternal(columnHandles, iterator);
  }
}
