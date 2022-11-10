/*
 * Copyright 2022 TiDB Project Authors.
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

package io.tidb.bigdata.flink.tidb.source;

import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TIKVSourceTest extends FlinkTestBase {

  public static final String CREATE_DATAGEN_TABLE_SQL =
      "CREATE TABLE datagen (\n"
          + " c1 tinyint,\n"
          + " c2 string,\n"
          + " proctime as PROCTIME()\n"
          + ") WITH (\n"
          + " 'connector' = 'datagen',\n"
          + " 'rows-per-second'='10',\n"
          + " 'fields.c1.kind'='random',\n"
          + " 'fields.c1.min'='1',\n"
          + " 'fields.c1.max'='10',\n"
          + " 'number-of-rows'='10'\n"
          + ")";
  public static final String TEST_SINK =
      "CREATE TABLE test_sink (\n"
          + " id int,\n"
          + " PRIMARY KEY (id) NOT ENFORCED,\n"
          + " name string,\n"
          + " nn int \n"
          + ") WITH (\n"
          + " 'connector' = 'tidb',\n"
          + " 'tidb.database.url' = 'jdbc:mysql://127.0.0.1:4000/test?serverTimezone=Asia/Shanghai&tinyInt1isBit=false',\n"
          + " 'tidb.database.name' = 'test',\n"
          + " 'tidb.table.name' = 'test_sink',\n"
          + " 'tidb.username' = 'root',\n"
          + " 'tidb.password' = ''\n"
          + ")";

  //  @Test
  //  public void testSnapshotRead() throws Exception {
  //    for (int i = 1; i <= 3; i++) {
  //      // insert
  //      Map<String, String> properties = ConfigUtils.defaultProperties();
  //      ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
  //      String tableName = RandomUtils.randomString();
  //      clientSession.sqlUpdate(
  //          String.format("CREATE TABLE `%s`.`%s` (`c1` int,`c2` int)", DATABASE_NAME, tableName),
  //          String.format("INSERT INTO `%s`.`%s` VALUES(1,1)", DATABASE_NAME, tableName));
  //
  //      if (i == 1) {
  //        // get timestamp
  //        properties.put(ClientConfig.SNAPSHOT_TIMESTAMP,
  //            ZonedDateTime.now().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
  //        // wait for 1 second, because we use client time rather than server time
  //        Thread.sleep(1000L);
  //      } else {
  //        // get version
  //        long version = clientSession.getSnapshotVersion().getVersion();
  //        properties.put(ClientConfig.SNAPSHOT_VERSION, Long.toString(version));
  //      }
  //
  //      // update
  //      clientSession.sqlUpdate(
  //          String.format("UPDATE `%s`.`%s` SET c1 = 2 WHERE c1 =1", DATABASE_NAME, tableName));
  //
  //      if (i == 3) {
  //        // get timestamp
  //        ZonedDateTime zonedDateTime = ZonedDateTime.now();
  //        properties.put(ClientConfig.SNAPSHOT_TIMESTAMP,
  //            zonedDateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
  //      }
  //
  //      // read by version
  //      TableEnvironment tableEnvironment = getTableEnvironment();
  //      properties.put("type", "tidb");
  //      String createCatalogSql = format("CREATE CATALOG `tidb` WITH ( %s )",
  //          TableUtils.toSqlProperties(properties));
  //      tableEnvironment.executeSql(createCatalogSql);
  //      String queryTableSql = format("SELECT * FROM `%s`.`%s`.`%s` where c1>0", "tidb",
  // DATABASE_NAME,
  //          tableName);
  //      CloseableIterator<Row> iterator = tableEnvironment.executeSql(queryTableSql).collect();
  //      while (iterator.hasNext()) {
  //        Row row = iterator.next();
  //        Assert.assertEquals(Row.of(1, 1), row);
  //      }
  //      iterator.close();
  //    }
  //  }
  //
  //  @Test
  //  public void testLookupTableSource() throws Exception {
  //    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
  //    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  //    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
  //    Map<String, String> properties = defaultProperties();
  //    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
  //    tiDBCatalog.open();
  //    String tableName = RandomUtils.randomString();
  //    String createTableSql1 = String.format(
  //        "CREATE TABLE `%s`.`%s` (c1 tinyint, c2 varchar(255), PRIMARY KEY(`c1`))",
  // DATABASE_NAME,
  //        tableName);
  //    String insertDataSql = String.format(
  //        "INSERT INTO `%s`.`%s` VALUES (1,'data1'),(2,'data2'),(3,'data3'),(4,'data4')",
  //        DATABASE_NAME, tableName);
  //    tiDBCatalog.sqlUpdate(createTableSql1, insertDataSql);
  //    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
  //    tableEnvironment.executeSql(CREATE_DATAGEN_TABLE_SQL);
  //    String sql = String.format("SELECT * FROM `datagen` "
  //        + "LEFT JOIN `%s`.`%s`.`%s` FOR SYSTEM_TIME AS OF datagen.proctime AS `dim_table` "
  //        + "ON datagen.c1 = dim_table.c1 ", "tidb", DATABASE_NAME, tableName);
  //    CloseableIterator<Row> iterator = tableEnvironment.executeSql(sql).collect();
  //    while (iterator.hasNext()) {
  //      Row row = iterator.next();
  //      Object c1 = row.getField(0);
  //      String c2 = String.format("data%s", c1);
  //      boolean isJoin = (int) c1 <= 4;
  //      Row row1 = Row.of(c1, row.getField(1), isJoin ? c1 : null, isJoin ? c2 : null);
  //      Assert.assertEquals(row, row1);
  //    }
  //  }
  //
  //  @Test
  //  public void testAsyncLookupTableSource() throws Exception {
  //    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
  //    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  //    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
  //    Map<String, String> properties = defaultProperties();
  //    properties.put("tidb.lookup.async", "true");
  //    properties.put("jdbc.source.flag", "true");
  //    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
  //    tiDBCatalog.open();
  //    String tableName = RandomUtils.randomString();
  //    String createTableSql1 = String.format(
  //        "CREATE TABLE `%s`.`%s` (c1 tinyint, tinyint_test tinyint,smallint_test
  // smallint,bigint_test bigint,date_test date,datetime_test datetime,varchar_test
  // varchar(36),decimal_test decimal(20,2),double_test double, PRIMARY KEY(`c1`))",
  //        DATABASE_NAME,
  //        tableName);
  //    String insertDataSql = String.format(
  //        "INSERT INTO `%s`.`%s` VALUES
  // (1,null,3,2033,'2022-4-20','2022-4-20','test',100.032,100.032),"
  //            + "(2,null,3,2033,'2022-4-20','2022-4-20','test',100.032,100.032)",
  //        DATABASE_NAME, tableName);
  //    tiDBCatalog.sqlUpdate(createTableSql1, insertDataSql);
  //    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
  //    tableEnvironment.executeSql(CREATE_DATAGEN_TABLE_SQL);
  //    String sql = String.format("SELECT * FROM `datagen` "
  //        + "LEFT JOIN `%s`.`%s`.`%s` FOR SYSTEM_TIME AS OF datagen.proctime AS `dim_table` "
  //        + "ON datagen.c1 = dim_table.c1 ", "tidb", DATABASE_NAME, tableName);
  //    CloseableIterator<Row> iterator = tableEnvironment.executeSql(sql).collect();
  //    while (iterator.hasNext()) {
  //      Row row = iterator.next();
  //      Byte c1 = (Byte) row.getField(0);
  //      boolean isJoin = c1.intValue() <= 2;
  //      Row row1 = Row.of(c1, row.getField(1), isJoin ? c1 : null,  null,isJoin ? 3 : null, isJoin
  // ? 2033 : null,
  //          isJoin ? "2022-04-20" : null,
  //          isJoin ? "2022-04-20T00:00" : null, isJoin ? "test" : null, isJoin ? 100.03 : null,
  //          isJoin ? 100.032 : null);
  //      Assert.assertEquals(row.toString(), row1.toString());
  //    }
  //  }
  //
  //  @Test
  //  public void testJdbcTableSource() throws Exception {
  //    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
  //    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  //    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
  //    Map<String, String> properties = defaultProperties();
  //    properties.put("jdbc.source.flag", "true");
  //    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
  //    tiDBCatalog.open();
  //    String tableName = RandomUtils.randomString();
  //    String createTableSql1 = String.format(
  //        "CREATE TABLE `%s`.`%s` (c1 int, c2 varchar(255),c3 datetime, PRIMARY KEY(`c1`))",
  // DATABASE_NAME,
  //        tableName);
  //    String insertDataSql = String.format(
  //        "INSERT INTO `%s`.`%s` VALUES (1,'data1','2022-4-1')",
  //        DATABASE_NAME, tableName);
  //    tiDBCatalog.sqlUpdate(createTableSql1, insertDataSql);
  //    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
  //    tableEnvironment.executeSql(CREATE_DATAGEN_TABLE_SQL);
  //    CloseableIterator<Row> iterator = tableEnvironment.executeSql(
  //        String.format("select c1,c2 from `%s`.`%s`.`%s`", "tidb", DATABASE_NAME,
  // tableName)).collect();
  //    while (iterator.hasNext()) {
  //      Row row = iterator.next();
  //      Assert.assertEquals(row, Row.of(1, "data1"));
  //    }
  //  }
  @Test
  public void testJdbcSink() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    tableEnvironment.executeSql(CREATE_DATAGEN_TABLE_SQL);
    DataStream<Row> dataStream =
        env.fromElements(
            Row.ofKind(RowKind.INSERT, 1, "Alice"),
            Row.ofKind(RowKind.INSERT, 2, "Bob"),
            Row.ofKind(RowKind.UPDATE_AFTER, 1, "AliceNew"),
            Row.ofKind(RowKind.DELETE, 1, "Alice"));
    Table table = tableEnvironment.fromChangelogStream(dataStream);
    tableEnvironment.createTemporaryView("InputTable", table);
    tableEnvironment.executeSql(TEST_SINK);
    tableEnvironment
        .executeSql(
            "insert into test_sink /*+ OPTIONS('sink.parallelism'='2') */   select a.*,0 from InputTable a ")
        .print();
  }

  @Test
  public void testJdbcConnection() throws Exception {}

  @Test
  public void testArrayList() throws Exception {
    CompletableFuture<Integer> a = new CompletableFuture<>();
    a.thenAcceptAsync(
        x -> {
          try {
            Thread.sleep(1000);
            System.out.println(x);

          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
    Random random = new Random();
    for (int i = 0; i < 20; i++) {
      a.complete(random.nextInt());
    }
    System.out.println("start job");
    Thread.sleep(100000);
  }
}
