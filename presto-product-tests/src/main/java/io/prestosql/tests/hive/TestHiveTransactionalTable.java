/*
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
package io.prestosql.tests.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.tempto.query.QueryResult;
import io.prestosql.tests.hive.util.TemporaryHiveTable;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.testing.assertions.Assert.assertEventually;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.hive.TestHiveTransactionalTable.CompactionMode.MAJOR;
import static io.prestosql.tests.hive.TestHiveTransactionalTable.CompactionMode.MINOR;
import static io.prestosql.tests.hive.TransactionalTableType.ACID;
import static io.prestosql.tests.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.testng.Assert.assertEquals;

public class TestHiveTransactionalTable
        extends HiveProductTest
{
    private static final Logger log = Logger.get(TestHiveTransactionalTable.class);

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, timeOut = 60 * 60 * 1000)
    public void runFullAcid100Times()
    {
        for (int i = 0; i < 100; i++) {
            readFullAcid();
        }
    }

    public void readFullAcid()
    {
        boolean isPartitioned = true;
        BucketingType bucketingType = BucketingType.NONE;

        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName("read_full_acid", isPartitioned, bucketingType))) {
            String tableName = table.getName();
            onHive().executeQuery("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("fcol", 4) + " " +
                    "STORED AS ORC " +
                    hiveTableProperties(ACID, bucketingType));

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            executeHiveQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");

            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 1));

            executeHiveQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(21, 1), row(22, 2));

            // test filtering
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            // test minor compacted data read
            executeHiveQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (20, 3)");
            compactTableAndWait(MINOR, tableName, hivePartitionString, Duration.valueOf("2m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(21, 1), row(22, 2));

            // delete a row
            executeHiveQuery("DELETE FROM " + tableName + " WHERE fcol=2");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(21, 1));

            // update the existing row
            String predicate = "fcol = 1" + (isPartitioned ? " AND part_col = 2 " : "");
            executeHiveQuery("UPDATE " + tableName + " SET col = 23 WHERE " + predicate);
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(23, 1));

            // test major compaction
            compactTableAndWait(MAJOR, tableName, hivePartitionString, Duration.valueOf("2m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(23, 1));
        }
    }

    @DataProvider
    public Object[][] partitioningAndBucketingTypeDataProvider()
    {
        return Stream.of(BucketingType.values())
                .flatMap(value -> Stream.of(
                        new Object[] {false, value},
                        new Object[] {true, value}))
                .toArray(Object[][]::new);
    }

    private static String hiveTableProperties(TransactionalTableType transactionalTableType, BucketingType bucketingType)
    {
        ImmutableList.Builder<String> tableProperties = ImmutableList.builder();
        tableProperties.addAll(transactionalTableType.getHiveTableProperties());
        tableProperties.addAll(bucketingType.getHiveTableProperties());
        tableProperties.add("'NO_AUTO_COMPACTION'='true'");
        return tableProperties.build().stream().collect(joining(",", "TBLPROPERTIES (", ")"));
    }

    private static QueryResult executeHiveQuery(String query)
    {
        log.info("Executing hive query: " + query);
        QueryResult queryResult = onHive().executeQuery(query);
        log.info("Executed hive query: " + query + ", rows: " + queryResult.rows());

        return queryResult;
    }

    private static void compactTableAndWait(CompactionMode compactMode, String tableName, String partitionString, Duration timeout)
    {
        assertEquals(getTableCompactions(compactMode, tableName).count(), 0);
        executeHiveQuery(format("ALTER TABLE %s %s COMPACT '%s'", tableName, partitionString, compactMode.name())).getRowsCount();

        log.info("Started compaction on table state: " + tableName);

        // Since we disabled table auto compaction and we checked that there are no compaction to the table
        // we can assume that every compaction from now on is triggered in this test
        // and all compaction should complete successfully before proceeding.
        assertEventually(timeout, () -> {
            Map<String, String> compaction = getOnlyElement(getTableCompactions(compactMode, tableName)
                    .collect(toImmutableList()));

            log.info("Compaction state: " + compaction);

            verify(!compaction.get("state").equals("failed"), "compaction has failed");
            assertEquals(compaction.get("state"), "succeeded");
        });
    }

    private static Stream<Map<String, String>> getTableCompactions(CompactionMode compactionMode, String tableName)
    {
        return Stream.of(onHive().executeQuery("SHOW COMPACTIONS")).flatMap(TestHiveTransactionalTable::mapRows)
                .filter(row -> isCompactionForTable(compactionMode, tableName, row));
    }

    private static Stream<Map<String, String>> mapRows(QueryResult result)
    {
        if (result.getRowsCount() == 0) {
            return Stream.of();
        }

        List<?> columnNames = result.row(0).stream()
                .filter(Objects::nonNull)
                .collect(toUnmodifiableList());

        ImmutableList.Builder<Map<String, String>> rows = ImmutableList.builder();
        for (int rowIndex = 1; rowIndex < result.getRowsCount(); rowIndex++) {
            ImmutableMap.Builder<String, String> singleRow = ImmutableMap.builder();
            List<?> row = result.row(rowIndex);

            for (int column = 0; column < columnNames.size(); column++) {
                String columnName = ((String) columnNames.get(column)).toLowerCase(ENGLISH);
                singleRow.put(columnName, (String) row.get(column));
            }

            rows.add(singleRow.build());
        }

        return rows.build().stream();
    }

    private static String tableName(String testName, boolean isPartitioned, BucketingType bucketingType)
    {
        return format("test_%s_%b_%s_%s", testName, isPartitioned, bucketingType.name(), randomTableSuffix());
    }

    private static boolean isCompactionForTable(CompactionMode compactMode, String tableName, Map<String, String> row)
    {
        return row.get("table").equals(tableName.toLowerCase(ENGLISH)) &&
                row.get("type").equals(compactMode.name());
    }

    public enum CompactionMode {
        MAJOR,
        MINOR,
        /**/;
    }
}
