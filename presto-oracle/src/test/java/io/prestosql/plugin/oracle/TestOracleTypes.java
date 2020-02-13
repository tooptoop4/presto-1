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
package io.prestosql.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.assertions.Assert;
import org.testcontainers.shaded.org.apache.commons.lang.ObjectUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static io.prestosql.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOracleTypes
        extends AbstractTestQueryFramework
{
    private OracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.oracleServer = new OracleServer();
        return createOracleQueryRunner(oracleServer, ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    @Test
    public void testVarcharType()
    {
        new UnderTest()
                .withType("varchar", "(10)")
                .withValues("test")
                .doTest();
        new UnderTest()
                .withType("varchar", null, "(4000)")
                .withValues("test")
                .doTest();
    }

    @Test
    public void testNumericTypes()
    {
        new UnderTest()
                .withType("tinyint")
                .withValues(123, (byte) 123)
                .doTest();
        new UnderTest()
                .withType("smallint")
                .withValues(123, (short) 123)
                .doTest();
        new UnderTest()
                .withType("integer")
                .withValues(123, 123)
                .doTest();
        new UnderTest()
                .withType("bigint")
                .withValues(123, 123L)
                .doTest();
        new UnderTest()
                .withType("decimal", null, "(38,0)")
                .withValues(123, BigDecimal.valueOf(123))
                .doTest();
        new UnderTest()
                .withType("decimal", "(5,1)")
                .withValues(123, BigDecimal.valueOf(123).setScale(1, RoundingMode.HALF_UP))
                .doTest();
        new UnderTest()
                .withType("decimal", "(5,2)")
                .withValues(123, BigDecimal.valueOf(123).setScale(2, RoundingMode.HALF_UP))
                .doTest();
        new UnderTest()
                .withType("decimal", "(5,2)")
                .withValues(123.046, BigDecimal.valueOf(123.05).setScale(2, RoundingMode.HALF_UP))
                .doTest();
    }

    @Test
    public void testClobColumn()
    {
        oracleServer.execute("CREATE TABLE test_clob_column (col CLOB)");
        assertUpdate("INSERT INTO test_clob_column (col) VALUES ('clob')", 1);

        assertTrue(getQueryRunner().tableExists(getSession(), "test_clob_column"));
        assertQuerySucceeds("select count(1) from test_clob_column");
        assertQuery("SELECT * FROM test_clob_column", "VALUES ('clob')");

        assertUpdate("DROP TABLE test_clob_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_clob_column"));
    }

    private class UnderTest
    {
        private String tableName;
        private String tableType;
        private String tableTypeExpected;
        private String tableValue;
        private Object tableValueExpected;

        protected UnderTest()
        {
        }

        protected UnderTest withType(String type)
        {
            return withType(type, null, null);
        }

        protected UnderTest withType(String type, String precision)
        {
            return withType(type, precision, precision);
        }

        protected UnderTest withType(String type, String precision, String precisionExpected)
        {
            this.tableName = format("test_type_%s", type);
            this.tableType = format("%s%s", type, ObjectUtils.defaultIfNull(precision, ""));
            this.tableTypeExpected = format("%s%s", type, ObjectUtils.defaultIfNull(precisionExpected, ""));
            return this;
        }

        protected UnderTest withValues(Object value)
        {
            return withValues(value, value);
        }

        protected UnderTest withValues(Object value, Object valueExpected)
        {
            this.tableValue = (value instanceof String) ? format("'%s'", value) : value.toString();
            this.tableValueExpected = valueExpected;
            return this;
        }

        private void doTest()
        {
            assertUpdate(format("CREATE TABLE %s (col %s)", tableName, tableType));
            assertTrue(getQueryRunner().tableExists(getSession(), tableName));

            assertUpdate(format("INSERT INTO %s (col) VALUES(%s)", tableName, tableValue), 1);
            assertQuerySucceeds(format("select count(1) from %s", tableName));
            assertQuery(format("SELECT * FROM %s", tableName), format("VALUES(%s)", tableValue));

            MaterializedResult materializedRows = computeActual(format("SELECT col FROM %s", tableName));
            assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), tableValueExpected);

            MaterializedResult actualColumns = computeActual(format("DESC %s", tableName)).toTestTypes();

            MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                    .row("col", tableTypeExpected, "", "")
                    .build();
            Assert.assertEquals(actualColumns, expectedColumns);

            assertUpdate(format("DROP TABLE %s", tableName));
            assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        }
    }
}
