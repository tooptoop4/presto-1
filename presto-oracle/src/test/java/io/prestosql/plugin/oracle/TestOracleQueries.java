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
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static io.prestosql.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestOracleQueries
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
    public void testSelectVarcharColumn()
            throws SQLException
    {
        assertUpdate("CREATE TABLE test_varchar_column (col varchar(10))");
        assertUpdate("INSERT INTO test_varchar_column (col) VALUES ('test')", 1);

        assertTrue(getQueryRunner().tableExists(getSession(), "test_varchar_column"));
        assertQuerySucceeds("select count(1) from test_varchar_column");
        assertQuery("SELECT * FROM test_varchar_column", "VALUES ('test')");

        assertUpdate("DROP TABLE test_varchar_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_varchar_column"));
    }

    @Test
    public void testSelectNumberColumn()
            throws SQLException
    {
        assertUpdate("CREATE TABLE test_number_column (col DECIMAL(5,2))");
        assertUpdate("INSERT INTO test_number_column (col) VALUES (123)", 1);

        assertTrue(getQueryRunner().tableExists(getSession(), "test_number_column"));
        assertQuerySucceeds("select count(1) from test_number_column");
        assertQuery("SELECT * FROM test_number_column", "VALUES (123)");

        assertUpdate("DROP TABLE test_number_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_number_column"));
    }

    @Test
    public void testCreateAsSelect()
            throws SQLException
    {
        assertUpdate("CREATE TABLE test_create_select (col DECIMAL(5,2))");
        assertUpdate("INSERT INTO test_create_select (col) VALUES (123)", 1);

        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_select"));
        assertQuerySucceeds("select count(1) from test_create_select");
        assertQuery("SELECT * FROM test_create_select", "VALUES (123)");

        assertUpdate("CREATE TABLE test_ctas AS SELECT * FROM test_create_select", 1);
        //assertQuery("SELECT * FROM test_ctas", "VALUES (123)");
    }

    @Test
    public void testSelectClobColumn()
            throws SQLException
    {
        oracleServer.execute("CREATE TABLE test_clob_column (col CLOB)");
        assertUpdate("INSERT INTO test_clob_column (col) VALUES ('clob')", 1);

        assertTrue(getQueryRunner().tableExists(getSession(), "test_clob_column"));
        assertQuerySucceeds("select count(1) from test_clob_column");
        assertQuery("SELECT * FROM test_clob_column", "VALUES ('clob')");

        assertUpdate("DROP TABLE test_clob_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_clob_column"));
    }

    @Test
    public void testSelectBigintColumn()
            throws SQLException
    {
        assertUpdate("CREATE TABLE test_bigint_column (col BIGINT)");
        assertUpdate("INSERT INTO test_bigint_column (col) VALUES (1)", 1);

        assertTrue(getQueryRunner().tableExists(getSession(), "test_bigint_column"));
        assertQuerySucceeds("select count(1) from test_bigint_column");
        assertQuery("SELECT * FROM test_bigint_column", "VALUES (1)");

        assertUpdate("DROP TABLE test_bigint_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_bigint_column"));
    }

    @Test
    public void testSelect1()
            throws SQLException
    {
        assertQuerySucceeds("select 1");
    }
}
