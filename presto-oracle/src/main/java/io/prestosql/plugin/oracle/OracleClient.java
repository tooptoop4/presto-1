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

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Collections.addAll;

public class OracleClient
        extends BaseJdbcClient
{
    private final int fetchSize;
    private final String[] tableTypes;

    @Inject
    public OracleClient(
            BaseJdbcConfig config,
            OracleConfig oracleConfig,
            ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);

        this.fetchSize = oracleConfig.getFetchSize();

        Set<String> tableTypes = new HashSet<>();
        addAll(tableTypes, "TABLE", "VIEW");
        if (oracleConfig.isIncludeSynonyms()) {
            addAll(tableTypes, "SYNONYM");
        }
        this.tableTypes = tableTypes.toArray(new String[0]);
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                tableTypes.clone());
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            case Types.CLOB:
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
            case Types.NUMERIC:
                int precision = columnSize == 0 ? 38 : columnSize;
                int scale = max(typeHandle.getDecimalDigits(), 0);
                return Optional.of(decimalColumnMapping(createDecimalType(precision, scale), RoundingMode.HALF_UP));
            case Types.LONGVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == 0) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
        }
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    @Override
    protected String generateTemporaryTableName()
    {
        return "presto_tmp_" + System.nanoTime();
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equalsIgnoreCase(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported in Oracle");
        }

        String sql = format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, schemaName, tableName),
                quoted(newTable.getTableName()));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof BigintType) {
            return WriteMapping.longMapping("number(19)", bigintWriteFunction());
        }
        return super.toWriteMapping(session, type);
    }
}
