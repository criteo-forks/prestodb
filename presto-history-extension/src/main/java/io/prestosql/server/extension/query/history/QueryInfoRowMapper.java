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
package io.prestosql.server.extension.query.history;

import io.prestosql.execution.QueryInfo;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.io.IOException;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryInfoRowMapper
        implements RowMapper<QueryInfo>
{
    @Override
    public QueryInfo map(ResultSet rs, StatementContext ctx) throws SQLException
    {
        Clob jsonText = rs.getClob("query_info");
        try {
            return QueryHistorySQLStore.deserializeQueryInfo(jsonText.getCharacterStream());
        }
        catch (IOException e) {
            throw new SQLException("Failed to parse query_info text", e);
        }
    }
}
