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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import io.prestosql.execution.QueryInfo;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.BasicQueryStats;
import io.prestosql.spi.QueryId;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.sql.DataSource;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Using RDBMS to store/read query history. It should support most jdbc drivers.
 */
public class QueryHistorySQLStore
        implements QueryHistoryStore
{
    private static final Logger LOG = Logger.get(QueryHistorySQLStore.class);
    private static final ObjectMapper queryJsonParser;

    // All jdbc connection properties should be put under this namesapce, thus `jdbcUrl` should be `sql.jdbcUrl`.
    public static final String SQL_CONFIG_PREFIX = "sql.";
    public static final String PRESTO_CLUSTER_KEY = "presto.cluster";

    static {
        queryJsonParser = new ObjectMapper();
        queryJsonParser.registerModule(new Jdk8Module());
        queryJsonParser.registerModule(new JavaTimeModule());
        queryJsonParser.registerModule(new JodaModule());
        queryJsonParser.registerModule(new GuavaModule());
        queryJsonParser.registerModule(new io.prestosql.server.extension.query.history.PrestoQueryInfoModule());
        queryJsonParser.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        queryJsonParser.enableDefaultTyping();
    }

    private Properties config;
    private String cluster;
    private DataSource dataSource;
    private QueryHistoryDAO queryHistoryDAO;

    @Override
    public void init(Properties props)
    {
        config = props;
        cluster = config.getProperty(PRESTO_CLUSTER_KEY);
        requireNonNull(cluster, "You should define presto.cluster in your properties file.");
        dataSource = createDataSource(config);
        queryHistoryDAO = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin()).onDemand(QueryHistoryDAO.class);
    }

    private DataSource createDataSource(Properties config)
    {
        // Take all the sql configs to build a data source.
        Properties sqlConfig = new Properties();
        for (Map.Entry<Object, Object> entry : config.entrySet()) {
            if (entry.getKey().toString().startsWith(SQL_CONFIG_PREFIX)) {
                LOG.debug("History extension jdbc config: %s -> %s", entry.getKey(), entry.getValue());
                sqlConfig.put(entry.getKey().toString().substring(SQL_CONFIG_PREFIX.length()), entry.getValue());
            }
        }
        return new HikariDataSource(new HikariConfig(sqlConfig));
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
    {
        try {
            return queryHistoryDAO.getQueryInfoByQueryId(queryId.getId());
        }
        catch (Exception e) {
            LOG.error("SQL error while getting query " + queryId, e);
            throw new NoSuchElementException("Cannot get query for " + queryId);
        }
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo(QueryId queryId)
    {
        QueryInfo fullQueryInfo = getFullQueryInfo(queryId);
        if (fullQueryInfo == null) {
            throw new NoSuchElementException("Cannot find QueryInfo from db: " + queryId);
        }
        return new BasicQueryInfo(fullQueryInfo.getQueryId(), fullQueryInfo.getSession(),
                fullQueryInfo.getResourceGroupId(), fullQueryInfo.getState(), fullQueryInfo.getMemoryPool(),
                fullQueryInfo.isScheduled(), fullQueryInfo.getSelf(), fullQueryInfo.getQuery(),
                new BasicQueryStats(fullQueryInfo.getQueryStats()), fullQueryInfo.getErrorType(),
                fullQueryInfo.getErrorCode());
    }

    @Override
    public void saveFullQueryInfo(QueryInfo queryInfo)
    {
        saveQueryHistory(queryInfo);
    }

    private boolean saveQueryHistory(QueryInfo queryInfo)
    {
        try {
            io.prestosql.server.extension.query.history.QueryHistory queryHistory = new io.prestosql.server.extension.query.history.QueryHistory(queryInfo, getCluster());
            queryHistoryDAO.insertQueryHistory(queryHistory);
            return true;
        }
        catch (Exception e) {
            LOG.error("Faield to save " + queryInfo, e);
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
        if (dataSource instanceof Closeable) {
            ((Closeable) dataSource).close();
        }
    }

    /**
     * This is to be used only in test. It creates the table without the column compression attribute (this feature not yet available for tests).
     */
    @VisibleForTesting
    void createTable()
    {
        // Try to create the table if it does not exist.
        queryHistoryDAO.createQueryHistoryTable();
    }

    private String getCluster()
    {
        return cluster;
    }

    public static String serializeQueryInfo(QueryInfo queryInfo) throws IOException
    {
        return queryJsonParser.writeValueAsString(queryInfo);
    }

    public static QueryInfo deserializeQueryInfo(String json) throws IOException
    {
        return queryJsonParser.readValue(json, QueryInfo.class);
    }

    public static QueryInfo deserializeQueryInfo(InputStream inputStream) throws IOException
    {
        return queryJsonParser.readValue(inputStream, QueryInfo.class);
    }

    public static QueryInfo deserializeQueryInfo(Reader reader) throws IOException
    {
        return queryJsonParser.readValue(reader, QueryInfo.class);
    }
}
