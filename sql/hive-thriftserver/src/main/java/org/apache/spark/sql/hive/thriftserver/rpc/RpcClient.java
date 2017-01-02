package org.apache.spark.sql.hive.thriftserver.rpc;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.rsc.RSCClient;
import org.apache.hive.service.cli.RowBasedSet;
import org.apache.hive.service.cli.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(RSCClient.class);
    private RSCClient rscClient;

    public RpcClient(RSCClient rscClient) throws NoSuchMethodException {
        this.rscClient = rscClient;
    }

    public JobHandle<?> executeSql(String statementId, String statement) throws Exception {
        LOG.info("RSC client is executing SQL query: " + statement + ", statementId = " + statementId);
        if (null == statementId || null == statement) {
            throw new IllegalArgumentException("Invalid statement/statementId specified. " +
                "statement = " + statement + ", statementId = " + statementId);
        }
        return rscClient.submit(RemoteDriver.createSqlStatementRequest(statementId, statement));
    }

    public JobHandle<RowBasedSet> fetchResult(String statementId, int maxRows) throws Exception {
        LOG.info("RSC client is fetching result for statementId = " + statementId);
        if (null == statementId) {
            throw new IllegalArgumentException("Invalid statementId specified. statementId = " +
                statementId);
        }
        return rscClient.submit(RemoteDriver.createFetchQueryOutputRequest(statementId, maxRows));
    }

    public JobHandle<TableSchema> fetchResultSchema(String statementId) throws Exception {
        LOG.info("RSC client is fetching result schema for statementId = " + statementId);
        if (null == statementId) {
            throw new IllegalArgumentException("Invalid statementId specified. statementId = " +
                statementId);
        }
        return rscClient.submit(RemoteDriver.createFetchResultSchemaRequest(statementId));
    }

    public JobHandle<?> cancelStatement(String statementId) throws Exception {
        LOG.info("RSC client is canceling SQL query for statementId = " + statementId);
        if (null == statementId) {
            throw new IllegalArgumentException("Invalid statementId specified. statementId = " +
                statementId);
        }
        return rscClient.submit(RemoteDriver.createCancelStatementRequest(statementId));
    }

    public JobHandle<?> closeOperation(String statementId) throws Exception {
        LOG.info("RSC client is closing operation for statementId = " + statementId);
        return rscClient.submit(RemoteDriver.createCloseOperationRequest(statementId));
    }

    public void stop(boolean shutdownContext) {
        rscClient.stop(shutdownContext);
    }

    public boolean isClosed() {
        return ! rscClient.isAlive();
    }
}
