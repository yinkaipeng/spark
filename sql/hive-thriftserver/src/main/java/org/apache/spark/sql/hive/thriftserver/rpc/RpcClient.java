package org.apache.spark.sql.hive.thriftserver.rpc;

import com.cloudera.livy.JobHandle;
import com.cloudera.livy.rsc.RSCClient;
import org.apache.hive.service.cli.RowBasedSet;
import org.apache.hive.service.cli.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class RpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(RSCClient.class);
    private RSCClient rscClient;

    public RpcClient(RSCClient rscClient) throws NoSuchMethodException {
        this.rscClient = rscClient;
    }

    public JobHandle<?> executeSql(String statement, String statementId) throws Exception {
        LOG.info("RSC client is executing SQL query: " + statement);
        return rscClient.submit(RemoteDriver.createSqlStatementRequest(statement, statementId));
    }

    public JobHandle<RowBasedSet> fetchResult(int maxRows) throws Exception {
        LOG.info("RSC client is fetching result");
        return rscClient.submit(RemoteDriver.createFetchQueryOutputRequest(maxRows));
    }

    public JobHandle<TableSchema> fetchResultSchema() throws Exception {
        LOG.info("RSC client is fetching result schema");
        return rscClient.submit(RemoteDriver.createFetchResultSchemaRequest());
    }

    public JobHandle<?> cancelStatement(String statementId) throws Exception {
        LOG.info("RSC client is canceling SQL query: " + statementId);
        return rscClient.submit(RemoteDriver.createCancelStatementRequest(statementId));
    }

    public JobHandle<?> closeOperation() throws Exception {
        return rscClient.submit(RemoteDriver.createCloseOperationRequest());
    }

    public void stop(boolean shutdownContext) {
        rscClient.stop(shutdownContext);
    }
}
