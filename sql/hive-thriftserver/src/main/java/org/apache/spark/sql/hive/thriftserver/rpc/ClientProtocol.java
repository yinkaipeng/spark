package org.apache.spark.sql.hive.thriftserver.rpc;

import com.cloudera.livy.rsc.BaseProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientProtocol extends BaseProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ClientProtocol.class);

    public static class SqlStatementRequest {

        public final String statement;
        public final String id;

        public SqlStatementRequest(String statement, String id) {
            this.statement = statement;
            this.id = id;
        }

        public SqlStatementRequest() {
            this(null, null);
        }
    }

    public static class FetchResultSchemaRequest {}

    public static class FetchQueryOutputRequest {

        public final int maxRows;

        public FetchQueryOutputRequest(int maxRows) {
            this.maxRows = maxRows;
        }

        public FetchQueryOutputRequest() {
            this(0);
        }
    }

    public static class CancelStatementRequest {

        public final String id;

        public CancelStatementRequest(String id) {
            this.id = id;
        }

        public CancelStatementRequest() {
            this(null);
        }
    }

    public static class CloseOperationRequest {}
}
