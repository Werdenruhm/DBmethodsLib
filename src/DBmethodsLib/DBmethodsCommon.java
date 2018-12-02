package DBmethodsLib;

//import CommonLib.DateTime; 
import CommonLib.Common;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
import org.postgresql.util.PGobject;

/**
 *
 * 
 */
public abstract class DBmethodsCommon implements DBaggregator {
    private String dB_URL;
    protected String uSER;
    protected String pASS;
    private final int maxPoolSize;
    public String getDB_URL()
    {
        if (dB_URL == null)
            throw new RuntimeException("MUSTNEVERTHROW: Auth data was not set!"); 
        return dB_URL;
    }
    protected DBmethodsCommon() { 
        this.maxPoolSize = 0; 
    }
    protected DBmethodsCommon(String DB_URL, String USER, String PASS)
    {
        this(DB_URL, USER, PASS, 0);
    }
    protected DBmethodsCommon(String DB_URL, String USER, String PASS, int maxPoolSize)
    {
        this.dB_URL = DB_URL;
        this.uSER = USER;
        this.pASS = PASS;
        this.maxPoolSize = maxPoolSize > 0 ? maxPoolSize : 0;
    }
    public boolean authSet()
    {
        return dB_URL != null;
    }
    public void setAuth(String DB_URL, String USER, String PASS)
    {
        if (dB_URL != null)
            throw new RuntimeException("MUSTNEVERTHROW: Auth data already set!");
        dB_URL = DB_URL;
        uSER = USER;
        pASS = PASS;
    }
    
    public abstract Connection getNewActiveConnection() throws SQLException;

    protected abstract CallableStatement new_CallableStatement(String sqlApplyingTransacParams, String sql_text, Connection CONN, int QueryTimeout) throws SQLException;
    protected CallableStatement new_CallableStatement_internal(String sql_text, Connection CONN, int QueryTimeout) throws SQLException
    {
        CallableStatement result = CONN.prepareCall(sql_text);
        if (QueryTimeout == -1)
        {
            QueryTimeout = result.getQueryTimeout();
        }
        result.setQueryTimeout(QueryTimeout);
        return result;
    }

    public void select_from(Common.Action1THROWSSPECIFIC<ResultSet, SQLException> onResultSet, boolean pooled, String sql_text) throws SQLException
    {
        select_from(onResultSet, pooled, sql_text, null); 
    }
    public void select_from(Common.Action1THROWSSPECIFIC<ResultSet, SQLException> onResultSet, boolean pooled, String sql_text, SqlParameterCollection params) throws SQLException
    {
        select_from(onResultSet, pooled, sql_text, params, -1, true); 
    }
    public void select_from(Common.Action1THROWSSPECIFIC<ResultSet, SQLException> onResultSet, boolean pooled, String sql_text, SqlParameterCollection params, int QueryTimeout, boolean CI) throws SQLException
    {
        select_from(onResultSet, pooled, sql_text, params, QueryTimeout, CI, null, null); 
    }

    public DataTable[] select_from(boolean pooled, String sql_text) throws SQLException
    {
        return select_from(pooled, sql_text, null); 
    }
    public DataTable[] select_from(boolean pooled, String sql_text, SqlParameterCollection params) throws SQLException
    {
        return select_from(pooled, sql_text, params, -1, true); 
    }
    public DataTable[] select_from(boolean pooled, String sql_text, SqlParameterCollection params, int QueryTimeout, boolean CI) throws SQLException
    {
        return select_from(pooled, sql_text, params, QueryTimeout, CI, null, null); 
    }
    private final ArrayList<RefConnection> pooledConnections = new ArrayList<>();
    private int pooledConnections_inUse()
    {
        int result = 0;
        for (int n = pooledConnections.size() - 1; n >= 0; n--)
        {
            if (pooledConnections.get(n).getInUse())
                result++;
        }
        return result;
    }
    private final Object pooledConnectionsLOCK = new Object();
    private volatile long pooledConnections_lastCallID;
    public long getPooledConnections_lastCallID()
    {
        return pooledConnections_lastCallID;
    }
    private volatile long pooledConnections_awaited;
    private final Object pooledConnections_lastCallIDLOCK = new Object();
    private class Alt_PoolAwaitingThread { volatile boolean isAwaiting; long CallID; long begin; Thread thread; String sql_text; String lastDelays;
        public String toShortString() { return "isAwaiting=" + isAwaiting + "; CallID=" + CallID + "; thread=" + thread.getName() + "; lastDelays=" + lastDelays; } 
        @Override public String toString() { return toShortString() + "; sql_text=" + sql_text; }        
    }
    private final static int alt_awaitingThreadsBuffer_length = 2000;
    private final Alt_PoolAwaitingThread[] alt_awaitingThreadsBuffer = new Alt_PoolAwaitingThread[alt_awaitingThreadsBuffer_length];
    private volatile int alt_awaitingThreadsCurrent = -1;
    private final Object alt_insertInAwaitingThreadsBufferLOCK = new Object();
    Alt_PoolAwaitingThread alt_insertInAwaitingThreadsBuffer(String sql_text)
    {
        Alt_PoolAwaitingThread ins = new Alt_PoolAwaitingThread(); ins.isAwaiting = true; ins.sql_text = sql_text; ins.thread = Thread.currentThread(); ins.begin = System.currentTimeMillis();
        synchronized(alt_insertInAwaitingThreadsBufferLOCK)
        {
            int next = alt_awaitingThreadsCurrent + 1;
            if (next == alt_awaitingThreadsBuffer_length)
                next = 0;      
            Alt_PoolAwaitingThread remove = alt_awaitingThreadsBuffer[next];
            if (remove != null && remove.isAwaiting)
            {
                System.out.println(Common.NowToString() + "  " + System.currentTimeMillis() + "    ; waiting for buffer slot");
                while (remove.isAwaiting)
                    Common.sleep(5);                
                System.out.println(Common.NowToString() + "  " + System.currentTimeMillis() + "    ; got buffer buffer slot");
            }
            alt_awaitingThreadsBuffer[next] = ins;
            alt_awaitingThreadsCurrent = next;
            return ins;
        }
    }    
    private String alt_awaitingThreads_info()
    {
        int[] diffresults = new int[5];
        int[] diffTresholds = { 1, 10, 100, 1000, 10000 };
        long ts = System.currentTimeMillis();
        for (int n = alt_awaitingThreadsBuffer_length - 1; n >= 0; n--)
        {
            Alt_PoolAwaitingThread pat = alt_awaitingThreadsBuffer[n];
            if (pat != null && pat.isAwaiting)
            {
                long diff = ts - pat.begin;
                for(int t = 0; t < diffTresholds.length; t++)
                    if (diff > diffTresholds[t])
                        diffresults[t]++;
            }                
        }
        StringBuilder result = new StringBuilder();
        result.append("(");
        for(int t = 0; t < diffTresholds.length; t++)
            result.append(diffTresholds[t]).append("ms|");
        result.append(") = (");
        for(int t = 0; t < diffTresholds.length; t++)
            result.append(diffresults[t]).append("|");
        result.append(")");
        return result.toString();
    }       
    DataTable[] select_from(boolean pooled, String sql_text, SqlParameterCollection params, int QueryTimeout, boolean CI, String additionalSessionTimeSql, SessionOrTransacParam[] volatileTransacParams) throws SQLException
    {
        ArrayList<DataTable> result = new ArrayList<>();
        select_from((rs) -> { rs2dtal(rs, result); }, pooled, sql_text, params, QueryTimeout, CI, additionalSessionTimeSql, volatileTransacParams);        
        return Common.collectionToArray(result, DataTable[].class);
    }    
    void select_from(Common.Action1THROWSSPECIFIC<ResultSet, SQLException> onResultSet, boolean pooled, String sql_text, SqlParameterCollection params, int QueryTimeout, boolean CI, String additionalSessionTimeSql, SessionOrTransacParam[] volatileTransacParams) throws SQLException
    {
        if (!pooled) 
        {
            RefConnection rc = new RefConnection(this);
            try
            {
                select_from(onResultSet, rc, sql_text, params, QueryTimeout, CI, additionalSessionTimeSql, volatileTransacParams);        
            }
            finally
            {
                rc.closeConnection();
            }            
        }
        else
        {
            Alt_PoolAwaitingThread currentPAT = alt_insertInAwaitingThreadsBuffer(sql_text);
            RefConnection rc = null;
            long CallID;
            synchronized (pooledConnections_lastCallIDLOCK)
            {
                pooledConnections_lastCallID++;
                CallID = pooledConnections_lastCallID;
                pooledConnections_awaited++;
            }
            try
            {
                long tmp;
                synchronized (pooledConnectionsLOCK)
                {
                    while (rc == null)
                    {
                        for (RefConnection cp_rc : pooledConnections)
                        {
                            if (!cp_rc.getInUse())
                            {
                                rc = cp_rc;
                                rc.setInUse(true);
                                break;
                            }
                        }
                        if (rc == null)
                        {
                            if (maxPoolSize <= 0 || pooledConnections.size() < maxPoolSize)
                            {
                                rc = new RefConnection(this);
                                rc.setInUse(true);
                                pooledConnections.add(rc);
                            }
                        }
                        if (rc == null)
                            Thread.sleep(1);
                    }
                    currentPAT.isAwaiting = false;
                    tmp = pooledConnections_awaited - 1;
                }
                synchronized (pooledConnections_lastCallIDLOCK)
                {
                    pooledConnections_awaited--;
                }
                if (IsDbDebug())
                {
                    int inUseDebug = pooledConnections_inUse();
                    long ts = System.currentTimeMillis();
                    System.out.println(Common.NowToString() + "  " + ts + "    ; waited = " + (ts - currentPAT.begin) + "    ; id = " + CallID + "    ; dbmethods pool" + (maxPoolSize <= 0 ? "" : "[" + maxPoolSize + "]") + " in use   :   " + inUseDebug + "   ; pooledConnections_awaited   :   " + tmp + "   ; awaiting_info   :   " + alt_awaitingThreads_info() + (isDbDebugShowSqlAlways ? " sql=" + sql_text : ""));
                }
                
                if (rc.isConnectionOlder(2 * 60))//reconnect if older than 2 min
                    rc.reconnect();
                    
                select_from(onResultSet, rc, sql_text, params, QueryTimeout, CI, additionalSessionTimeSql, volatileTransacParams);        
            }
            catch (InterruptedException ex)
            {
                throw new RuntimeException(ex);
            }
            finally
            {
                currentPAT.isAwaiting = false;
                if (rc != null)
                    rc.setInUse(false);
            }
        }
    }
    static public volatile boolean isDbDebugShowSqlAlways;
    static volatile Boolean isDbDebug;
    public static boolean IsDbDebug()
    {
        if (isDbDebug != null)
            return isDbDebug;
        return Common.IsDebug();
//        if (isDbDebug == null)
//        {
//            String javaargs = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().toLowerCase();
//            isDbDebug = javaargs.contains("-DasIstNetBeans=ja".toLowerCase())                    
//            ;
//        }
//        return isDbDebug;
    }
    public static void setIsDbDebug(Boolean v) { isDbDebug = v; }
    public static class ConnectionID { public String value; public ConnectionID(String value) { this.value = value; } }

    public DataTable[] select_from(ConnectionID connectionID, String sql_text) throws SQLException
    {
        return select_from(connectionID, sql_text, null); 
    }
    public DataTable[] select_from(ConnectionID connectionID, String sql_text, SqlParameterCollection params) throws SQLException
    {
        return select_from(connectionID, sql_text, params, -1, true); 
    }
    DataTable[] select_from(ConnectionID connectionID, String sql_text, SqlParameterCollection params, String additionalSessionTimeSql) throws SQLException
    {
        return select_from(connectionID, sql_text, params, -1, true, additionalSessionTimeSql, null); 
    }
    public DataTable[] select_from(ConnectionID connectionID, String sql_text, SqlParameterCollection params, int QueryTimeout, boolean CI) throws SQLException
    {
        return select_from(connectionID, sql_text, params, QueryTimeout, CI, null, null); 
    }
    HashMap<String, RefConnection> nonPooledConnections = new HashMap<>();
    final Object nonPooledConnectionsLOCK = new Object();
    DataTable[] select_from(ConnectionID connectionID, String sql_text, SqlParameterCollection params, int QueryTimeout, boolean CI, String additionalSessionTimeSql, SessionOrTransacParam[] volatileTransacParams) throws SQLException
    {
        ArrayList<DataTable> result = new ArrayList<>();
        select_from((rs) -> { rs2dtal(rs, result); }, connectionID, sql_text, params, QueryTimeout, CI, additionalSessionTimeSql, volatileTransacParams);        
        return Common.collectionToArray(result, DataTable[].class);
    }
    void select_from(Common.Action1THROWSSPECIFIC<ResultSet, SQLException> onResultSet, ConnectionID connectionID, String sql_text, SqlParameterCollection params, int QueryTimeout, boolean CI, String additionalSessionTimeSql, SessionOrTransacParam[] volatileTransacParams) throws SQLException
    {
        if (connectionID.value.equals("pooled"))
            select_from(onResultSet, true, sql_text, params, QueryTimeout, CI, additionalSessionTimeSql, volatileTransacParams);        
        else
        {
            if (!nonPooledConnections.containsKey(connectionID.value))
                synchronized (nonPooledConnectionsLOCK)
                {
                    if (!nonPooledConnections.containsKey(connectionID.value))
                        nonPooledConnections.put(connectionID.value, new RefConnection(this));
                }
            select_from(onResultSet, nonPooledConnections.get(connectionID.value), sql_text, params, QueryTimeout, CI, additionalSessionTimeSql, volatileTransacParams);        
        }
    }
    void rs2dtal(ResultSet rs, ArrayList<DataTable> result) throws SQLException
    {
        DataTable resDT = new DataTable();
        ResultSetMetaData rsmd = rs.getMetaData();
        for(int n = 1; n <= rsmd.getColumnCount(); n++)
        {
            DataColumn c = new DataColumn();
            c.setColumnName(rsmd.getColumnName(n));
            if (resDT.Columns.containsKey(c.getColumnName()))
                throw new RuntimeException("Resultset has more than one '" + c.getColumnName() + "' column");
            c.DataType = rsmd.getColumnType(n);
            resDT.Columns.put(c.getColumnName(), c);
        }                    
        while (rs.next()) {
            DataRow r = resDT.Rows.add();
            for(int n = 1; n <= rsmd.getColumnCount(); n++)
            {
                String colnm = rsmd.getColumnName(n);
                Object v = rs.getObject(colnm);

                r.put(colnm, givmegoodsqlval(getGoodValue(v)));                    
            }    
        }
        result.add(resDT);
    }
    void select_from(Common.Action1THROWSSPECIFIC<ResultSet, SQLException> onResultSet, RefConnection rcnn, String sql_text, SqlParameterCollection params, int QueryTimeout, boolean CI, String additionalSessionTimeSql, SessionOrTransacParam[] volatileTransacParams) throws SQLException
    {
        int cll;
        if ((cll=sql_text.indexOf("{")) != -1 && (cll=sql_text.indexOf("call ", cll+1)) != -1 && sql_text.indexOf("}", cll+1) != -1)
            throw new RuntimeException("Use execProc method instead of default JDBC function call notation { ?= call ... }");
        try
        {
            rcnn.setInUse(true);
            if (!closingDaemonStarted)
                startClosingDaemon();

            for(int trys=1; true; trys++)
            {
                try
                {
            
            setCSCIorReconnect(rcnn, CI, additionalSessionTimeSql);

            CallableStatement CMD;
            String sqlApplyingTransacParams = null;
            if (constantTransacParams.size() > 0)
            {
                sqlApplyingTransacParams = getSqlApplyingTransacParams(volatileTransacParams);
            }
            if (params != null && params.size() > 0)
            {
                boolean[] codeMask = SqlCodeAnalizer.instance.getCodeMask(sql_text);
                //sort by name length
                HashMap<Long, SqlParameter> nameLenC = new HashMap<>();
                long[] nameLenA = new long[params.size()];
                int n=0;
                for (SqlParameter p: params.values())
                {
                    long k = n + p.ParameterName.length() * 1000;
                    nameLenA[n] = k;
                    nameLenC.put(k, p);
                    n++;
                }
                Arrays.sort(nameLenA);

                HashMap<Integer, SqlParameter> sqlOffsetC = new HashMap<>();
                int[] sqlOffsetA = new int[100];
                int sqlOffsetA_n = 0;
                StringBuilder sqlOffsetSql = new StringBuilder(sql_text);
                for(n = nameLenA.length - 1; n >= 0; n--)
                {
                    SqlParameter p = nameLenC.get(nameLenA[n]);
                    String pn = ":" + p.ParameterName;
                    int i = sqlOffsetSql.indexOf(pn);
                    if (i == -1)
                        throw new RuntimeException("Parameter " + pn + " not found!");
                    while (i != -1)
                    {
                        if (codeMask[i])
                        {                                    
                            sqlOffsetSql.replace(i, i + pn.length(), "?                                                               ".substring(0, pn.length()));
                            sqlOffsetA[sqlOffsetA_n] = i;
                            sqlOffsetC.put(i, p);
                            sqlOffsetA_n++;
                            if (p.Direction == SqlParameter.ParameterDirection.Output && sqlOffsetSql.indexOf(pn) != -1)
                                throw new RuntimeException("parameter " + pn + " is Output, but it occurs more than once! Delete its dubs (even in comments)!");
                        }
                        else                                
                            throw new RuntimeException("Please, remove parameter names from comments and literal constants! parameter name: " + pn + ", position: " + i);
                        i = sqlOffsetSql.indexOf(pn, i+1);
                    }
                }
                sqlOffsetA = Arrays.copyOf(sqlOffsetA, sqlOffsetA_n);
                Arrays.sort(sqlOffsetA);


                CMD = new_CallableStatement(sqlApplyingTransacParams, sqlOffsetSql.toString(), rcnn.getConnection(), QueryTimeout);
                
                for(n = 0; n < sqlOffsetA.length; n++)
                {
                    SqlParameter p = sqlOffsetC.get(sqlOffsetA[n]);
                    if (p.Direction == SqlParameter.ParameterDirection.Input)
                    {
                        CallableStatement_setInParameter(CMD, n+1, p.ParameterName, p.Value, p.DbType);
                    }
                    else
                    {
                        p.ParameterIndex = n+1;
                        CallableStatement_setOutParameter(CMD, n+1, p.ParameterName, p.DbType);
                    }
                }
            }
            else
                CMD = new_CallableStatement(sqlApplyingTransacParams, sql_text, rcnn.getConnection(), QueryTimeout);
            CallableStatement_execute(CMD);            
            if (params != null && params.size() > 0)
            {
                for (SqlParameter p: params.values())
                {
                    if (p.Direction == SqlParameter.ParameterDirection.Output)
                    {
                        Object v = CallableStatement_getOutParameter(CMD, p.ParameterIndex, p.ParameterName, p.DbType);
                        p.Value = givmegoodsqlval(getGoodValue(v));
                    }
                }
            }
            try
            {
                ResultSet rs;
                while ((rs = CMD.getResultSet()) != null || CMD.getUpdateCount() >= 0)
                {
                    if (rs != null)
                    {
                        onResultSet.call(rs);
                    }
                    if ((CMD.getMoreResults() == false) && (CMD.getUpdateCount() == -1))
                        break;
                }
//            }
            }
            finally
            {
                if (!CMD.isClosed())
                    CMD.close();
            }
            return;
                }
                catch(SQLRecoverableException ex)
                {
                    if (trys == 5)
                        throw ex;
                    rcnn.reconnect();
                    if (Common.IsDebug())
                        System.out.println("DBmethodsLib.DBmethodsCommon.select_from:SQLRecoverableException:" + Common.throwableToString(ex, Common.getCurrentSTE()) + Common.hr() + "sql_text:" + sql_text + Common.hr());
                }
                Common.sleep(100);
            }
        }
        finally
        {
            rcnn.setInUse(false);
        }    
    }
    abstract void setCSCIorReconnect(RefConnection rcnn, boolean CI, String additionalSessionTimeSql) throws SQLException;
    abstract void CallableStatement_setInParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, Object ParameterValue, DbType ParameterSqlType) throws SQLException;
    abstract void CallableStatement_setOutParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, DbType ParameterSqlType) throws SQLException;
    abstract void CallableStatement_execute(CallableStatement CMD) throws SQLException, SQLRecoverableException;
    abstract Object CallableStatement_getOutParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, DbType ParameterSqlType) throws SQLException;
    abstract Object getGoodValue(Object v) throws SQLException;
    abstract public DataTable execText(ConnectionID connectionID, String beginEndText, SqlParameterCollection execParams) throws SQLException;
    abstract public DataTable execText(ConnectionID connectionID, String beginEndText, SqlParameterCollection execParams, boolean returnsTable) throws SQLException;
    abstract public DataTable execText(String beginEndText, SqlParameterCollection execParams) throws SQLException;
    abstract public DataTable execText(String beginEndText, SqlParameterCollection execParams, boolean returnsTable) throws SQLException;
    /**
     * Replacement of default JDBC notation { ?= call ... } 
     * @param connectionID - custom name of connection or "pooled" (to use connection pool)
     * @param procName - name of procedure/function
     * @param procParams - params of procedure/function
     * @return 
     * @throws SQLException
     */
    abstract public DataTable execProc(ConnectionID connectionID, String procName, SqlParameterCollection procParams) throws SQLException;
    /**
     * Replacement of default JDBC notation { ?= call ... } 
     * @param connectionID - custom name of connection or "pooled" (to use connection pool)
     * @param procName - name of procedure/function
     * @param procParams - params of procedure/function
     * @param returnsTable
     * @return 
     * @throws SQLException
     */
    abstract public DataTable execProc(ConnectionID connectionID, String procName, SqlParameterCollection procParams, boolean returnsTable) throws SQLException;
    /**
     * Replacement of default JDBC notation { ?= call ... } . Uses connection pool
     * @param procName - name of procedure/function
     * @param procParams - params of procedure/function
     * @return 
     * @throws SQLException
     */
    abstract public DataTable execProc(String procName, SqlParameterCollection procParams) throws SQLException;
    /**
     * Replacement of default JDBC notation { ?= call ... } . Uses connection pool
     * @param procName - name of procedure/function
     * @param procParams - params of procedure/function
     * @param returnsTable
     * @return 
     * @throws SQLException
     */
    abstract public DataTable execProc(String procName, SqlParameterCollection procParams, boolean returnsTable) throws SQLException;
    
    static Object givmegoodsqlval(Object v)
    {
        if (v == null)
            v = DBNull.Value;
        else if (v instanceof java.util.Date || v instanceof java.sql.Date)
        {                                                
            //v = new DateTime(((java.util.Date)v).getTime());
            v = (new java.util.Date(((java.util.Date)v).getTime())).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        return v;
    }
    
  
 
    
    
    private final Object constantSessionAndTransacParamsLOCK = new Object();
    private final LinkedHashMap<String, SessionOrTransacParam> constantSessionParams = new LinkedHashMap<>();
    private final LinkedHashMap<String, SessionOrTransacParam> constantTransacParams = new LinkedHashMap<>();
    public static class SessionOrTransacParam { public String paramName; public SessionOrTransacParamTypeENUM paramType; public Object paramValue; public SessionOrTransacParamENUM paramLifetime; }
    public enum SessionOrTransacParamTypeENUM { c, t, n }
    public enum SessionOrTransacParamENUM { session, transaction }
    public void addConstantTransacParam(String paramName, SessionOrTransacParamTypeENUM paramType, Object paramValue)
    {
        addConstantSessionOrTransacParam(paramName, paramType, paramValue, SessionOrTransacParamENUM.transaction);        
    }
    public void addConstantSessionParam(String paramName, SessionOrTransacParamTypeENUM paramType, Object paramValue)
    {
        addConstantSessionOrTransacParam(paramName, paramType, paramValue, SessionOrTransacParamENUM.session);
    }
    private void addConstantSessionOrTransacParam(String paramName, SessionOrTransacParamTypeENUM paramType, Object paramValue, SessionOrTransacParamENUM paramLifetime)
    {
        synchronized (constantSessionAndTransacParamsLOCK)
        {
            if (sessionParamsConstructor == null && paramLifetime == SessionOrTransacParamENUM.session)
                throw new RuntimeException("MUSTNEVERTHROW: sessionParamsConstructor is null!");
            if (transacParamsConstructor == null && paramLifetime == SessionOrTransacParamENUM.transaction)
                throw new RuntimeException("MUSTNEVERTHROW: transacParamsConstructor is null!");
            if (constantSessionParams.containsKey(paramName.toLowerCase()) || constantTransacParams.containsKey(paramName.toLowerCase()))
                throw new RuntimeException("MUSTNEVERTHROW: Param '" + paramName + "' already added!");
            SessionOrTransacParam p = new SessionOrTransacParam();
            p.paramName = paramName;
            p.paramType = paramType;
            p.paramValue = paramValue;
            p.paramLifetime = paramLifetime;
            if (paramLifetime == SessionOrTransacParamENUM.session)
                constantSessionParams.put(paramName.toLowerCase(), p);
            else
                constantTransacParams.put(paramName.toLowerCase(), p);
        }
    }
    int constantSessionParams_size() { return constantSessionParams.size(); }
    int constantTransacParams_size() { return constantTransacParams.size(); }
    String getSqlApplyingNewSessionParams(RefConnection rcnn)
    {
        String result = null;
        if (rcnn.constantSessionParamsSet < constantSessionParams.size())
        {
            return sessionParamsConstructor.call(Arrays.copyOfRange(constantSessionParams.values().toArray(), rcnn.constantSessionParamsSet, constantSessionParams.size(), SessionOrTransacParam[].class));
        }
        return result;
    }
    String getSqlApplyingTransacParams(SessionOrTransacParam[] volatileTransacParams)
    {
        String result = null;
        if ((volatileTransacParams != null && volatileTransacParams.length > 0) || constantTransacParams.size() > 0)
        {
            if (volatileTransacParams != null && volatileTransacParams.length > 0)
            {
                if (transacParamsConstructor == null)
                    throw new RuntimeException("MUSTNEVERTHROW: transacParamsConstructor is null!!");
                for(SessionOrTransacParam p : volatileTransacParams)
                    if (p.paramLifetime == SessionOrTransacParamENUM.session)
                        throw new RuntimeException("MUSTNEVERTHROW: volatileTransacParams['" + p.paramName + "'].paramLifetime == SessionOrTransacParamENUM.session!");
            }
            return transacParamsConstructor.call(Common.ConcatArray(Common.collectionToArray(constantTransacParams.values(), SessionOrTransacParam[].class), volatileTransacParams));
        }
        return result;
    }
    private Common.Func1<SessionOrTransacParam[], String> sessionParamsConstructor;
    public void setSessionParamsConstructor(Common.Func1<SessionOrTransacParam[], String> value) { if (sessionParamsConstructor != null) throw new Common.MustNeverHappenException(); sessionParamsConstructor = value; }
    protected Common.Func1<SessionOrTransacParam[], String> transacParamsConstructor;
    public void setTransacParamsConstructor(Common.Func1<SessionOrTransacParam[], String> value) { if (transacParamsConstructor != null) throw new Common.MustNeverHappenException(); transacParamsConstructor = value; }
    protected volatile boolean transacParamsConstructorChecked = false;
    protected void overwriteSessionParamsAndConstructor(Common.Func1<SessionOrTransacParam[], String> ssparcon, Common.Func1<SessionOrTransacParam[], String> trparcon, SessionOrTransacParam[] sotpars)
    {
        synchronized (constantSessionAndTransacParamsLOCK)
        {
            sessionParamsConstructor = ssparcon;
            transacParamsConstructor = trparcon;
            constantSessionParams.clear();
            constantTransacParams.clear();
            for(SessionOrTransacParam p : sotpars)
            {
                if (p.paramLifetime == SessionOrTransacParamENUM.session)
                    constantSessionParams.put(p.paramName.toLowerCase(), p);
                else if (p.paramLifetime == SessionOrTransacParamENUM.transaction)
                    constantTransacParams.put(p.paramName.toLowerCase(), p);
                else 
                    throw new UnsupportedOperationException();
            }
        }
    }
    public void overwriteSessionParamsAndConstructor(DBmethodsCommon src)
    {
        Object[] a = Common.ConcatArray(src.constantSessionParams.values().toArray(), src.constantTransacParams.values().toArray());
        overwriteSessionParamsAndConstructor(src.sessionParamsConstructor, src.transacParamsConstructor, Arrays.copyOf(a, a.length, SessionOrTransacParam[].class));
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    boolean closingDaemonStarted;
    final Object startClosingDaemonLOCK = new Object();
    final long closingDaemonTimeout_ms = 20 * 60 * 1000;
    final long closingDaemonHangedTimeout_ms = 55 * 60 * 1000;
    void startClosingDaemon()
    {
        synchronized (startClosingDaemonLOCK)
        {
            if (!closingDaemonStarted)
            {
                closingDaemon = new Thread(()->{
                    try
                    {
                        closingDaemonStarted = true;
                        while (true)
                        {
                            try
                            {
                                long currTS = System.currentTimeMillis();
                                String NowToString = null;
                                for(RefConnection rc : pooledConnections)
                                    if (rc.lastUse != 0 && (currTS - rc.lastUse) >  (rc.getInUse()?closingDaemonHangedTimeout_ms:closingDaemonTimeout_ms))
                                    {
                                        if (rc.closeConnection())
                                        {
                                            if (NowToString == null)
                                                NowToString = Common.NowToString();
                                            System.out.println(NowToString + " : DBmethodsCommon(" + dB_URL + ") closingDaemon: pooled connection closed.");
                                        }
                                    }
                                for(String rcKey : nonPooledConnections.keySet())
                                {
                                    RefConnection rc = nonPooledConnections.get(rcKey);
                                    if (rc.lastUse != 0 && (currTS - rc.lastUse) >  (rc.getInUse()?closingDaemonHangedTimeout_ms:closingDaemonTimeout_ms))
                                    {
                                        if (rc.closeConnection())
                                        {
                                            if (NowToString == null)
                                                NowToString = Common.NowToString();
                                            System.out.println(NowToString + " : DBmethodsCommon(" + dB_URL + ") closingDaemon: nonPooled connection ('" + rcKey + "') closed.");
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                try
                                {
                                    System.err.println(Common.NowToString() + " : DBmethodsCommon(" + dB_URL + ") closingDaemon exception: " + ex.toString());
                                }
                                catch (Exception dummy) { }
                            }                 
                            for (int n = 0; n < 10; n++)
                                Common.sleep(1000);
                        }
                    }
                    finally
                    {
                        closingDaemonStarted = false;
                    }
                });
                closingDaemon.setName("Closing daemon of DBmethodsCommon(" + dB_URL + ")");
                closingDaemon.setDaemon(true);
                closingDaemon.start();
                while (!closingDaemonStarted)
                {
                    Common.sleep(10);
                }                
            }
        }
    }
    Thread closingDaemon;
    
    public void selectToPgCsv(String sql, String csvFile, Common.Log log) throws SQLException
    {
        Common.Container<Boolean> was = new Common.Container<>(false);
        if (log != null)
            log.write("start");
        select_from((rs) -> 
        {
            if (was.value)
                throw new RuntimeException("Must be only one ResultSet!");            
            if (log != null)
                log.write("begin rs");
            was.value = true;
            ResultSetMetaData rsmd = rs.getMetaData();
            try 
            {
                Path p = Paths.get(csvFile);
                try (FileChannel fc = FileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
                {
                    java.nio.channels.FileLock fclock = fc.tryLock();
                    if (fclock == null)
                        throw new IOException("lock could not be acquired because another program holds an overlapping lock");
                    try 
                    {
                        fc.position(0);



                        StringBuilder sb = new StringBuilder();
                        String nl = Common.NewLine();
                        char[] mustEscStr = new char[] { '"', ',', '\n' };
                        if (nl.contains("\r"))
                            mustEscStr = Common.arrayAppend(mustEscStr, '\r');
                        int cnt = 0;
                        long cntB = 0;
                        long lastLogTs = System.currentTimeMillis();
                        while (rs.next()) 
                        {
                            cnt++;
                            for(int n = 1; n <= rsmd.getColumnCount(); n++)
                            {
                                if (n > 1)
                                    sb.append(",");
                                String colnm = rsmd.getColumnName(n);
                                Object v = rs.getObject(colnm);
                                if (v != null)
                                    if (v instanceof Integer || v instanceof Long || v instanceof BigDecimal)
                                    {
                                        sb.append(v.toString());
                                    }
                                    else if (v instanceof String || v instanceof PGobject || v.getClass().isArray())
                                    {
                                        String s;
                                        if (v instanceof String)
                                            s = (String)v;
                                        else if (v instanceof PGobject)
                                            s = v.toString();
                                        else //if (v.getClass().isArray())
                                            s = "{" + Common.ArrayToString((Object[])v, (e) -> {
                                                if (e instanceof String)
                                                    throw new UnsupportedOperationException("array of String not supported yet!");
                                                else if (v instanceof Integer || v instanceof Long)
                                                    return v.toString();
                                                throw new UnsupportedOperationException("array of " + v.getClass().getCanonicalName() + " not supported yet!");
                                            }, ",") + "}";
                                        
                                        if (Common.stringContainsAnyOfChars(s, mustEscStr))
                                            sb.append("\"").append(s.replace("\"", "\"\"")).append("\"");
                                        else
                                            sb.append(s);
                                    }
                                    else if (v instanceof Boolean)
                                    {
                                        sb.append((Boolean)v ? "t" : "f"); 
                                    }
                                    else if (v instanceof java.util.Date)
                                    {
                                        java.util.Date d = (java.util.Date)v;   
                                        if (d.getTime() % 1000 > 0)
                                            sb.append(Common.DateToString((java.util.Date)v, "yyyy-MM-dd HH:mm:ss.S"));
                                        else
                                            sb.append(Common.DateToString((java.util.Date)v, "yyyy-MM-dd HH:mm:ss"));
                                    }
                                    else
                                        throw new UnsupportedOperationException("" + v.getClass().getCanonicalName() + " type not supported yet!");
                                        
                            }    
                            sb.append(nl);
                            if (sb.length() > 10 * 1024 * 1024)
                            {
                                byte[] b = sb.toString().getBytes(StandardCharsets.UTF_8);
                                fc.write(ByteBuffer.wrap(b));
                                cntB += b.length;
                                sb.setLength(0);
                                if (log != null)
                                    log.write("rs: written: " + cnt + " rows (" + cntB + " bytes)");
                                System.out.println(Common.NowToString() + "    written: " + cnt + " rows (" + cntB + " bytes)");
                            }
                            if (Common.lapsed_sec(lastLogTs) > 10)
                            {
                                lastLogTs = System.currentTimeMillis();
                                if (log != null)
                                    log.write("rs: read: " + cnt + " rows (" + cntB + " bytes)");
                                System.out.println(Common.NowToString() + "    read: " + cnt + " rows (" + cntB + " bytes)");
                            }                                
                        }
                        if (sb.length() > 0)
                        {
                            byte[] b = sb.toString().getBytes(StandardCharsets.UTF_8);
                            fc.write(ByteBuffer.wrap(b));
                            cntB += b.length;
                        }
                        if (log != null)
                            log.writeSync("end rs: in total: " + cnt + " rows (" + cntB + " bytes)");
                        System.out.println(Common.NowToString() + "    in total: " + cnt + " rows (" + cntB + " bytes)");



                        fc.force(true);
                    } 
                    finally 
                    {
                        fclock.release();
                    }
                }
            } 
            catch (IOException ex) 
            {
                throw new RuntimeException(ex);
            }


        }, true, sql);        
    }
}
class RefConnection
{
    private final DBmethodsCommon dbmethods;
    public RefConnection(DBmethodsCommon dbmethods)
    {
        this.dbmethods = dbmethods;
    }
    private Connection connection = null;
    volatile int constantSessionParamsSet = 0;
    public Connection getConnection() throws SQLException
    {
        if (isClosed())
            reconnect();
        return connection;
    }
    final Object reconnectLOCK = new Object();
    volatile int reconnectCOUNT = 0;
    private volatile long reconnect_ts;
    public boolean isConnectionOlder(long than_sec) { return reconnect_ts != 0 && System.currentTimeMillis() > (reconnect_ts + (than_sec * 1000)); }
    public void reconnect() throws SQLException
    {
        int rc = reconnectCOUNT;
        synchronized(reconnectLOCK)
        {
            if (rc == reconnectCOUNT)
            {
                closeConnection();
                connection = dbmethods.getNewActiveConnection();
                reconnect_ts = System.currentTimeMillis();
                constantSessionParamsSet = 0;
                additionalSessionTimeSqls.clear();
                reconnectCOUNT++;
                if (DBmethodsCommon.IsDbDebug() && reconnectCOUNT > 1)
                    System.out.println(Common.NowToString() + "    Connection to " + dbmethods.getDB_URL() + " reconnected. reconnectCOUNT=" + reconnectCOUNT);
                //
                //    reconnectCOUNT = 0;
                //lastReconnected = new DateTime();
            }
        }
    }
    public caseSensivityENUM caseSensivity = caseSensivityENUM.notSet;
    private volatile boolean inUse;
    public void setInUse(boolean v)
    {
        inUse = v;
        lastUse = System.currentTimeMillis();
    }
    public boolean getInUse()
    {
        return inUse;
    }
    public volatile long lastUse;
    
    volatile int reconnectCOUNT_on_closeConnection = -1;
    public boolean closeConnection()
    {
        if (reconnectCOUNT_on_closeConnection != reconnectCOUNT)
        {
            try
            {
                if (connection != null && !connection.isClosed())
                {
                    synchronized(reconnectLOCK)
                    {
                        connection.close();
                        reconnectCOUNT_on_closeConnection = reconnectCOUNT;
                        return true; 
                    }
                }
                else
                    return false;
            }
            catch(SQLException dummy) 
            { 
                return false; 
            }
        }
        else
            return false;
    }
    private boolean isClosed()
    {
        try 
        {
            return connection == null || connection.isClosed();
        } 
        catch (SQLException dummy) 
        {
            return true;
        }
    }
    HashMap<String, Object> additionalSessionTimeSqls = new HashMap<>();
}
enum caseSensivityENUM
{
    notSet,CS,CI;
}