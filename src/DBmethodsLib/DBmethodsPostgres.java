package DBmethodsLib;

import CommonLib.Common;
import CommonLib.Crypto.CryptoExCommon;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.zip.CRC32;
import org.postgresql.jdbc.PgArray;

/**
 *
 * 
 */
public class DBmethodsPostgres extends DBmethodsCommon //implements DBaggregator
{
    public DBmethodsPostgres() { super(); }
    public DBmethodsPostgres(String DB_URL, String USER, String PASS) {
        super(DB_URL, USER, PASS);
    }
    public DBmethodsPostgres(String DB_URL, String USER, String PASS, int maxPoolSize) {
        super(DB_URL, USER, PASS, maxPoolSize);
    }

    @Override
    public Connection getNewActiveConnection() throws SQLException
    {
        //try {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return DriverManager.getConnection(getDB_URL(), uSER, pASS);
        /*} catch (Exception ex) {
            String stack = "";
            for (StackTraceElement stackTrace : ex.getStackTrace()) 
                stack += stackTrace.toString() + "\r";
            throw new RuntimeException(ex.toString() + "\r\r" + stack);
        }*/
        
    }
    @Override 
    public CallableStatement new_CallableStatement(String sqlApplyingTransacParams, String sql_text, Connection CONN, int QueryTimeout) throws SQLException
    {
        return new_CallableStatement_internal((sqlApplyingTransacParams == null ? "" : sqlApplyingTransacParams + ";\r\n") + sql_text, CONN, QueryTimeout);
    }
    @Override 
    void setCSCIorReconnect(RefConnection rcnn, boolean CI, String additionalSessionTimeSql) throws SQLException
    {
        try
        {
            String sql = getSqlApplyingNewSessionParams(rcnn);
                
            String additionalSessionTimeSqlHash = additionalSessionTimeSql == null ? null : CryptoExCommon.sha1.hashB64(additionalSessionTimeSql);
            if (additionalSessionTimeSqlHash != null && !rcnn.additionalSessionTimeSqls.containsKey(additionalSessionTimeSqlHash))
                sql = (sql == null ? "" : sql + (sql.endsWith(";") ? "\n" : ";\n")) + additionalSessionTimeSql;

            if (!transacParamsConstructorChecked && transacParamsConstructor != null)
            {//chk that  getSqlApplyingTransacParams does not return resultset
                SessionOrTransacParam[] dummy  = new SessionOrTransacParam[] { new SessionOrTransacParam() };
                dummy[0].paramName = "dummyTransacParamTest";
                dummy[0].paramType = SessionOrTransacParamTypeENUM.n;
                dummy[0].paramValue = 1;
                dummy[0].paramLifetime = SessionOrTransacParamENUM.transaction;                    
                
                sql = (sql == null ? "" : sql + (sql.endsWith(";") ? "\n" : ";\n")) + getSqlApplyingTransacParams(dummy);
            }
            if (sql == null)
                sql = "do $$ begin null; end $$";
            try (PreparedStatement CMD2 = rcnn.getConnection().prepareStatement(sql))
            {            
                CMD2.executeUpdate();
                if (additionalSessionTimeSqlHash != null && !rcnn.additionalSessionTimeSqls.containsKey(additionalSessionTimeSqlHash))
                    rcnn.additionalSessionTimeSqls.put(additionalSessionTimeSqlHash, null);
                if (!transacParamsConstructorChecked && transacParamsConstructor != null)
                    transacParamsConstructorChecked = true;
                if (rcnn.constantSessionParamsSet < constantSessionParams_size())
                    rcnn.constantSessionParamsSet = constantSessionParams_size();
            }
        }
        catch (org.postgresql.util.PSQLException ex)
        {
            if (ex.getMessage().contains("terminating connection due to administrator command")
                ||
                ex.getMessage().contains("Ошибка ввода/ввывода при отправке бэкенду")
            )
            {
                throw new SQLRecoverableException(ex);
            }
            else
                throw ex;
        }
    }

    @Override
    void CallableStatement_setInParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, Object ParameterValue, DbType ParameterSqlType) throws SQLException
    {
        if (ParameterIndex == -1)
            throw new Error("postgres supports only numbered params!");
        if (ParameterValue == null || ParameterValue == DBNull.Value)
            CMD.setNull(ParameterIndex, ParameterSqlType.toInt());
        else 
        if (ParameterSqlType == DbType.NUMBER)
            CMD.setBigDecimal(ParameterIndex, new BigDecimal(ParameterValue.toString()));
        else 
        if (ParameterSqlType == DbType.INTEGER)
            CMD.setInt(ParameterIndex, (Integer)ParameterValue);
        else 
        if (ParameterSqlType == DbType.BOOLEAN)
            CMD.setBoolean(ParameterIndex, (Boolean)ParameterValue);
        else 
        if (ParameterSqlType == DbType.BIGINT)
            CMD.setLong(ParameterIndex, (Long)ParameterValue);
        else 
        if (ParameterSqlType == DbType.TIMESTAMP)
        {
            if (ParameterValue instanceof java.util.Date)
            {
                ParameterValue = new java.sql.Timestamp(((java.util.Date)ParameterValue).getTime());
            }
            else if (ParameterValue instanceof LocalDateTime)
            {
                ParameterValue = java.sql.Timestamp.valueOf((LocalDateTime)ParameterValue);
            }
            CMD.setTimestamp(ParameterIndex, (java.sql.Timestamp)ParameterValue);
        }
        else             
        if (ParameterSqlType == DbType.VARCHAR || ParameterSqlType == DbType.JSON)
            CMD.setString(ParameterIndex, (String)ParameterValue);
        else 
        if (ParameterSqlType == DbType.ARRAY)
        {
            Array a;
            if (ParameterValue instanceof long[] || ParameterValue instanceof Long[])
            {
                Long[] v;
                if (ParameterValue instanceof Long[])
                    v = (Long[])ParameterValue;
                else
                {
                    long[] v_ = (long[])ParameterValue;
                    v = new Long[v_.length];
                    for(int n = 0; n < v_.length; n++) v[n] = v_[n];
                }
                a = CMD.getConnection().createArrayOf("bigint", v);
            }
            else if (ParameterValue instanceof int[] || ParameterValue instanceof Integer[])
            {
                Integer[] v;
                if (ParameterValue instanceof Integer[])
                    v = (Integer[])ParameterValue;
                else
                {
                    int[] v_ = (int[])ParameterValue;
                    v = new Integer[v_.length];
                    for(int n = 0; n < v_.length; n++) v[n] = v_[n];
                }
                a = CMD.getConnection().createArrayOf("integer", v);
            }
            else
                a = CMD.getConnection().createArrayOf("varchar", (Object[])ParameterValue);
            CMD.setArray(ParameterIndex, a);
        }
        else 
        if (ParameterSqlType == DbType.BLOB)
        {
            if (!(ParameterValue instanceof byte[]))
                throw new Error("DbType.BLOB params must be set as byte[] !");
            CMD.setBytes(ParameterIndex, (byte[])ParameterValue);
        }
        else 
            throw new Error("ParameterSqlType==" + ParameterSqlType.toString() + " not supported for postgres yet!");
    }
    @Override
    void CallableStatement_setOutParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, DbType ParameterSqlType) throws SQLException {
        if (ParameterIndex == -1)
            throw new Error("postgres supports only numbered params!");
        int t = ParameterSqlType.toInt();
        if (ParameterSqlType == DbType.BLOB)
            t = -2;
        CMD.registerOutParameter(ParameterIndex, t);
    }
    @Override
    Object CallableStatement_getOutParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, DbType ParameterSqlType) throws SQLException {
        if (ParameterIndex == -1)
            throw new Error("postgres supports only numbered params!");
        if (ParameterSqlType == DbType.BLOB)
            return CMD.getBytes(ParameterIndex);
        else if (ParameterSqlType == DbType.ARRAY)
        {
            PgArray v = (PgArray)CMD.getObject(ParameterIndex);
            return v.getArray();
        }
        else
            return CMD.getObject(ParameterIndex);
    }

    @Override
    Object getGoodValue(Object v) throws SQLException {
        if (v instanceof PgArray)
        {
            return ((PgArray)v).getArray();
        }
        else
            return v;
    }
    
    @Override
    public DataTable execText(String beginEndText, SqlParameterCollection execParams) throws SQLException
    {
        return exec(new ConnectionID("pooled"), beginEndText, execParams, false, ExecType.text);
    }    
    @Override
    public DataTable execText(String beginEndText, SqlParameterCollection execParams, boolean returnsTable_aka_setof) throws SQLException
    {
        return exec(new ConnectionID("pooled"), beginEndText, execParams, returnsTable_aka_setof, ExecType.text);
    }    

    @Override
    public DataTable execText(ConnectionID connectionID, String beginEndText, SqlParameterCollection execParams) throws SQLException
    {
        return exec(connectionID, beginEndText, execParams, false, ExecType.text);
    }

    @Override
    public DataTable execText(ConnectionID connectionID, String beginEndText, SqlParameterCollection execParams, boolean returnsTable_aka_setof) throws SQLException
    {
        return exec(connectionID, beginEndText, execParams, returnsTable_aka_setof, ExecType.text);
    }

    @Override
    void CallableStatement_execute(CallableStatement CMD) throws SQLException, SQLRecoverableException {
        try
        {
            CMD.execute();
        }
        catch (org.postgresql.util.PSQLException ex)
        {
            if (ex.getMessage().contains("schema \"pg_temp\" does not exist")
            )
            {
                throw new SQLRecoverableException(ex);
            }
            else
                throw ex;
        }
    }
    
    enum ExecType { proc, text }
    private DataTable exec(ConnectionID connectionID, String beginEndText_or_procName, SqlParameterCollection execParams, boolean returnsTable_aka_setof, ExecType execType) throws SQLException
    {
        String ddlArgs = "";
        String callArgs = "";
        String outProcSelectColumnsWithTypes = "";
        String outProcSelectColumns = "";
        SqlParameterCollection parmsIn = new SqlParameterCollection();
        SqlParameterCollection parmsOut = new SqlParameterCollection();
        if (execParams != null)
            for (SqlParameter p: execParams.values())
            {
                if (!p.ParameterName.startsWith("p_") && !p.ParameterName.startsWith(p.Direction == SqlParameter.ParameterDirection.Input ? "in_" : "out_"))
                    throw new RuntimeException("Param name '" + p.ParameterName + "' is not begins with 'p_' or '" + (p.Direction == SqlParameter.ParameterDirection.Input ? "in_" : "out_") + "'! Add that prefix to param name.");
                if (execType == ExecType.text && !beginEndText_or_procName.contains(p.ParameterName))
                    throw new RuntimeException("Param '" + p.ParameterName + "' not found in beginEndText!");
                ddlArgs = (ddlArgs.isEmpty() ? "" : ddlArgs + ", ") + (p.Direction == SqlParameter.ParameterDirection.Output ? "out " : "") 
                        + p.ParameterName + " " + (p.Direction == SqlParameter.ParameterDirection.Output ? p.DbType.toPgOutName() : p.DbType.toPgName());
                if (p.Direction == SqlParameter.ParameterDirection.Input)
                {
                    callArgs = (callArgs.isEmpty() ? "" : callArgs + ", ") + "cast( :" + p.ParameterName + " as " + p.DbType.toPgName() + " )";
                    parmsIn.Add(p);
                }
                else
                {
                    outProcSelectColumnsWithTypes = (outProcSelectColumnsWithTypes.isEmpty() ? "" : outProcSelectColumnsWithTypes + ", ") + p.ParameterName + " " + p.DbType.toPgOutName();
                    outProcSelectColumns = (outProcSelectColumns.isEmpty() ? "" : outProcSelectColumns + ", ") + p.ParameterName;
                    parmsOut.Add(p);
                }
            }
        if (returnsTable_aka_setof && parmsOut.isEmpty())
            throw new RuntimeException("returnsTable=true allowed only if out params set!");
                
        String sql;
        String additionalSessionTimeSql = null;
        if (execType == ExecType.text)
        {
            CRC32 crc32 = new CRC32();
            crc32.update(ddlArgs.getBytes());
            long ddlArgsCrc32 = Math.abs(crc32.getValue());
            crc32.update(beginEndText_or_procName.getBytes());
            long beginEndTextCrc32 = Math.abs(crc32.getValue());
            String returnType = (parmsOut.isEmpty() ? "void" : (returnsTable_aka_setof ? "setof " : "") + (parmsOut.size() > 1 ? "record" : parmsOut.toArray()[0].DbType.toPgOutName()));
            crc32.update(returnType.getBytes());
            long returnTypeCrc32 = Math.abs(crc32.getValue());
            String func_name = "pg_temp.anon_" + Long.toHexString(ddlArgsCrc32) + "_" + Long.toHexString(beginEndTextCrc32) + "_" + (Long.toHexString(returnTypeCrc32) + "0000").substring(0, 4);//"_th" + Thread.currentThread().getId();
            additionalSessionTimeSql = 
                "create or replace function " + func_name + "(" + ddlArgs + ")\r\n" +
                "returns " + returnType + " language plpgsql as $anon_body$\r\n" +
                beginEndText_or_procName + "\r\n" +
                "$anon_body$;";
            sql = "select * from " + func_name + "(" + callArgs + ");";
        }
        else if (execType == ExecType.proc)
        {
            sql = "select * from " + beginEndText_or_procName + "(" + callArgs + ")";
            if (returnsTable_aka_setof)
                sql += " as t(" + outProcSelectColumnsWithTypes + ")";
            else if (outProcSelectColumns.length() > 0)
                sql += " as t(" + outProcSelectColumns + ")";
                
            sql += ";";
        }
        else 
            throw new UnsupportedOperationException();
        DataTable[] results;
//        try {
            results = select_from(connectionID, sql, parmsIn, additionalSessionTimeSql);
//        } catch (Exception ex) {
//            throw ex;
//        }
        DataTable result = results[results.length - 1];
        if (returnsTable_aka_setof)
            return result;
        for (SqlParameter p: parmsOut.values())
        {
            Object o = result.Rows.get(0).get(p.ParameterName);
            if (p.DbType == DbType.BLOB)
                p.Value = o;
            else if (p.DbType == DbType.ARRAY && o instanceof PgArray)
            {
                PgArray v = (PgArray)o;
                p.Value =  v.getArray();
            }
            else
                p.Value = o;
        }
        return result;
    }

    @Override
    public DataTable execProc(String procName, SqlParameterCollection procParams) throws SQLException 
    {
        return exec(new ConnectionID("pooled"), procName, procParams, false, ExecType.proc);
    }
    @Override
    public DataTable execProc(String procName, SqlParameterCollection procParams, boolean returnsTable_aka_setof) throws SQLException 
    {
        return exec(new ConnectionID("pooled"), procName, procParams, returnsTable_aka_setof, ExecType.proc);
    }
    @Override
    public DataTable execProc(ConnectionID connectionID, String procName, SqlParameterCollection procParams) throws SQLException
    {
        return exec(connectionID, procName, procParams, false, ExecType.proc);
    }
    @Override
    public DataTable execProc(ConnectionID connectionID, String procName, SqlParameterCollection procParams, boolean returnsTable_aka_setof) throws SQLException
    {
        return exec(connectionID, procName, procParams, returnsTable_aka_setof, ExecType.proc);
    }
    
    /**
     * Methon allows to select  Postgres data from PLpgSQL (including readonly-DB)
     * @param PLpgSQL must contain "OPEN outcursor FOR", e.g. filling the cursor with a name outcursor
     * @return
     * @throws SQLException
     */
    @Override
    public DataTable execDoWithOutcursor(String PLpgSQL) throws SQLException 
    {
        if (!Objects.requireNonNull(PLpgSQL, "beginEndText is empty").replace("\r", " ").replace("\n", " ") .contains(" OPEN outcursor FOR "))
            throw new NullPointerException("beginEndText does not contains 'OPEN outcursor FOR'");
        return select_from(true, 
"DO $$\n" +
"DECLARE\n" +
"    outcursor CONSTANT refcursor := 'outcursor';\n" +
"BEGIN\n" + 
PLpgSQL + "\n" +
"END\n" +
"$$;\n" +
"FETCH ALL FROM outcursor;\n"
        )[0];        
    }
    
    
    
    public static String getSqlValue(Object value, DbType type)
    {
        return getSqlValue(value, type, true, null, null);
    }
    public static String getSqlValue(Object value, DbType type, Object defaultValue)
    {
        return getSqlValue(value, type, true, null, defaultValue);
    }
    public static String getSqlValue(Object value, DbType type, DbType arrayType)
    {
        return getSqlValue(value, type, true, arrayType, null);
    }
    public static String getSqlValue(Object value, DbType type, boolean emptyVarcharIsNull, DbType arrayElemType, Object defaultValue)
    {
        if (value == null || value == DBNull.Value || ("".equals(value) && (type != DbType.VARCHAR || emptyVarcharIsNull)))
            if (defaultValue == null)
                return "null::" + (type == DbType.ARRAY ? arrayElemType.toPgName() + "[]" : type.toPgName());
            else
                return getSqlValue(defaultValue, type, emptyVarcharIsNull, arrayElemType, null);
        String valueS = value.toString().replace("'", "''");
        if (type == DbType.VARCHAR)
        {
            return "'" + valueS + "'";
        }
        else if (type == DbType.TIMESTAMP)
        {
            return "'" + valueS + "'::" + type.toPgName();
        }
        else if (type == DbType.BIGINT || type == DbType.INTEGER)
        {
            valueS = valueS.replace(",", ".").replace(" ", "");
            try
            {                
                return (value instanceof Long || value instanceof Integer ? value : Long.valueOf(valueS)) + "::" + type.toPgName();
            }
            catch(Exception ex)
            {
                throw new RuntimeException("Unsupported value (" + value.toString() + ") for type " + type.toString() + "! Details: " + ex.toString());
            }
        }
        else if (type == DbType.NUMBER)
        {
            valueS = valueS.replace(",", ".").replace(" ", "");
            try
            {                
                return (value instanceof BigDecimal ? value : new BigDecimal(valueS)).toString() + "::" + type.toPgName();
            }
            catch(Exception ex)
            {
                throw new RuntimeException("Unsupported value (" + value.toString() + ") for type " + type.toString() + "! Details: " + ex.toString());
            }
        }
        else if (type == DbType.BOOLEAN)
        {
            valueS = valueS.trim().toLowerCase();
            if (valueS.equals("true") || valueS.equals("t") || valueS.equals("1") || valueS.equals("-1") || valueS.equals("y") || valueS.equals("yes"))
                return "true::" + type.toPgName();
            else
            if (valueS.equals("false") || valueS.equals("f") || valueS.equals("0") || valueS.equals("n") || valueS.equals("no"))
                return "false::" + type.toPgName();
            else
                throw new RuntimeException("Unsupported value (" + value.toString() + ") for type " + type.toString() + "!");
        }
        else if (type == DbType.ARRAY)
        {
            Object v = value;
            if (v instanceof long[])
                v = Common.Array_long_to_Long((long[])value);
            if (v instanceof int[])
                v = Common.Array_int_to_Integer((int[])value);
            String result = null;
            if (v instanceof String[] || v instanceof Long[] || v instanceof Integer[] || v instanceof BigDecimal[])
            {
                for(Object valueEl : (Object[])v)
                    result = (result == null ? "ARRAY[" : result + ", ") + getSqlValue(valueEl, arrayElemType);
            }
            else
                throw new RuntimeException("Not ready yet for value type (" + value.getClass().getCanonicalName() + ") and type == DbType.ARRAY :(");
            return result + "]::" + arrayElemType.toPgName() + "[]";
        }
        else
            throw new RuntimeException("Not ready yet for value type (" + type.toString() + ") :(");
    }

    
    
    //<DBaggregator>

    @Override
    public DataTable select_from(String nodeRequest, String sql_text) throws SQLException {//DBaggregator
        return select_from(nodeRequest, sql_text, null);
    }
    @Override
    public DataTable select_from(String nodeRequest, String sql_text, SqlParameterCollection params) throws SQLException {//DBaggregator
        DataTable[] dts = select_from(true, sql_text, params);
        if (dts.length > 0)
            return dts[0];
        else
            return null;
    }
    @Override
    public DataTable[] select_from_multi(String nodeRequest, String sql_text) throws SQLException {//DBaggregator
        return select_from(true, sql_text, null);
    }
    @Override
    public DataTable[] select_from_multi(String nodeRequest, String sql_text, SqlParameterCollection params) throws SQLException {//DBaggregator
        return select_from(true, sql_text, params);
    }
    @Override
    public DataTable execText(String nodeRequest, String beginEndText, SqlParameterCollection execParams) throws SQLException {//DBaggregator
        return execText(beginEndText, execParams);
    }
    @Override
    public DataTable execText(String nodeRequest, String beginEndText, SqlParameterCollection execParams, boolean returnsTable) throws SQLException {//DBaggregator
        return execText(beginEndText, execParams, returnsTable);
    }
    @Override
    public DataTable execProc(String nodeRequest, String procName, SqlParameterCollection procParams) throws SQLException {//DBaggregator
        return execProc(procName, procParams);
    }
    @Override
    public DataTable execProc(String nodeRequest, String procName, SqlParameterCollection procParams, boolean returnsTable) throws SQLException {//DBaggregator
        return execProc(procName, procParams, returnsTable);
    }
    //</DBaggregator>
    

    
    
    
    
    
    
    
    
    
    
    public static String DB_URL_appendPostgesApplicationName(String DB_URL, String ApplicationName, String errMsgIfContains)
    {
        if (!DB_URL.toLowerCase().contains("applicationname="))
            return DB_URL + (DB_URL.toLowerCase().contains("?") ? "&" : "?") + "ApplicationName=" + ApplicationName + "|" + ManagementFactory.getRuntimeMXBean().getName();
        else
            throw new Error(errMsgIfContains);
    }
}
