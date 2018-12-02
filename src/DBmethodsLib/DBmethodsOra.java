package DBmethodsLib;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.time.LocalDateTime;

/**
 *
 * 
 */
public class DBmethodsOra extends DBmethodsCommon{
    public DBmethodsOra() { super(); }
    public DBmethodsOra(String DB_URL, String USER, String PASS) {
        super(DB_URL, USER, PASS);
    }

    @Override
    public Connection getNewActiveConnection() throws SQLException
    {        
        oracle.jdbc.pool.OracleDataSource ods = new oracle.jdbc.pool.OracleDataSource();        
        ods.setURL(getDB_URL());
        ods.setUser(uSER);
        ods.setPassword(pASS);
        return ods.getConnection();        
         
        /*
        try {
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(getDB_URL(), getUSER(), getPASS());
        } catch (Exception ex) {
            String stack = "";
            for (StackTraceElement stackTrace : ex.getStackTrace()) 
                stack += stackTrace.toString() + "\r";
            throw new RuntimeException(ex.toString() + "\r\r" + stack);
        }
        */
    }
    @Override 
    void setCSCIorReconnect(RefConnection rcnn, boolean CI, String additionalSessionTimeSql) throws SQLException
    {
        if (CI && rcnn.caseSensivity != caseSensivityENUM.CI)
        {
            PreparedStatement CMD2 = rcnn.getConnection().prepareStatement("ALTER SESSION SET NLS_COMP=LINGUISTIC");  
            CMD2.executeUpdate();
            CMD2.executeUpdate("ALTER SESSION SET NLS_SORT=BINARY_CI");
            if (!CMD2.isClosed())
                CMD2.close();
            rcnn.caseSensivity = caseSensivityENUM.CI;
        }
        else if (!CI && rcnn.caseSensivity != caseSensivityENUM.CS)
        {
            PreparedStatement CMD2 = rcnn.getConnection().prepareStatement("ALTER SESSION SET NLS_COMP=BINARY");  
            CMD2.executeUpdate();
            if (!CMD2.isClosed())
                CMD2.close();
            rcnn.caseSensivity = caseSensivityENUM.CS;
        }
        else//chk connection
        {
            PreparedStatement CMD2 = rcnn.getConnection().prepareStatement("begin null; end;");
            CMD2.executeUpdate();
            if (!CMD2.isClosed())
                CMD2.close();
        }
    }
    
    @Override
    void CallableStatement_setInParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, Object ParameterValue, DbType ParameterSqlType) throws SQLException
    {
        if (ParameterValue == null || ParameterValue == DBNull.Value)
            CMD.setNull(ParameterIndex, ParameterSqlType.toInt());
        else 
        if (ParameterSqlType == DbType.BLOB)
        {
            if (!(ParameterValue instanceof byte[]))
                throw new Error("DbType.BLOB params must be set as byte[] !");
            if (ParameterIndex != -1)
                CMD.setBlob(ParameterIndex, new ByteArrayInputStream((byte[])ParameterValue));
            else
                CMD.setBlob(ParameterName, new ByteArrayInputStream((byte[])ParameterValue));
        }
        else
        {
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
            }
            if (ParameterIndex != -1)
                CMD.setObject(ParameterIndex, ParameterValue, ParameterSqlType.toInt());
            else
                CMD.setObject(ParameterName, ParameterValue, ParameterSqlType.toInt());
        }
    }
    @Override
    void CallableStatement_setOutParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, DbType ParameterSqlType) throws SQLException {
        if (ParameterIndex != -1)
            CMD.registerOutParameter(ParameterIndex, ParameterSqlType.toInt());
        else
            CMD.registerOutParameter(ParameterName, ParameterSqlType.toInt());
    }
    @Override
    Object CallableStatement_getOutParameter(CallableStatement CMD, int ParameterIndex, String ParameterName, DbType ParameterSqlType) throws SQLException {
        if (ParameterSqlType == DbType.BLOB)
        {
            if (ParameterIndex != -1)
                return CMD.getBytes(ParameterIndex);
            else
                return CMD.getBytes(ParameterName);  
        }
        else
            if (ParameterIndex != -1)
                return CMD.getObject(ParameterIndex);
            else
                return CMD.getObject(ParameterName);  
    }

    @Override
    Object getGoodValue(Object v) throws SQLException {
        if (v instanceof oracle.sql.Datum)
            v = ((oracle.sql.Datum)v).stringValue();
        return v;
    }

    @Override
    public DataTable execText(String beginEndText, SqlParameterCollection execParams, boolean returnsTable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execText(String beginEndText, SqlParameterCollection execParams) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execProc(String procName, SqlParameterCollection procParams) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execProc(String procName, SqlParameterCollection procParams, boolean returnsTable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected CallableStatement new_CallableStatement(String sqlApplyingTransacParams, String sql_text, Connection CONN, int QueryTimeout) throws SQLException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        return new_CallableStatement_internal((sqlApplyingTransacParams == null ? "" : sqlApplyingTransacParams + ";\r\n/\r\n") + sql_text, CONN, QueryTimeout);
    }

    @Override
    public DataTable execText(ConnectionID connectionID, String beginEndText, SqlParameterCollection execParams) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execText(ConnectionID connectionID, String beginEndText, SqlParameterCollection execParams, boolean returnsTable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execProc(ConnectionID connectionID, String procName, SqlParameterCollection procParams) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execProc(ConnectionID connectionID, String procName, SqlParameterCollection procParams, boolean returnsTable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable select_from(String nodeRequest, String sql_text) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable select_from(String nodeRequest, String sql_text, SqlParameterCollection params) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable[] select_from_multi(String nodeRequest, String sql_text) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable[] select_from_multi(String nodeRequest, String sql_text, SqlParameterCollection params) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execText(String nodeRequest, String beginEndText, SqlParameterCollection execParams) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execText(String nodeRequest, String beginEndText, SqlParameterCollection execParams, boolean returnsTable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execProc(String nodeRequest, String procName, SqlParameterCollection procParams) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execProc(String nodeRequest, String procName, SqlParameterCollection procParams, boolean returnsTable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DataTable execDoWithOutcursor(String PLpgSQL) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    void CallableStatement_execute(CallableStatement CMD) throws SQLException, SQLRecoverableException 
    {        
        CMD.execute();
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
