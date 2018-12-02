package DBmethodsLib;

import java.sql.SQLException;

/**
 *
 * 
 */
public interface DBaggregator 
{
    public DataTable select_from(String nodeRequest, String sql_text) throws SQLException;
    public DataTable select_from(String nodeRequest, String sql_text, SqlParameterCollection params) throws SQLException;

    public DataTable[] select_from_multi(String nodeRequest, String sql_text) throws SQLException;
    public DataTable[] select_from_multi(String nodeRequest, String sql_text, SqlParameterCollection params) throws SQLException;
    
    public DataTable execText(String nodeRequest, String beginEndText, SqlParameterCollection execParams) throws SQLException;
    public DataTable execText(String nodeRequest, String beginEndText, SqlParameterCollection execParams, boolean returnsTable) throws SQLException;
    /**
     * Replacement of default JDBC notation { ?= call ... } 
     * @param nodeRequest
     * @param procName - name of procedure/function
     * @param procParams - params of procedure/function
     * @return 
     * @throws SQLException
     */
    public DataTable execProc(String nodeRequest, String procName, SqlParameterCollection procParams) throws SQLException;
    /**
     * Replacement of default JDBC notation { ?= call ... }
     * @param nodeRequest
     * @param procName - name of procedure/function
     * @param procParams - params of procedure/function
     * @param returnsTable
     * @return 
     * @throws SQLException
     */
    public DataTable execProc(String nodeRequest, String procName, SqlParameterCollection procParams, boolean returnsTable) throws SQLException;
    
    /**
     * Methon allows to select  Postgres data from PLpgSQL (including readonly-DB)
     * @param PLpgSQL must contain "OPEN outcursor FOR", e.g. filling the cursor with a name outcursor
     * @return
     * @throws SQLException
     */
    public DataTable execDoWithOutcursor(String PLpgSQL) throws SQLException;

    public static class DBagregatorTest implements DBaggregator
    {
        final DBmethodsPostgres dbmethods;
        public DBagregatorTest(DBmethodsPostgres dbmethods)
        {
            this.dbmethods = dbmethods;
        }
        @Override
        public DataTable select_from(String nodeRequest, String sql_text) throws SQLException
        {
            DataTable[] a = dbmethods.select_from(true, sql_text);
            return a.length == 0 ? null : a[0]; 
        }
        @Override
        public DataTable select_from(String nodeRequest, String sql_text, SqlParameterCollection params) throws SQLException
        {
            DataTable[] a = dbmethods.select_from(true, sql_text, params);
            return a.length == 0 ? null : a[0]; 
        }

        @Override
        public DataTable[] select_from_multi(String nodeRequest, String sql_text) throws SQLException {
            return dbmethods.select_from(true, sql_text);
        }
        @Override
        public DataTable[] select_from_multi(String nodeRequest, String sql_text, SqlParameterCollection params) throws SQLException {
            return dbmethods.select_from(true, sql_text, params);
        }

        @Override
        public DataTable execText(String nodeRequest, String beginEndText, SqlParameterCollection execParams) throws SQLException {
            return dbmethods.execText(beginEndText, execParams); 
        }

        @Override
        public DataTable execText(String nodeRequest, String beginEndText, SqlParameterCollection execParams, boolean returnsTable) throws SQLException {
            return dbmethods.execText(beginEndText, execParams, returnsTable); 
        }

        @Override
        public DataTable execProc(String nodeRequest, String procName, SqlParameterCollection procParams) throws SQLException {
            return dbmethods.execProc(procName, procParams); 
        }

        @Override
        public DataTable execProc(String nodeRequest, String procName, SqlParameterCollection procParams, boolean returnsTable) throws SQLException {
            return dbmethods.execProc(procName, procParams, returnsTable); 
        }

        @Override
        public DataTable execDoWithOutcursor(String PLpgSQL) throws SQLException 
        {
            return dbmethods.execDoWithOutcursor(PLpgSQL); 
        }
        
        
    }
}
