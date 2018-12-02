package DBmethodsLib;

//import CommonLib.DateTime;
import java.time.LocalDateTime;
import CommonLib.Common;
import java.util.Objects;

/**
 *
 * 
 */
public class AnyTable {
    private volatile DataTable value;
    private final Object valueLOCK = new Object();
    private volatile LocalDateTime lastCACHERESET;
    public final String sqlExpr;
    private final String[] indexes;
    private final DBmethodsCommon.ConnectionID connId;
    protected final DBmethodsCommon dbmethods;
    private volatile long lastrefresh;
    private final int timeToLive;
    public AnyTable(DBmethodsCommon dbmethods, String sqlExpr)
    {
        this(dbmethods, sqlExpr, null);
    }
    public AnyTable(DBmethodsCommon dbmethods, String sqlExpr, String[] indexes)
    {
        this(dbmethods, sqlExpr, indexes, "pooled");
    }
    public AnyTable(DBmethodsCommon dbmethods, String sqlExpr, String[] indexes, String connId)
    {
        this(dbmethods, sqlExpr, indexes, connId, -1);
    }
    public AnyTable(DBmethodsCommon dbmethods, String sqlExpr, String[] indexes, String connId, int timeToLive)
    {
        this(dbmethods, sqlExpr, indexes, connId, timeToLive, false);
    }
    public AnyTable(DBmethodsCommon dbmethods, String sqlExpr, String[] indexes, String connId, int timeToLive, boolean isAsync)
    {
        this(dbmethods, sqlExpr, indexes, connId, timeToLive, isAsync, null);
    }
    public AnyTable(DBmethodsCommon dbmethods, String sqlExpr, String[] indexes, String connId, int timeToLive, boolean isAsync, Common.Action1<DataTable> onAsyncRefreshed)
    {
        this(dbmethods, sqlExpr, indexes, connId, timeToLive, isAsync, onAsyncRefreshed, -1);
    }
    public AnyTable(DBmethodsCommon dbmethods, String sqlExpr, String[] indexes, String connId, int timeToLive, boolean isAsync, Common.Action1<DataTable> onAsyncRefreshed, int resetAfter_async)
    {
        this(dbmethods, sqlExpr, indexes, connId, timeToLive, isAsync, onAsyncRefreshed, -1 , -1);
    }
    public AnyTable(DBmethodsCommon dbmethods, String sqlExpr, String[] indexes, String connId, int timeToLive, boolean isAsync, Common.Action1<DataTable> onAsyncRefreshed, int resetAfter_async, int pauseAsyncRefreshAfter)
    {
        this.sqlExpr = sqlExpr;
        this.indexes = indexes;
        this.connId = new DBmethodsCommon.ConnectionID(connId);
        this.dbmethods = Objects.requireNonNull(dbmethods);
        this.timeToLive = timeToLive;
        this.isAsync = isAsync;
        this.onAsyncRefreshed = onAsyncRefreshed;
        this.resetAfter_async = resetAfter_async;
        this.pauseAsyncRefreshAfter = pauseAsyncRefreshAfter;        
    }
    private final boolean isAsync;
    private volatile boolean asyncInited;
    private long asyncInitedTS;
    private AsyncLoadThread asyncLoadThread;
    private final Object initAsyncLOCK = new Object();
    private final int resetAfter_async;
    private final int pauseAsyncRefreshAfter;
    private volatile long lastget;
    public void initAsync()
    {
        synchronized(initAsyncLOCK)
        {
            if (!asyncInited)
            {
                asyncLoadThread = new AsyncLoadThread(this);
                Thread th = new Thread(asyncLoadThread);
                th.setDaemon(true);
                th.setName("anytable.AsyncLoadThread");
                th.start();
                asyncInited = true;
                asyncInitedTS = System.currentTimeMillis();
            }
        }
        
    }
    protected LocalDateTime getCACHERESET() { return LocalDateTime.MIN; }

    private DataTable get(/*boolean forceRefresh, */boolean ensureNotNull)
    {
        lastget = System.currentTimeMillis();
        if (!isAsync /*|| forceRefresh*/)
        {
//            if (forceRefresh) 
//                lastCACHERESET = null; 
           return refresh().dt;
        }
        if (isAsync)
        {
            if (!asyncInited)
                initAsync();
            else if (asyncLoadThread.lastError != null)
                throw new RuntimeException(asyncLoadThread.lastError);
            if (ensureNotNull && value == null)
                return refresh().dt;
        }       
        return value;
    }
    public DataTable get() 
    {
        return get(/*false, */true); 
    }   
    public DataTable getNullable()
    {
        return get(/*false, */false);
    }
    
    static class refreshRESULT { DataTable dt; boolean refreshed; }
    private refreshRESULT refresh()
    {
        synchronized(valueLOCK)
        {
            refreshRESULT result = new refreshRESULT();
            LocalDateTime cr = getCACHERESET();
            if (value == null || lastCACHERESET == null || !lastCACHERESET.equals(cr) 
                || 
                (
                    timeToLive > 0 && (lastrefresh == 0 || (System.currentTimeMillis() - lastrefresh) > (timeToLive * 1000))
                )
            )
            {
                try
                {
                    DataTable dt = dbmethods.select_from(connId, sqlExpr)[0];
                    dt.addIndexes(indexes);
                    value = dt;
                    lastrefresh = System.currentTimeMillis();
                    lastCACHERESET = cr;
                    result.refreshed = true;
                }
                catch(Exception ex)
                {
                    if (Common.IsDebug())
                        throw new RuntimeException(ex.toString() + Common.hr + "sqlExpr: " + Common.br() + sqlExpr);
                    else
                        throw new RuntimeException(ex);
                }
            }
            result.dt = value;
            return result;
        }
    }
    public Common.Action1<DataTable> onAsyncRefreshed;
    
    static class AsyncLoadThread implements Runnable
    {
        AsyncLoadThread(AnyTable parent)
        {
            this.parent = parent;
        }
        private final AnyTable parent;
        static boolean stopAllNow = false;
        volatile String lastError = null;
        @Override
        public void run() {
            while (!stopAllNow)
            {
                try
                {                    
                    Thread.sleep(100);
                    if (parent.lastrefresh != 0
                        &&
                        (
                            parent.resetAfter_async > 0 && ((System.currentTimeMillis() - parent.lastrefresh) > (parent.resetAfter_async * 1000))
                        )
                        &&
                        parent.value != null
                    )
                    {
                        synchronized(parent.valueLOCK)
                        {
                            parent.value = null;
                        }
                    }
                    else if (parent.pauseAsyncRefreshAfter > 0 && (System.currentTimeMillis() - (parent.lastget > 0 ? parent.lastget : parent.asyncInitedTS)) > (parent.pauseAsyncRefreshAfter * 1000))
                    {
                        Thread.sleep(100);
                    }
                    else
                    {
                        refreshRESULT rrs = parent.refresh();
                        if (rrs.refreshed && parent.onAsyncRefreshed != null)
                            parent.onAsyncRefreshed.call(rrs.dt);
                        lastError = null;
                        Thread.sleep(900);
                    }
                }
                catch (Exception ex)
                {
                    lastError = "AsyncLoadThread.run exception:" + ex.toString().trim() + "\r\n\r\n" + Common.getGoodStackTrace(ex, 0);
                    try { Thread.sleep(10000); } catch (InterruptedException iex) {
                        lastError = "(thread stopped - InterruptedException)" + lastError;
                        throw new RuntimeException(iex);
                    }            
                }
                catch (Throwable ex)
                {
                    lastError = "AsyncLoadThread.run (fatal error, thread stopped):" + ex.toString().trim() + "\r\n\r\n" + Common.getGoodStackTrace(ex, 0);
                    throw ex;
                }
            }
        }
    }
    
    
    public static class DBCACHERESET 
    {
        final DBmethodsCommon dbmethods;
        final String sqlExp;
        final int timeToLive;
        public DBCACHERESET(DBmethodsCommon dbmethods, String sqlExp, int timeToLive)
        {
            this.dbmethods = dbmethods;
            this.sqlExp = sqlExp;
            if (timeToLive <= 0)
                throw new RuntimeException("MUSTNEVERTHROW: timeToLive <= 0");
            this.timeToLive = timeToLive;
        }
        private volatile LocalDateTime value = null;
        private final Object valueLOCK = new Object();
        private volatile long lastrefresh;
        private final DBmethodsCommon.ConnectionID DBCACHERESET_connectionID = new DBmethodsCommon.ConnectionID("DBCACHERESET");
        public LocalDateTime get()
        {
            if (value == null 
            || (lastrefresh == 0 || (System.currentTimeMillis() - lastrefresh) > (timeToLive * 1000))
            )
            {
                synchronized(valueLOCK)
                {
                    if (value == null 
                    || (lastrefresh == 0 || (System.currentTimeMillis() - lastrefresh) > (timeToLive * 1000))
                    )
                    {
                        try
                        {
                            value = (LocalDateTime)dbmethods.select_from(DBCACHERESET_connectionID, sqlExp)[0].Rows.get(0).get(0);
                            lastrefresh = System.currentTimeMillis();
                        }
                        catch(Exception ex)
                        {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            }
            return value;
        }    
//        public LocalDateTime getLastrefresh()
//        {
//            return lastrefresh;
//        }
    }
}
