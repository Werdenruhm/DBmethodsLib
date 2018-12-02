package DBmethodsLib;

import CommonLib.Common;
import java.time.LocalDateTime;
import java.util.HashMap;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 *
 * 
 */
public class AnyDictionaryTable extends AnyTable
{          
    private static final HashMap<DBmethodsCommon, DBCACHERESET> cchreses = new HashMap<>();
    private static final Object cchresesLOCK = new Object();
    private        final String dBCacheResetSQLexpr;
    private static final int    cchresI = 30;
    public AnyDictionaryTable(DBmethodsCommon dbmethods, String sqlexpr, String dBCacheResetSQLexpr)
    {
        super(dbmethods, sqlexpr, null, "pooled", -1, true);
        this.dBCacheResetSQLexpr = dBCacheResetSQLexpr;
        constructorSuffix(dbmethods);
    }
    public AnyDictionaryTable(DBmethodsCommon dbmethods, String sqlexpr, String[] indexes, String dBCacheResetSQLexpr)
    {
        super(dbmethods, sqlexpr, indexes, "pooled", -1, true);
        this.dBCacheResetSQLexpr = dBCacheResetSQLexpr;
        constructorSuffix(dbmethods);
    }
    public AnyDictionaryTable(DBmethodsCommon dbmethods, String sqlexpr, String[] indexes, Common.Action1<DataTable> onAsyncRefreshed, String dBCacheResetSQLexpr)
    {
        super(dbmethods, sqlexpr, indexes, "pooled", -1, true, onAsyncRefreshed);
        this.dBCacheResetSQLexpr = dBCacheResetSQLexpr;
        constructorSuffix(dbmethods);
    }
    private void constructorSuffix(DBmethodsCommon dbmethods)
    {
        if (!cchreses.containsKey(dbmethods))
            synchronized (cchresesLOCK)
            {
                if (!cchreses.containsKey(dbmethods))
                    cchreses.put(dbmethods, new DBCACHERESET(dbmethods, dBCacheResetSQLexpr, cchresI));
            }
    }
    @Override
    protected LocalDateTime getCACHERESET() { return cchreses.get(dbmethods).get(); }

    static public CompiledScript getCompiledScript(DataRow r, String scriptColName, String compiledScriptColName)
    {
        if (r.get(compiledScriptColName) == DBNull.Value)
        {
            ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine engine = manager.getEngineByName("js");
            CompiledScript result;
            try {
                result = ((Compilable)engine).compile(r.get(scriptColName).toString());
            } catch (ScriptException ex) {
                throw new RuntimeException(ex);
            }                    
            r.put(compiledScriptColName, result);
            return result;                
        }
        else
            return (CompiledScript)r.get(compiledScriptColName);
    }
}
