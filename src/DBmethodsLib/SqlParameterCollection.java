package DBmethodsLib;

import java.util.Arrays;
import java.util.LinkedHashMap;

public class SqlParameterCollection extends LinkedHashMap<String, SqlParameter> {
    public SqlParameterCollection() { }
    public SqlParameterCollection(SqlParameter p1) { Add(p1); }
    public SqlParameterCollection(SqlParameter p1,SqlParameter p2) { Add(p1);Add(p2); }
    public SqlParameterCollection(SqlParameter p1,SqlParameter p2,SqlParameter p3) { Add(p1);Add(p2);Add(p3); }
    public SqlParameterCollection(SqlParameter p1,SqlParameter p2,SqlParameter p3,SqlParameter p4) { Add(p1);Add(p2);Add(p3);Add(p4); }
    public SqlParameterCollection(SqlParameter p1,SqlParameter p2,SqlParameter p3,SqlParameter p4,SqlParameter p5) { Add(p1);Add(p2);Add(p3);Add(p4);Add(p5); }
    
    public final SqlParameter Add(SqlParameter parameter)
    {
        this.put(parameter.ParameterName, parameter);
        return parameter;                
    }

    public SqlParameter add_param(String parname, Object parvalue, DbType partype)
    {
        SqlParameter p = new SqlParameter(parname, parvalue,  partype);
        this.put(parname, p);
        return p;                
    }
    public SqlParameter add_param(String parname, Object parvalue, DbType partype, SqlParameter.ParameterDirection direction)
    {
        SqlParameter p = new SqlParameter(parname, parvalue,  partype, direction);
        this.put(parname, p);
        return p;                
    }
    
    public SqlParameter[] toArray() {
        return Arrays.copyOf(values().toArray(), size(), SqlParameter[].class);
    }
}
