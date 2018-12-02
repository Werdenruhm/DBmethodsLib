package DBmethodsLib;

import CommonLib.Common;

public class SqlParameter {
    public SqlParameter(String parameterName, Object value, DbType targetSqlType)
    {
        ParameterName = parameterName; Value = value; DbType = targetSqlType; Direction = ParameterDirection.Input;
    }
    public SqlParameter(String parameterName, Object value, DbType targetSqlType, int size)
    {
        ParameterName = parameterName; Value = value; DbType = targetSqlType; Direction = ParameterDirection.Input; Size = size;
    }
    public SqlParameter(String parameterName, Object value, DbType targetSqlType, ParameterDirection direction)
    {
        ParameterName = parameterName; Value = value; DbType = targetSqlType; Direction = direction;
    }
    public SqlParameter(String parameterName, Object value, DbType targetSqlType, int size, ParameterDirection direction)
    {
        ParameterName = parameterName; Value = value; DbType = targetSqlType; Direction = direction; Size = size;
    }
    
    public enum ParameterDirection
    {
        Input,
        Output
    }
    
    public String ParameterName;
    public Object Value;
    public DbType DbType;
    public int Size;
    public ParameterDirection Direction;
    int ParameterIndex;
    
    public <T>T getNoDBNullValue(Class<T> targetClass)
    {
        if (Value == null || Value == DBNull.Value) return null;
        return Common.DirectCast(Value, targetClass);
    }
}
