package DBmethodsLib;

import CommonLib.Common;
import java.util.*;

/**
 *
 * 
 */
public class DataRow {//extends LinkedHashMap<String, Object> {
    private final Object DataRowLOCK = new Object();
    private Object[] values;
    final DataTable dataTable_;
    DataRow(DataTable dataTable)
    {
        dataTable_ = dataTable;
        values = new Object[dataTable.Columns.size()];
    }
    public void put(String ColumnName, Object val)
    {
        put(dataTable_.Columns.colIndexes.get(ColumnName), val, true);
    }
    public void put(int ColumnIndex, Object val)
    {
        put(ColumnIndex, val, false);
    }
    private void put(int ColumnIndex, Object val, boolean internalCall)
    {
        if (internalCall && ColumnIndex >= values.length)
        {
            synchronized(DataRowLOCK)
            {
                if (ColumnIndex >= values.length)
                    values = Arrays.copyOf(values, ColumnIndex + 1);
            }
        }
        if (val == null)
            values[ColumnIndex] = DBNull.Value;
        else
            values[ColumnIndex] = val;
    }
    public Object get(String ColumnName)
    {
        Integer ix = dataTable_.Columns.colIndexes.get(ColumnName);
        if (ix == null)
            throw new RuntimeException("Column '" + ColumnName + "' not found in datatable!");
        return get(ix);
    }
    public Object get(int ColumnIndex)
    {
        return values[ColumnIndex];
    }
    public <T>T getNoDBNull(String ColumnName, Class<T> targetClass)
    {
        Object o = get(ColumnName);
        if (o == null || o == DBNull.Value) return null;
        return Common.DirectCast(o, targetClass);
    }
    public <T>T getNoDBNull(int ColumnIndex, Class<T> targetClass)
    {
        Object o = get(ColumnIndex);
        if (o == null || o == DBNull.Value) return null;
        return Common.DirectCast(o, targetClass);
    }
}
