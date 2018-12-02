package DBmethodsLib;

import java.util.*;

/**
 *
 * 
 */
public class DataColumnCollection extends LinkedHashMap<String, DataColumn> {
    private final DataTable dataTable_;
    public static class UpperCaseStringLinkedHashMap<V> extends LinkedHashMap<String, V>
    {
        @Override
        public V put(String key, V value)
        {
            return super.put(key.toUpperCase(), value);
        }
        @Override
        public boolean containsKey(Object key) {
            return super.containsKey(key.toString().toUpperCase());
        }
        @Override
        public V get(Object key) {
            return super.get(key.toString().toUpperCase());
        }
    }
    final UpperCaseStringLinkedHashMap<Integer> colIndexes;
    DataColumnCollection(DataTable dataTable)
    {
        dataTable_ = dataTable;
        colIndexes = new UpperCaseStringLinkedHashMap<>();
    }
    @Override
    public DataColumn put(String key, DataColumn value) 
    {
        if (dataTable_.Rows.size() > 0)
            throw new RuntimeException("Change of columns set after rows added is not supported");
        if (!colIndexes.containsKey(key))
            colIndexes.put(key, colIndexes.size());
        return super.put(key, value);
    }
    @Override
    public DataColumn remove(Object key)
    {
        if (dataTable_.Rows.size() > 0)
            throw new RuntimeException("Change of columns set after rows added is not supported");
        return super.remove(key);
    }
    @Override
    public boolean remove(Object key, Object value)
    {
        if (dataTable_.Rows.size() > 0)
            throw new RuntimeException("Change of columns set after rows added is not supported");
        return super.remove(key, value);
    }
}
