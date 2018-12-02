package DBmethodsLib;

import java.util.*;

/**
 *
 * 
 */
public class DataTable {
    final public DataRowCollection Rows;
    final public DataColumnCollection Columns;
    public DataTable(){        
        Rows = new DataRowCollection(this);
        Columns = new DataColumnCollection(this);
        Indexes = new DataColumnCollection.UpperCaseStringLinkedHashMap<>();
        IndexesCS = new DataColumnCollection.UpperCaseStringLinkedHashMap<>();
    }            
    final private DataColumnCollection.UpperCaseStringLinkedHashMap<HashMap<Object, DataRow[]>> Indexes;
    final private DataColumnCollection.UpperCaseStringLinkedHashMap<HashMap<Object, DataRow[]>> IndexesCS;
    public void addIndexes(String[] columnNames)
    {
        if (columnNames != null)
            for(String columnName : columnNames)  
            {
                if (!Columns.colIndexes.containsKey(columnName))
                    throw new RuntimeException("Error adding index: column '" + columnName + "' not found in datatable!");
                HashMap<Object, DataRow[]> ix = new HashMap<>();
                HashMap<Object, DataRow[]> ixCS = new HashMap<>();
                for (DataRow r : Rows )
                {
                    DataRow[] ixrws;

                    Object v;
                    v = r.get(columnName).toString().toLowerCase();
                    if (ix.containsKey(v))
                    {
                        DataRow[] ixrwsOld = ix.get(v);
                        ixrws = Arrays.copyOf(ixrwsOld, ixrwsOld.length + 1);
                    }
                    else                        
                        ixrws = new DataRow[1];                    
                    ixrws[ixrws.length - 1] = r;
                    ix.put(v, ixrws);          
                    
                    v = r.get(columnName).toString();
                    if (ixCS.containsKey(v))
                    {
                        DataRow[] ixrwsOld = ixCS.get(v);
                        ixrws = Arrays.copyOf(ixrwsOld, ixrwsOld.length + 1);
                    }
                    else
                        ixrws = new DataRow[1];
                    ixrws[ixrws.length - 1] = r;
                    ixCS.put(v, ixrws);            
                }
                Indexes.put(columnName, ix);
                IndexesCS.put(columnName, ixCS);
            }
    }
    
    public DataRow[] Select(SelectFilter filter)
    {
        DataRow[] result = {};
        if ((filter.type == SelectFilterType.EqualsCI || filter.type == SelectFilterType.EqualsCS) && Indexes.containsKey(filter.columnName))
        {
            DataRow[] res = 
                    filter.type == SelectFilterType.EqualsCS
                    ?                    
                    IndexesCS.get(filter.columnName).get(filter.columnValue.toString())
                    :
                    Indexes.get(filter.columnName).get(filter.columnValue.toString().toLowerCase())
                    ;
            if (res != null)
                result = Arrays.copyOf(res, res.length);
            if (filter.andFilter != null)
                result = internalSelect(result, filter.andFilter);
        }
        else
            result = internalSelect(Rows.toDataRowArray(), filter);
        return result;
    }
    public static DataRow[] Select(DataRow[] rows, SelectFilter filter)
    {
        return internalSelect(rows, filter);
    }
    static DataRow[] internalSelect(DataRow[] rows, SelectFilter filter)
    {
        Objects.requireNonNull(filter);
        DataRow[] preresult = new DataRow[rows.length];
        int result_length = 0;
        for (DataRow r : rows )
        {
            if (
                (filter.type == SelectFilterType.EqualsCI && r.get(filter.columnName).toString().toLowerCase().equals(filter.columnValue.toString().toLowerCase()))
                ||
                (filter.type == SelectFilterType.EqualsCS && r.get(filter.columnName).toString().equals(filter.columnValue.toString()))
                ||
                (filter.type == SelectFilterType.Function && filter.func.call(r))
            )
            {
                preresult[result_length] = r;
                result_length++;
            }
        }
        DataRow[] result = result_length == 0 ? new DataRow[0] : Arrays.copyOf(preresult, result_length);
        if (filter.andFilter != null)
            result = internalSelect(result, filter.andFilter);
        return result;
    }
    
    public Object[][] getData()
    {
        Object[][] result = new Object[this.Rows.size()][];
        for (int rN = 0; rN < this.Rows.size(); rN++)
        {
            DataRow r = this.Rows.get(rN);
            Object[] rr = new Object[this.Columns.size()];
            result[rN] = rr;
            for (int cN = 0; cN < this.Columns.size(); cN++)
            {
                rr[cN] = r.get(cN);
            }
        }
        return result;
    }
    public Object[] getColumnIdentifiers()
    {
        return this.Columns.keySet().toArray();
    }
    
    static public class SelectFilter
    {
        public String columnName;
        public SelectFilterType type;
        public Object columnValue; 
        public SelectFilter andFilter; 
        public SelectFilterFunction func;
        public SelectFilter(String columnName, SelectFilterType type, Object columnValue)
        {
            if (type != SelectFilterType.EqualsCI && type != SelectFilterType.EqualsCS)
                throw new RuntimeException("this constructor if for type = EqualsCI or EqualsCS");
            this.columnName = columnName;
            this.type = type;
            this.columnValue = columnValue;
        }
        public SelectFilter(String columnName, SelectFilterType type, Object columnValue, SelectFilter andFilter)
        {
            this(columnName, type, columnValue);
            this.andFilter = andFilter;
        }
        public SelectFilter(SelectFilterType type, SelectFilterFunction func, SelectFilter andFilter)
        {
            if (type != SelectFilterType.Function)
                throw new RuntimeException("this constructor if for type = Function");
            this.func = func;
            this.andFilter = andFilter;
            this.type = type;
        }
        public SelectFilter(SelectFilterType type, SelectFilterFunction func)
        {
            this(type, func, null);
        }
        @FunctionalInterface
        public static interface SelectFilterFunction
        {
            boolean call(DataRow r);
        }
    }
    static public enum SelectFilterType
    {
        /**
         * Case insensitive
         */
        EqualsCI,
        
        /**
         * Case sensitive
         */
        EqualsCS,
        Function;
    }    
}
