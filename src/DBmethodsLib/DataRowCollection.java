package DBmethodsLib;

import java.util.*;

/**
 *
 * 
 */
public class DataRowCollection extends ArrayList<DataRow> {
    private final DataTable dataTable_;
    DataRowCollection(DataTable dataTable)
    {
        dataTable_ = dataTable;
    }
    DataRow add()
    {
        DataRow r = new DataRow(dataTable_);
        super.add(r);
        return r;
    }
    public DataRow[] toDataRowArray()
    {
        return Arrays.copyOf(this.toArray(), this.size(), DataRow[].class);
    }
    public DataRow singleRowOrThrow()
    {
        return singleRowOrThrow("No rows were selected", "More than one row were selected");
    }
    public DataRow singleRowOrThrow(String emptyErrMsg, String moreThanOneErrMsg)
    {
        if (isEmpty())
            throw new RuntimeException(emptyErrMsg);
        if (size() > 1)
            throw new RuntimeException(moreThanOneErrMsg);
        return get(0);
    }
}
