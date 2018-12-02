package DBmethodsLib;

public class DataColumn {
    public int DataType;
    private String columnName;
    public String getColumnName() { return columnName; }
    public void setColumnName(String value) { columnName = value.toUpperCase(); }
}
