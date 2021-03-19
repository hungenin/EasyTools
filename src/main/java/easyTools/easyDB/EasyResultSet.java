package easyTools.easyDB;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class EasyResultSet{
    private List<String> columnTypes = new ArrayList<>();
    private List<String> columnLabels = new ArrayList<>();
    private List<ArrayList<Object>> rows = new ArrayList<>();

    private Integer rowIterator = null;

    public EasyResultSet(){
    }

    public EasyResultSet(ResultSet resultSet){
        try{
            createColumnTypesAndLabels(resultSet.getMetaData());

            while (resultSet.next()){
                ArrayList<Object> row = saveResultSetToList(resultSet);
                if (row.size() > 0) rows.add(row);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public String getString(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");

        if (!columnTypes.get(columnIndex - 1).equals("String")) throw new ClassCastException("Column type is not String");

        return (String) rows.get(rowIterator).get(columnIndex - 1);
    }
    public String getString(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getString(i + 1);
        }
        return null;
    }

    public Integer getInt(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Integer")) throw new ClassCastException("Column type is not Integer");

        return (Integer) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Integer getInt(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getInt(i + 1);
        }
        return null;
    }

    public Short getShort(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Short")) throw new ClassCastException("Column type is not Short");

        return (Short) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Short getShort(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getShort(i + 1);
        }
        return null;
    }

    public Long getLong(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Long")) throw new ClassCastException("Column type is not Long");

        return (Long) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Long getLong(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getLong(i + 1);
        }
        return null;
    }

    public BigDecimal getBigDecimal(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("BigDecimal")) throw new ClassCastException("Column type is not BigDecimal");

        return (BigDecimal) rows.get(rowIterator).get(columnIndex - 1);
    }
    public BigDecimal getBigDecimal(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getBigDecimal(i + 1);
        }
        return null;
    }

    public Float getFloat(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Float")) throw new ClassCastException("Column type is not Float");

        return (Float) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Float getFloat(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getFloat(i + 1);
        }
        return null;
    }

    public Double getDouble(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Double")) throw new ClassCastException("Column type is not Double");

        return (Double) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Double getDouble(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getDouble(i + 1);
        }
        return null;
    }

    public Boolean getBoolean(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Boolean")) throw new ClassCastException("Column type is not Boolean");

        return (Boolean) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Boolean getBoolean(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getBoolean(i + 1);
        }
        return null;
    }

    public Time getTime(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Time")) throw new ClassCastException("Column type is not Time");

        return (Time) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Time getTime(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getTime(i + 1);
        }
        return null;
    }

    public Timestamp getTimestamp(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Timestamp")) throw new ClassCastException("Column type is not Timestamp");

        return (Timestamp) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Timestamp getTimestamp(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getTimestamp(i + 1);
        }
        return null;
    }

    public Date getDate(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        if (!columnTypes.get(columnIndex - 1).equals("Date")) throw new ClassCastException("Column type is not Date");

        return (Date) rows.get(rowIterator).get(columnIndex - 1);
    }
    public Date getDate(String columnLabel){
        for (int i = 0; i < columnLabel.length(); i++){
            if (columnLabels.get(i).equals(columnLabel.toUpperCase())) return getDate(i + 1);
        }
        return null;
    }


    public Integer columnCount(){
        return columnTypes.size();
    }
    public String getLabel(int columnIndex){
        if (columnIndex < 1 || columnIndex > columnTypes.size()) throw new ArrayIndexOutOfBoundsException("Invalid column index");
        return columnLabels.get(columnIndex - 1);
    }

    public boolean next(){
        if (rowIterator == null){
            if (rows.size() > 0){
                rowIterator = 0;
                return true;
            }
            return false;
        }
        if (rowIterator < rows.size() - 1){
            rowIterator++;
            return true;
        }
        rowIterator = null;
        return false;
    }


    private void createColumnTypesAndLabels(ResultSetMetaData metaData) throws SQLException{
        for (int i = 0; i < metaData.getColumnCount(); i++){
            String columnType = getColumnType(metaData.getColumnTypeName(i + 1));

            if (columnType == null) throw new SQLException("Not implemented column type: " + metaData.getColumnTypeName(i + 1));

            columnTypes.add(columnType);
            columnLabels.add(metaData.getColumnName(i + 1));
        }
    }

    private ArrayList<Object> saveResultSetToList(ResultSet resultSet) throws SQLException{
        ArrayList<Object> row = new ArrayList<>();
        for (int i = 0; i < columnTypes.size(); i++){
            switch (columnTypes.get(i)){
                case "String":
                    row.add(resultSet.getString(i + 1));
                    break;
                case "Integer":
                    row.add(resultSet.getInt(i + 1));
                    break;
                case "Short":
                    row.add(resultSet.getShort(i + 1));
                    break;
                case "Long":
                    row.add(resultSet.getLong(i + 1));
                    break;
                case "Float":
                    row.add(resultSet.getFloat(i + 1));
                    break;
                case "Double":
                    row.add(resultSet.getDouble(i + 1));
                    break;
                case "Boolean":
                    row.add(resultSet.getBoolean(i + 1));
                    break;
                case "Time":
                    row.add(resultSet.getTime(i + 1));
                    break;
                case "Timestamp":
                    row.add(resultSet.getTimestamp(i + 1));
                    break;
                case "Date":
                    row.add(resultSet.getDate(i + 1));
                    break;
                case "BigDecimal":
                    row.add(resultSet.getBigDecimal(i + 1));
                default:
                    throw new SQLException("Unknown column type: " + columnTypes.get(i));
            }
        }

        return row;
    }

    private String getColumnType(String columnType){
        String type = getH2ColumnType(columnType);
        if (type == null) type = getPostgresColumnType(columnType);

        return type;
    }

    private String getH2ColumnType(String h2ColumnType){
        switch (h2ColumnType){
            case "CLOB":
            case "VARCHAR":
            case "LONGVARCHAR":
            case "TEXT":
                return "String";
            case "INTEGER":
                return "Integer";
            case "SMALLINT":
                return "Short";
            case "BIGINT":
                return "Long";
            case "DECIMAL":
                return "BigDecimal";
            case "FLOAT":
                return "Float";
            case "DOUBLE":
                return "Double";
            case "BOOLEAN":
                return "Boolean";
            case "TIME":
                return "Time";
            case "TIMESTAMP":
                return "Timestamp";
            case "DATE":
                return "Date";
            default:
                return null;
        }
    }

    private String getPostgresColumnType(String postgresColumnType){
        switch (postgresColumnType){
            case "varchar":
                return "String";
            case "int4":
                return "Integer";
            default:
                return null;
        }
    }

}
