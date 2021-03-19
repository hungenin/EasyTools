package easyTools.easyDB;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;

public class EasyDB{
    private String username;
    private String password;
    private String databaseUrl;

    public EasyDB(String databaseUrl, String username, String password){
        this.username = username;
        this.password = password;
        this.databaseUrl = databaseUrl;
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        return DriverManager.getConnection(databaseUrl, username, password);
    }

    public Integer executeUpdate(String SQL, Object... params){
        Integer generatedKey = null;
        try (Connection connection = getConnection()){
            PreparedStatement statement = connection.prepareStatement(SQL, PreparedStatement.RETURN_GENERATED_KEYS);

            for (int i = 0; i < params.length; i++){
                setParameterInStatement(i + 1, params[i], statement);
            }

            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();

            resultSet.next();
            if (resultSet.getMetaData().getColumnCount() > 0) generatedKey = resultSet.getInt(1);

            statement.close();
        }catch (SQLException | ClassNotFoundException e){
            e.printStackTrace();
        }
        return generatedKey;
    }

    public EasyResultSet executeQuery(String SQL, Object... params){
        EasyResultSet easyResultSet = null;
        try (Connection connection = getConnection()){
            PreparedStatement statement = connection.prepareStatement(SQL);

            for (int i = 0; i < params.length; i++){
                setParameterInStatement(i + 1, params[i], statement);
            }

            ResultSet resultSet = statement.executeQuery();
            easyResultSet = new EasyResultSet(resultSet);

            statement.close();
        }catch (SQLException | ClassNotFoundException e){
            e.printStackTrace();
            easyResultSet = new EasyResultSet();
        }
        return easyResultSet;
    }

    private void setParameterInStatement(int parameterIndex, Object parameter, PreparedStatement statement) throws SQLException{
        if (parameter instanceof String) statement.setString(parameterIndex, (String) parameter);
        else if (parameter instanceof Integer) statement.setInt(parameterIndex, (Integer) parameter);
        else if (parameter instanceof Short) statement.setShort(parameterIndex, (Short) parameter);
        else if (parameter instanceof Long) statement.setLong(parameterIndex, (Long) parameter);
        else if (parameter instanceof Float) statement.setFloat(parameterIndex, (Float) parameter);
        else if (parameter instanceof Double) statement.setDouble(parameterIndex, (Double) parameter);
        else if (parameter instanceof BigDecimal) statement.setBigDecimal(parameterIndex, (BigDecimal) parameter);
        else if (parameter instanceof Boolean) statement.setBoolean(parameterIndex, (Boolean) parameter);
        else if (parameter instanceof Time) statement.setTime(parameterIndex, (Time) parameter);
        else if (parameter instanceof Timestamp) statement.setTimestamp(parameterIndex, (Timestamp) parameter);
        else if (parameter instanceof Date) statement.setDate(parameterIndex, (Date) parameter);
        else if (parameter instanceof URL) statement.setURL(parameterIndex, (URL) parameter);
        else throw new SQLFeatureNotSupportedException("EasyDB error - Data type not implemented yet...");
    }
}
