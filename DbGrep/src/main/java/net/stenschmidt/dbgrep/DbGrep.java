package net.stenschmidt.dbgrep;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DbGrep {

    private static boolean _currentTablePrinted;
    private static String _currentTABLE;
    private static String _currentTABLE_FIELDS;
    
    public static void main(String... args) {

        try {
            try (Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/orcl", "scott",
                    "*****")) {

                String delimiter = ";";
                String tableSchemaPattern = "SCOTT";
                String tableNamePattern = "%";
                
                //TODO: Support case
                String filter = "EMP";

                int timeout = 5;

                try (Statement statement = connection.createStatement()) {

                    List<String> tablesList = getTablesList(connection, delimiter, tableSchemaPattern,
                            tableNamePattern);
                    //tablesList.forEach(System.out::println);

                    statement.setQueryTimeout(timeout);

                    tablesList.forEach(table -> {
                        
                        _currentTablePrinted = false;
                        String sql = String.format("SELECT * FROM %s", table);
                        _currentTABLE = String.format("TABLE        : %s", table);
                        conditionalPrintTable(_currentTABLE, filter);

                        ResultSet resultSet;
                        try {
                            resultSet = statement.executeQuery(sql);
                            ResultSetMetaData metaData = resultSet.getMetaData();

                            String headers = "";
                            for (int x = 1; x < metaData.getColumnCount() + 1; x++) {
                                headers += (x == 1 ? "" : delimiter) + metaData.getColumnLabel(x);
                            }
                            
                            _currentTABLE_FIELDS = String.format("TABLE_FIELDS : %s", headers); 
                            conditionalPrint(_currentTABLE_FIELDS, filter);
                            printTableRow(resultSet, filter, delimiter);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<String> getTablesList(Connection connection, String delimiter, String tableSchemaPattern,
            String tableNamePattern) throws SQLException {

        List<String> result = new ArrayList<String>();
        ResultSet dbTables = connection.getMetaData().getTables(null, tableSchemaPattern, tableNamePattern, null);
        while (dbTables.next()) {
            result.add(dbTables.getString("TABLE_NAME"));
        }
        return result;
    }

    public static void printTableRow(ResultSet resultSet, String filter, String delimiter) throws SQLException {
        while (resultSet.next()) {
            int colcount = resultSet.getMetaData().getColumnCount();
            String row = "";
            for (int x = 1; x < colcount + 1; x++) {
                row += (x == 1 ? "" : delimiter) + resultSet.getObject(x);
            }
            conditionalPrint(String.format("TABLE_ROW    : %s", row), filter);
        }
    }
    
    public static void conditionalPrintTable(String printMsg, String filter) {
        _currentTablePrinted = conditionalPrint(printMsg, filter);
    }
    
    public static boolean conditionalPrint(String printMsg, String filter) {
        boolean result = false;
        if(printMsg.contains(filter) || filter.equals("*") || filter.equals("%")) {
            if (!_currentTablePrinted) {
                System.out.println(_currentTABLE);
                _currentTablePrinted = true;
            }
            System.out.println(printMsg);
            result = true;
        }
        return result;
    }
}