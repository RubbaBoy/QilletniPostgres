package is.yarr.qilletni.lib.postgres;

import is.yarr.qilletni.api.lang.internal.FunctionInvoker;
import is.yarr.qilletni.api.lang.types.EntityType;
import is.yarr.qilletni.api.lang.types.FunctionType;
import is.yarr.qilletni.api.lang.types.JavaType;
import is.yarr.qilletni.api.lang.types.QilletniType;
import is.yarr.qilletni.api.lang.types.StaticEntityType;
import is.yarr.qilletni.api.lang.types.conversion.TypeConverter;
import is.yarr.qilletni.api.lang.types.entity.EntityDefinitionManager;
import is.yarr.qilletni.api.lang.types.entity.EntityInitializer;
import is.yarr.qilletni.api.lang.types.list.ListInitializer;
import is.yarr.qilletni.api.lang.types.typeclass.QilletniTypeClass;
import is.yarr.qilletni.api.lib.annotations.BeforeAnyInvocation;
import is.yarr.qilletni.api.lib.annotations.NativeOn;
import is.yarr.qilletni.lib.postgres.exceptions.DatabaseException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@NativeOn("Connection")
public class ConnectionFunctions {

    private final EntityInitializer entityInitializer;
    private final FunctionInvoker functionInvoker;
    private final EntityDefinitionManager entityDefinitionManager;
    private final TypeConverter typeConverter;
    private final ListInitializer listInitializer;

    private final StaticEntityType staticResult;
    private final FunctionType errorResultFunction;
    private final FunctionType successResultFunction;

    private Connection connection;

    public ConnectionFunctions(EntityInitializer entityInitializer, FunctionInvoker functionInvoker, EntityDefinitionManager entityDefinitionManager, TypeConverter typeConverter, ListInitializer listInitializer) {
        this.entityInitializer = entityInitializer;
        this.functionInvoker = functionInvoker;
        this.entityDefinitionManager = entityDefinitionManager;
        this.typeConverter = typeConverter;
        this.listInitializer = listInitializer;

        staticResult = entityDefinitionManager.lookup("Result").createStaticInstance();
        errorResultFunction = staticResult.getEntityScope().lookupFunction("errorResult", 2, staticResult.getTypeClass()).getValue();
        successResultFunction = staticResult.getEntityScope().lookupFunction("successResult", 1, staticResult.getTypeClass()).getValue();
    }

    @BeforeAnyInvocation
    public void setupConnection(EntityType entityType) {
        connection = entityType.getEntityScope().<JavaType>lookup("_connection").getValue().getReference(Connection.class);
    }

    public boolean isConnected(EntityType entityType) {
        try {
            return connection != null && !connection.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    public EntityType query(EntityType entityType, String queryString) throws SQLException {
        verifyConnection();

        var statement = connection.createStatement();
        var resultSet = statement.executeQuery(queryString);

        var metaData = resultSet.getMetaData();
        var columnNames = new ArrayList<String>();
        var columnLabels = new ArrayList<String>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columnNames.add(metaData.getColumnName(i));
            columnLabels.add(metaData.getColumnLabel(i));
        }

        var resultMetadata = entityInitializer.initializeEntity("ResultMetadata", listInitializer.createListFromJava(columnNames, QilletniTypeClass.STRING), listInitializer.createListFromJava(columnLabels, QilletniTypeClass.STRING));
        return createResult(entityInitializer.initializeEntity("ResultSet", resultMetadata, resultSet));
    }

    public EntityType fetchOne(EntityType entityType, String queryString) {
        try {
            verifyConnection();

            var statement = connection.createStatement();
            statement.setFetchSize(1);

            try (var rs = statement.executeQuery(queryString)) {
                // Process the first row
                if (rs.next()) {
                    var row = new ArrayList<>();
                    int columnCount = rs.getMetaData().getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        row.add(rs.getObject(i));
                    }

                    return createResult(listInitializer.createListFromJava(row));
                }
            }

            return createResult(ErrorType.NO_ROWS_RETURNED);
        } catch (SQLException e) {
            return createResult(ErrorType.SQL_EXCEPTION, e.getMessage());
        } catch (DatabaseException e) {
            return createResult(ErrorType.DISCONNECTED, e.getMessage());
        }
    }

    public EntityType fetchAll(EntityType entityType, String queryString) {
        try {
            var statement = connection.createStatement();
            statement.setFetchSize(1);

            var allRows = new ArrayList<QilletniType>();

            try (var rs = statement.executeQuery(queryString)) {
                while (rs.next()) {
                    var row = new ArrayList<>();
                    int columnCount = rs.getMetaData().getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        row.add(rs.getObject(i));
                    }

                    allRows.add(listInitializer.createListFromJava(row));
                }
            }

            return createResult(listInitializer.createList(allRows));
        } catch (SQLException e) {
            return createResult(ErrorType.SQL_EXCEPTION, e.getMessage());
        } catch (DatabaseException e) {
            return createResult(ErrorType.DISCONNECTED, e.getMessage());
        }
    }

    public EntityType update(EntityType entityType, String queryString) {
        try {
            verifyConnection();

            var statement = connection.createStatement();
            return createResult(typeConverter.convertToQilletniType(statement.executeUpdate(queryString)));
        } catch (SQLException e) {
            return createResult(ErrorType.SQL_EXCEPTION, e.getMessage());
        } catch (DatabaseException e) {
            return createResult(ErrorType.DISCONNECTED, e.getMessage());
        }
    }

    public EntityType execute(EntityType entityType, String queryString) {
        try {
            verifyConnection();

            var statement = connection.createStatement();
            return createResult(typeConverter.convertToQilletniType(statement.execute(queryString)));
        } catch (SQLException e) {
            return createResult(ErrorType.SQL_EXCEPTION, e.getMessage());
        } catch (DatabaseException e) {
            return createResult(ErrorType.DISCONNECTED, e.getMessage());
        }
    }

    public boolean beginTransaction(EntityType entityType) {
        try {
            verifyConnection();
            
            connection.setAutoCommit(false);
            
            return true;
        } catch (SQLException | DatabaseException e) {
            return false;
        }
    }

    public boolean commit(EntityType entityType) {
        try {
            verifyConnection();
            
            connection.commit();
            connection.setAutoCommit(true);

            return true;
        } catch (SQLException | DatabaseException e) {
            return false;
        }
    }

    public boolean rollback(EntityType entityType) {
        try {
            verifyConnection();
            
            connection.rollback();
            connection.setAutoCommit(true);

            return true;
        } catch (SQLException | DatabaseException e) {
            return false;
        }
    }

    public boolean disconnect(EntityType entityType) {
        try {
            verifyConnection();
            
            connection.close();

            return true;
        } catch (SQLException | DatabaseException e) {
            return false;
        }
    }

    private void verifyConnection() throws SQLException {
        if (connection == null) {
            throw new DatabaseException("Connection is not initialized");
        }

        if (connection.isClosed()) {
            throw new DatabaseException("Connection is closed");
        }
    }

    private EntityType createResult(ErrorType errorType, String... format) {
        // Invoke the static method Result.errorResult(errorCode, errorMessage)
        return functionInvoker.invokeFunctionWithResult(errorResultFunction, List.of(typeConverter.convertToQilletniType(errorType.getCode()), typeConverter.convertToQilletniType(errorType.getMessage().formatted((Object[]) format))));
    }

    private EntityType createResult(QilletniType value) {
        // Invoke the static method Result.successResult(value)
        return functionInvoker.invokeFunctionWithResult(successResultFunction, List.of(value));
    }

    enum ErrorType {
        SUCCESS(0, "Success"),
        NO_ROWS_RETURNED(1, "No rows returned"),
        SQL_EXCEPTION(10, "SQL Exception: %s"),
        DISCONNECTED(99, "Database disconnected: %s");

        private final int code;
        private final String message;

        ErrorType(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }

}
