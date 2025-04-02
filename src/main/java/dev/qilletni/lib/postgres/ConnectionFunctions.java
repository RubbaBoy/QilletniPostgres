package dev.qilletni.lib.postgres;

import dev.qilletni.api.lang.internal.FunctionInvoker;
import dev.qilletni.api.lang.types.AnyType;
import dev.qilletni.api.lang.types.EntityType;
import dev.qilletni.api.lang.types.FunctionType;
import dev.qilletni.api.lang.types.JavaType;
import dev.qilletni.api.lang.types.QilletniType;
import dev.qilletni.api.lang.types.StaticEntityType;
import dev.qilletni.api.lang.types.StringType;
import dev.qilletni.api.lang.types.conversion.TypeConverter;
import dev.qilletni.api.lang.types.entity.EntityDefinitionManager;
import dev.qilletni.api.lang.types.entity.EntityInitializer;
import dev.qilletni.api.lang.types.list.ListInitializer;
import dev.qilletni.api.lang.types.typeclass.QilletniTypeClass;
import dev.qilletni.api.lib.annotations.BeforeAnyInvocation;
import dev.qilletni.api.lib.annotations.NativeOn;
import dev.qilletni.lib.postgres.exceptions.DatabaseException;
import dev.qilletni.lib.postgres.exceptions.InvalidPreparedStatementType;
import dev.qilletni.lib.postgres.exceptions.InvalidStatementTypeException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
    
    public EntityType prepareStatement(EntityType entityType, String statementString) {
        return prepareStatement(entityType, statementString, Collections.emptyList());
    }

    /**
     * Executes a query() from a {@link QilletniType}, which may either be a {@link StringType} or an
     * {@link EntityType} of PreparedStatement.
     * 
     * @param query The query, either a string or PreparedStatement
     * @return The result of the query
     */
    private QueryResult queryStatement(QilletniType query) throws SQLException {
        return queryStatement(query, -1);
    }

    /**
     * Executes a query() from a {@link QilletniType}, which may either be a {@link StringType} or an
     * {@link EntityType} of PreparedStatement.
     * 
     * @param query The query, either a string or PreparedStatement
     * @return The result of the query
     */
    private QueryResult queryStatement(QilletniType query, int fetchSize) throws SQLException {
        if (query instanceof StringType queryString) {
            var statement = connection.createStatement();
            if (fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }

            var resultSet = statement.executeQuery(queryString.getValue());
            return new QueryResult(resultSet, statement);
        } else if (query instanceof EntityType preparedStatementEntity) {
            var preparedStatement = preparedStatementEntity.getEntityScope().<JavaType>lookup("_preparedStatement").getValue().getReference(PreparedStatement.class);
            
            // Old fetch size might not be relevant now, but may save headache later
            int oldFetchSize = -1;
            if (fetchSize > 0) {
                oldFetchSize = preparedStatement.getFetchSize();
                preparedStatement.setFetchSize(fetchSize);
            }
            
            var resultSet = preparedStatement.executeQuery();
            
            if (fetchSize > 0) {
                preparedStatement.setFetchSize(oldFetchSize);
            }
            
            return new QueryResult(resultSet);
        }
        
        throw new InvalidStatementTypeException("Expected a string or a PreparedStatement, got %s".formatted(query.getTypeClass().getTypeName()));
    }

    /**
     * Executes an update() from a {@link QilletniType}, which may either be a {@link StringType} or an
     * {@link EntityType} of PreparedStatement.
     * 
     * @param query The query, either a string or PreparedStatement
     * @return The number of rows affected
     */
    private int updateStatement(QilletniType query) throws SQLException {
        if (query instanceof StringType queryString) {
            try (var statement = connection.createStatement()) {
                return statement.executeUpdate(queryString.getValue());
            }
        } else if (query instanceof EntityType preparedStatementEntity) {
            var preparedStatement = preparedStatementEntity.getEntityScope().<JavaType>lookup("_preparedStatement").getValue().getReference(PreparedStatement.class);
            return preparedStatement.executeUpdate();
        }
        
        throw new InvalidStatementTypeException("Expected a string or a PreparedStatement, got %s".formatted(query.getTypeClass().getTypeName()));
    }

    /**
     * Executes an execute() from a {@link QilletniType}, which may either be a {@link StringType} or an
     * {@link EntityType} of PreparedStatement.
     * 
     * @param query The query, either a string or PreparedStatement
     * @return If the execution returned a ResultSet
     */
    private boolean executeStatement(QilletniType query) throws SQLException {
        if (query instanceof StringType queryString) {
            try (var statement = connection.createStatement()) {
                return statement.execute(queryString.getValue());
            }
        } else if (query instanceof EntityType preparedStatementEntity) {
            var preparedStatement = preparedStatementEntity.getEntityScope().<JavaType>lookup("_preparedStatement").getValue().getReference(PreparedStatement.class);
            return preparedStatement.execute();
        }
        
        throw new InvalidStatementTypeException("Expected a string or a PreparedStatement, got %s".formatted(query.getTypeClass().getTypeName()));
    }
    
    public EntityType prepareStatement(EntityType entityType, String statementString, List<QilletniType> paramList) {
        try {
            verifyConnection();
            
            List<Object> paramObjects = paramList.stream().map(qilletniType -> {
                if (!(qilletniType instanceof AnyType anyType)) {
                    throw new InvalidPreparedStatementType("Invalid type for PreparedStatement: %s".formatted(qilletniType.getTypeClass().getTypeName()));
                }

                return DatabaseTypeUtility.fromQilletniToNativeJava(anyType);
            }).toList();

            var statement = connection.prepareStatement(statementString);
            for (int i = 0; i < paramObjects.size(); i++) {
                statement.setObject(i + 1, paramObjects.get(i));
            }

            return entityInitializer.initializeEntity("PreparedStatement", statement);
        } catch (SQLException e) {
            return createResult(ErrorType.SQL_EXCEPTION, e.getMessage());
        } catch (DatabaseException e) {
            return createResult(ErrorType.DISCONNECTED, e.getMessage());
        }
    }

    public EntityType query(EntityType entityType, QilletniType query) {
        try {
            verifyConnection();
    
            var queryResult = queryStatement(query);
            var resultSet = queryResult.resultSet();
            var metaData = resultSet.getMetaData();
            var columnNames = new ArrayList<String>();
            var columnLabels = new ArrayList<String>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                columnNames.add(metaData.getColumnName(i));
                columnLabels.add(metaData.getColumnLabel(i));
            }

            var resultMetadata = entityInitializer.initializeEntity("ResultMetadata", listInitializer.createListFromJava(columnNames, QilletniTypeClass.STRING), listInitializer.createListFromJava(columnLabels, QilletniTypeClass.STRING));
            return createResult(entityInitializer.initializeEntity("ResultSet", resultMetadata, resultSet, Optional.ofNullable(queryResult.statement())));
        } catch (SQLException e) {
            return createResult(ErrorType.SQL_EXCEPTION, e.getMessage());
        } catch (DatabaseException e) {
            return createResult(ErrorType.DISCONNECTED, e.getMessage());
        }
    }

    public EntityType fetchOne(EntityType entityType, QilletniType query) {
        try {
            verifyConnection();

            try (var queryResult = queryStatement(query, 1)) {
                var resultSet = queryResult.resultSet();
                
                // Process the first row
                if (resultSet.next()) {
                    var row = new ArrayList<>();
                    int columnCount = resultSet.getMetaData().getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        row.add(resultSet.getObject(i));
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

    public EntityType fetchAll(EntityType entityType, QilletniType query) {
        try {
            var allRows = new ArrayList<QilletniType>();

            try (var queryResult = queryStatement(query)) {
                var resultSet = queryResult.resultSet();

                while (resultSet.next()) {
                    var row = new ArrayList<>();
                    int columnCount = resultSet.getMetaData().getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        row.add(resultSet.getObject(i));
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

    public EntityType update(EntityType entityType, QilletniType query) {
        try {
            verifyConnection();

            return createResult(typeConverter.convertToQilletniType(updateStatement(query)));
        } catch (SQLException e) {
            return createResult(ErrorType.SQL_EXCEPTION, e.getMessage());
        } catch (DatabaseException e) {
            return createResult(ErrorType.DISCONNECTED, e.getMessage());
        }
    }

    public EntityType execute(EntityType entityType, QilletniType query) {
        try {
            verifyConnection();

            // TODO: Handle the actual output of execute! I'm lazy and it likely wouldn't need to be used for a while so oops
            return createResult(typeConverter.convertToQilletniType(executeStatement(query)));
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
