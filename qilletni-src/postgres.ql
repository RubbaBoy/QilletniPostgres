/**
  * An entity for interacting with a PostgreSQL database.
  */
entity Database {

    string url
    string username
    string password

    Database(url, username, password)
    
    /**
     * Create an unauthenticated reference to a database. No connection occurs yet.
     *
     * @param[@type string] host The hostname of the database
     * @param[@type string] port The port of the database
     * @param[@type string] database The name of the database to connect to
     * @param[@type string] username The username to connect with
     * @param[@type string] password The password to connect with
     */
    static fun createDatabase(host, port, database, username, password) {
        return new Database("jdbc:postgresql://%s:%d/%s".format([host, port, database]), username, password)
    }
    
    /**
     * Create an unauthenticated reference to a database. No connection occurs yet.
     *
     * @param[@type string] url The URL of the database
     * @param[@type string] username The username to connect with
     * @param[@type string] password The password to connect with
     */
    static fun createDatabase(url, username, password) {
        return new Database(url, username, password)
    }
    
    /**
     * Create an unauthenticated reference to a database. No connection occurs yet.
     *
     * @param[@type string] url The URL of the database
     */
    static fun createDatabase(url) {
        return new Database(url, "", "")
    }
    
    /**
     * Create a connection to the database.
     *
     * @returns[@type postgres.Connection] The created connection
     */
    native fun createConnection()
}

/**
 * An entity for holding a single prepared statement.
 */
entity PreparedStatement {

    /**
    * The internal prepared statement.
    * [@type @java java.sql.PreparedStatement]
    */
    java _preparedStatement
    
    PreparedStatement(_preparedStatement)
    
    /**
    * Set a parameter in the prepared statement.
    *
    * @param[@type int] index The 1-indexed index of the parameter to set
    * @param value The value to set the parameter to
    * @returns[@type boolean] If the parameter was set successfully
    */
    native fun setParam(index, value)
    
    /**
    * Close the prepared statement.
    *
    * @returns[@type boolean] If the prepared statement was closed successfully
    */
    native fun close()
}

entity Connection {

    /**
     * The internal connection state.
     * [@type @java java.sql.DriverManager]
     */
    java _connection
    
    Connection(_connection)
    
    /**
     * Check if the connection is connected.
     *
     * @returns[@type boolean] If the connection is connected
     */
    native fun isConnected()
    
    /**
     * Prepare a statement for execution without any parameters given yet.
     *
     * @param[@type string] statementString The statement to prepare
     * @returns[@type postgres.PreparedStatement] The prepared statement
     */
    native fun prepareStatement(statementString)
    
    /**
     * Prepare a statement for execution with pre populated parameters.
     *
     * @param[@type string] statementString The statement to prepare
     * @param[@type list] paramList The list of parameters to prepare
     * @returns[@type postgres.PreparedStatement] The prepared statement
     */
    native fun prepareStatement(statementString, paramList)

    /**
     * Query the database.
     *
     * @param query The query string to execute. Either a [@type string] or a [@type postgres.PreparedStatement]
     * @returns[@type postgres.Result] The result of the query, containing a ResultSet
     */
    native fun query(query)
    
    /**
     * Fetch one row from the database.
     *
     * @param[@type string] query The query string to execute. Either a [@type string] or a [@type postgres.PreparedStatement]
     * @returns[@type postgres.Result] The result of the query, containing a list of the resulting row
     */
    native fun fetchOne(query)
    
    /**
     * Fetch all rows from the database.
     *
     * @param query The query string to execute. Either a [@type string] or a [@type postgres.PreparedStatement]
     * @returns[@type postgres.Result] The result of the query, containing a 2D list of the resulting rows
     */
    native fun fetchAll(query)
    
    /**
     * Update entries in the database.
     *
     * @param query The query string to execute. Either a [@type string] or a [@type postgres.PreparedStatement]
     * @returns[@type postgres.Result] How many rows were updated
     */
    native fun update(query)
    
    /**
     * Executes a query that does not return any data.
     *
     * @param query The query string to execute. Either a [@type string] or a [@type postgres.PreparedStatement]
     * @returns[@type boolean] `true` if the query returned any ResultSets, `false` otherwise
     */
    native fun execute(query)
    
    /**
     * Begins a transaction for executing multiple queries.
     */
    native fun beginTransaction()
    
    /**
     * Commits a transaction.
     */
    native fun commit()

    /**
     * Rolls back a transaction.
     */
    native fun rollback()

    /**
     * Disconnect from the database.
     */
    native fun disconnect()
}

/**
 * Metadata for the result of a query.
 */
entity ResultMetadata {

    string[] columnNames
    string[] columnLabels
    
    ResultMetadata(columnNames, columnLabels)
}

entity ResultSet {
    
    ResultMetadata metadata
    
    /**
     * The internal result set.
     *
     * [@type @java java.sql.ResultSet]
     */
    java _resultSet
    
    /**
     * The statement that created the ResultSet, for closing purposes.
     * This is an Optional that holds a [@type @java java.sql.Statement]. This may not need to be an Optional, and
     * should probably be changed in the future.
     *
     * [@type @java java.util.Optional]
     */
    java _statement
    
    ResultSet(metadata, _resultSet, _statement)
    
    /**
     * Checks if there are more rows to fetch, and moves the ResultSet to the next row.
     *
     * @returns[@type boolean] If there are more rows to fetch
     */
    native fun hasNext()
    
    /**
     * Gets the value from the current row by either the column name or index.
     *
     * @param column The column name or 0-indexed index to get the value from
     * @returns The value of the column
     */
    native fun getValue(column)
    
    /**
     * Closes the ResultSet, freeing it from memory.
     *
     * NOTE: This also closes the resulting statement, if it is not a [@type postgres.PreparedStatement]
     */
    native fun close()
}

/**
 * The result of a database operation that retrieves data.
 */
entity Result {
    
    // The error code of the result, 0 if success
    // Values:
    //   0: Success
    //   1: No rows returned
    //   10: SQL Exception
    //   98: Timeout
    //   99: Database disconnected
    int errorCode
    Optional value
    string message
    
    Result(errorCode, value, message)
    
    /**
     * Create a new error result.
     *
     * @param[@type int] errorCode The error code of the result
     * @param[@type string] message The error message
     * @returns[@type postgres.Result] The error result
     */
    static fun errorResult(errorCode, message) {
        return new Result(errorCode, Optional.fromEmpty(), message)
    }
    
    /**
     * Create a new success result.
     *
     * @param value The value of the result
     * @returns[@type postgres.Result] The success result
     */
    static fun successResult(value) {
        return new Result(0, Optional.fromValue(value), "")
    }
    
    /**
     * Gets the value from the internal optional of the result.
     */
    fun getValue() {
        return value.getValue()
    }
    
    /**
     * Checks if the result is successful.
     *
     * @returns[@type boolean] If the result is successful
     */
    fun isSuccess() {
        return errorCode == 0
    }
    
    fun toString() {
        if (isSuccess()) {
            return "Result(value = %s)".format([value])
        } else {
            return "Result(error = %d, message = %s)".format([errorCode, message])
        }
    }
}
