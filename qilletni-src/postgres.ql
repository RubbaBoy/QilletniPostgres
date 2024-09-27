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
     * Query the database.
     *
     * @param[@type string] queryString The query string to execute
     * @returns[@type postgres.Result] The result of the query, containing a ResultSet
     */
    native fun query(queryString)
    
    /**
     * Fetch one row from the database.
     *
     * @param[@type string] queryString The query string to execute
     * @returns[@type postgres.Result] The result of the query, containing a list of the resulting row
     */
    native fun fetchOne(queryString)
    
    /**
     * Fetch all rows from the database.
     *
     * @param[@type string] queryString The query string to execute
     * @returns[@type postgres.Result] The result of the query, containing a 2D list of the resulting rows
     */
    native fun fetchAll(queryString)
    
    /**
     * Update entries in the database.
     *
     * @param[@type string] queryString The query string to execute
     * @returns[@type postgres.Result] How many rows were updated
     */
    native fun update(queryString)
    
    /**
     * Executes a query that does not return any data.
     *
     * @param[@type string] queryString The query string to execute
     * @returns[@type boolean] `true` if the query returned any ResultSets, `false` otherwise
     */
    native fun execute(queryString)
    
    /**
     * Begins a transaction for executing multiple queries.
     */
    native fun beginTransaction(queryString)
    
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

// TODO: Result set
//entity ResultSet {
//    
//}

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
