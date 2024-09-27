import "postgres:postgres.ql"

print("Creating table...")

Database database = Database.createDatabase("localhost", 5444, "example_db", "admin", "pass")
Connection connection = database.createConnection()

if (connection.isConnected()) {
    print("Connected to database!")
} else {
    print("Failed to connect to database!")
}

Result result = connection.execute("CREATE TABLE IF NOT EXISTS example_table (id SERIAL PRIMARY KEY, name VARCHAR(255))")

print("Result: %s".format([result]))

//result = connection.execute("INSERT INTO example_table (name) VALUES ('Alice')")
//
//print("Row insert result = %s".format([result]))

result = connection.fetchAll("SELECT * FROM example_table")

if (result.isSuccess()) {
    print("Fetched rows:")
    print(" ID  Name")
    for (row : result.getValue()) {
        print("%3d  %s".format([row[0], row[1]]))
    }
} else {
    print("Failed to fetch rows!")
}
