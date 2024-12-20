import "postgres:postgres.ql"

print("Creating table...")

Database database = Database.createDatabase("localhost", 5444, "example_db", "admin", "pass")
Connection connection = database.createConnection()

if (connection.isConnected()) {
    print("Connected to database!")
} else {
    print("Failed to connect to database!")
}

PreparedStatement statement = connection.prepareStatement("INSERT INTO example_table (name) VALUES (?)")

string[] names = ["foo", "baz"]

for (name : names) {
    statement.setParam(1, name)
    Result result = connection.update(statement)
    
    if (result.isSuccess()) {
        print("Row insert result = %s".format([result]))
    } else {
        print("Failed to insert row!")
    }
}

Result result = connection.query("SELECT * FROM example_table")

if (result.isSuccess()) {
    ResultSet rs = result.getValue()

    print("Fetched rows:")
    string[] labels = rs.metadata.columnLabels
    print(" %s  %s".format(labels))
    
    for (rs.hasNext()) {
        print("%3d  %s".format([rs.getValue(0), rs.getValue(1)]))
    }
} else {
    print("Failed to fetch rows: %s".format([result.message]))
}
