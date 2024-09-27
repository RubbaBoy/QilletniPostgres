package is.yarr.qilletni.lib.postgres;

import is.yarr.qilletni.api.lang.types.EntityType;
import is.yarr.qilletni.api.lang.types.StringType;
import is.yarr.qilletni.api.lang.types.conversion.TypeConverter;
import is.yarr.qilletni.api.lang.types.entity.EntityInitializer;
import is.yarr.qilletni.api.lang.types.list.ListInitializer;
import is.yarr.qilletni.api.lib.annotations.NativeOn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@NativeOn("Database")
public class DatabaseFunctions {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseFunctions.class);
    
    private final EntityInitializer entityInitializer;
    private final TypeConverter typeConverter;
    private final ListInitializer listInitializer;

    public DatabaseFunctions(EntityInitializer entityInitializer, TypeConverter typeConverter, ListInitializer listInitializer) {
        this.entityInitializer = entityInitializer;
        this.typeConverter = typeConverter;
        this.listInitializer = listInitializer;
    }

    public EntityType createConnection(EntityType entityType) throws SQLException {
        var url = entityType.getEntityScope().<StringType>lookup("url").getValue().getValue();
        var username = entityType.getEntityScope().<StringType>lookup("username").getValue().getValue();
        var password = entityType.getEntityScope().<StringType>lookup("password").getValue().getValue();

        Connection connection;
        
        if (username.isBlank() && password.isBlank()) {
            connection = DriverManager.getConnection(url);
        } else {
            connection = DriverManager.getConnection(url, username, password);
        }
        
        if (connection == null) {
            LOGGER.warn("Unable to create connection to database!");
        }
        
        return entityInitializer.initializeEntity("Connection", connection);
    }
    
}
