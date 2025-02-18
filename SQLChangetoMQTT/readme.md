# SQLChangetoMQTT

## Description

SQLChangetoMQTT is a C# application that listens for changes in a SQL Server database and publishes the changes to an MQTT broker. The application uses SQL Server Service Broker to detect changes in the database and MQTTnet to communicate with the MQTT broker. It also includes a retry mechanism for both SQL Server and MQTT connections to ensure reliability.

## Features

- Listens for changes in a SQL Server database using SQL Server Service Broker.
- Publishes database changes to an MQTT broker.
- Supports MQTT authentication with username and password.
- Includes retry mechanisms for SQL Server and MQTT connections.
- Logs events and errors using Serilog.

## Requirements

- .NET 8
- SQL Server with Service Broker enabled
- MQTT broker

## Configuration

The application is configured using an `appsettings.json` file. Below is an example configuration:

### appsettings.json

### Configuration Parameters

- `DatabaseSettings`
  - `Server`: The SQL Server instance.
  - `Database`: The database name.
  - `UserId`: The SQL Server user ID.
  - `Password`: The SQL Server user password.
  - `DataChangeQuery`: The query to detect data changes.
  - `DataQuery`: The query to fetch new data.

- `MqttSettings`
  - `ClientId`: The MQTT client ID.
  - `IpAddress`: The IP address of the MQTT broker.
  - `Port`: The port of the MQTT broker.
  - `Topic`: The MQTT topic to publish data to.
  - `Username`: The MQTT username (optional).
  - `Password`: The MQTT password (optional).

- `Logger`
  - `location`: The logging location (`Console` or `File`).

## Usage

1. Clone the repository.
2. Update the `appsettings.json` file with your configuration.
3. Build and run the application using Visual Studio 2022 or the .NET CLI.

## Adding a User with Login Authorization for SqlDependency

To use `SqlDependency`, the SQL Server user must have the necessary permissions. Follow these steps to create a user with the required permissions:

1. **Create a Login:**
```
CREATE LOGIN [engineer] WITH PASSWORD = 'engineer';
```
2. **Create a User in the Database:**
```
USE [pubs]; CREATE USER [engineer] FOR LOGIN [engineer];
```
3. **Grant Permissions:**
```
USE [pubs]; GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [engineer]; 
GRANT RECEIVE ON QueryNotificationErrorsQueue TO [engineer]; 
GRANT REFERENCES ON CONTRACT::[http://schemas.microsoft.com/SQL/Notifications/PostQueryNotification] TO [engineer];
```

4. **Enable Service Broker:**

Ensures that the Service Broker is enabled for the database:
```
ALTER DATABASE [pubs] SET ENABLE_BROKER;
```
   


   
   
