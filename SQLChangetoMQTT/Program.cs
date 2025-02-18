using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Exceptions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;


class Program
{
    private static string? connectionString;
    private static string? ChangeDetectQuery;
    private static string? DataQuery;
    private static string? MqttClientId;
    private static string? IpAdressMqttBroker;
    private static int PortMqttBroker = 1883;
    private static string? MqttTopic;
    private static string? DebugLocation;

    private static IMqttClient? mqttClient;
    private static MqttClientOptions? mqttOptions;
    private static Queue<string> messageCache = new Queue<string>();

    static async Task Main(string[] args)
    {
        using IHost host = Host.CreateApplicationBuilder(args).Build();

        // Ask the service provider for the configuration abstraction.
        IConfiguration config = host.Services.GetRequiredService<IConfiguration>();

        // Read the appsettings.json file from the settings, with default values if not found.
        var server = config.GetValue<string>("DatabaseSettings:Server") ?? "localhost";
        var database = config.GetValue<string>("DatabaseSettings:Database") ?? "pubs";
        var userId = config.GetValue<string>("DatabaseSettings:UserId") ?? "engineer";
        var password = config.GetValue<string>("DatabaseSettings:Password") ?? "engineer";

        connectionString = $"Server={server};Database={database};User Id={userId};Password={password};TrustServerCertificate=True;";
        ChangeDetectQuery = config.GetValue<string>("DatabaseSettings:DataChangeQuery") ?? "SELECT au_id, au_lname, au_fname FROM [dbo].[authors]";
        DataQuery = config.GetValue<string>("DatabaseSettings:DataQuery") ?? "SELECT au_id, au_lname, au_fname FROM [dbo].[authors]";
        MqttClientId = config.GetValue<string>("MqttSettings:ClientId") ?? "ClientID";
        IpAdressMqttBroker = config.GetValue<string>("MqttSettings:IpAddress") ?? "192.168.88.1";
        PortMqttBroker = config.GetValue<int>("MqttSettings:Port");
        MqttTopic = config.GetValue<string>("MqttSettings:Topic") ?? "MSQL/Data";
        DebugLocation = config.GetValue<string>("Logger:location") ?? "Console";

        // Set up logger
        var loggerConfig = new LoggerConfiguration()
          .MinimumLevel.Debug();

        if (DebugLocation == "Console")
        {
            loggerConfig.WriteTo.Console();
        }
        else
        {
            loggerConfig.WriteTo.File("log.txt", shared: true, rollingInterval: RollingInterval.Day);
        }
        Log.Logger = loggerConfig.CreateLogger();

        // Check SQL broker and permissions with retry mechanism
        await CheckSqlBrokerAndPermissions();

        // Set up SQL dependency
        SqlDependency.Start(connectionString);
        await RegisterSqlDependency();

        Log.Logger.Information("Listening for database changes...");

        // Initialize MQTT client
        var factory = new MqttClientFactory();
        mqttClient = factory.CreateMqttClient();
        mqttOptions = new MqttClientOptionsBuilder()
            .WithClientId(MqttClientId)
            .WithTcpServer(IpAdressMqttBroker, PortMqttBroker)
            .Build();

        mqttClient.DisconnectedAsync += async e =>
        {
            Log.Logger.Information("Disconnected from MQTT server. Attempting to reconnect...");
            while (!mqttClient.IsConnected)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5)); // Wait before reconnecting
                    await mqttClient.ConnectAsync(mqttOptions, CancellationToken.None);
                    Log.Logger.Information("Reconnected to MQTT server.");
                    await SendCachedMessages();
                }
                catch (MqttCommunicationException ex)
                {
                    Log.Logger.Error($"Reconnection failed: {ex.Message}");
                }
            }
        };

        // Set up retry parameters
        int retryCount = 0;
        const int maxRetries = 15;
        const int delayBetweenRetries = 6000; // 6 seconds

        // Retry loop for MQTT connection
        while (retryCount < maxRetries)
        {
            try
            {
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(4));
                if (!mqttClient.IsConnected)
                {
                    await mqttClient.ConnectAsync(mqttOptions, cts.Token);
                }
                Log.Logger.Information("Connected to MQTT server.");
                break; // Exit the loop if successful
            }
            catch (Exception ex)
            {
                retryCount++;
                Log.Logger.Error($"Failed to connect to MQTT server: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Log.Logger.Error("Max retry attempts reached. Exiting...");
                    Environment.Exit(1); // Exit the program gracefully
                }
                await Task.Delay(delayBetweenRetries);
            }
        }

        await Task.Delay(Timeout.Infinite); //wait infinitive, but handle all events

        // Clean up
        SqlDependency.Stop(connectionString);
        await mqttClient.DisconnectAsync();
    }

    private static SqlDependency? dependency;

    private static async Task CheckSqlBrokerAndPermissions()
    {
        int retryCount = 0;
        const int maxRetries = 5;
        const int delayBetweenRetries = 5000; // 5 seconds

        while (retryCount < maxRetries)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    // Check if Service Broker is enabled
                    using (SqlCommand command = new SqlCommand("SELECT is_broker_enabled FROM sys.databases WHERE name = @database", connection))
                    {
                        command.Parameters.AddWithValue("@database", connection.Database);
                        var isBrokerEnabled = (bool)(await command.ExecuteScalarAsync() ?? false);
                        if (!isBrokerEnabled)
                        {
                            Log.Logger.Error("Service Broker is not enabled on the database.");
                            Environment.Exit(1); // Exit the program gracefully
                        }
                    }

                    // Check if the user has the necessary permissions
                    using (SqlCommand command = new SqlCommand("SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'SUBSCRIBE QUERY NOTIFICATIONS')", connection))
                    {
                        var hasPermissions = (int)(await command.ExecuteScalarAsync() ?? 0);
                        if (hasPermissions == 0)
                        {
                            Log.Logger.Error("User does not have permission to subscribe to query notifications.");
                            Environment.Exit(1); // Exit the program gracefully
                        }
                    }
                }
                Log.Logger.Information("SQL connection successful"); 
                break; // Exit the loop if successful
            }
            catch (SqlException ex)
            {
                retryCount++;
                Log.Logger.Error($"SQL connection error: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Log.Logger.Error("Max retry attempts reached. Exiting...");
                    Environment.Exit(1); // Exit the program gracefully
                }
                await Task.Delay(delayBetweenRetries);
            }
        }
    }

    private static async Task RegisterSqlDependency()
    {
        int retryCount = 0;
        const int maxRetries = 20;
        const int delayBetweenRetries = 5000; // 5 seconds

        while (retryCount < maxRetries)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    using (SqlCommand command = new SqlCommand(ChangeDetectQuery, connection))
                    {
                        dependency = new SqlDependency(command);
                        dependency.OnChange += new OnChangeEventHandler(OnDatabaseChange);

                        await connection.OpenAsync();
                        await command.ExecuteReaderAsync();
                        if (retryCount > 0) { Log.Logger.Information("Connection to SQL server re-established, Listening for database changes... "); }
                    }
                }
                break; // Exit the loop if successful
            }
            catch (SqlException ex)
            {
                retryCount++;
                Log.Logger.Error($"SQL connection error: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Log.Logger.Error("Max retry attempts reached. Exiting...");
                    Environment.Exit(1); // Exit the program gracefully
                }
                await Task.Delay(delayBetweenRetries);
            }
        }
    }

    private static async void OnDatabaseChange(object sender, SqlNotificationEventArgs e)
    {
        switch (e.Type)
        {
            case SqlNotificationType.Change:
                if (e.Info != SqlNotificationInfo.Error)
                {
                    Log.Logger.Information("Database change detected!");

                    // Fetch new data
                    string? newData = await FetchNewData();

                    // Transmit new data via MQTT
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(MqttTopic)
                        .WithPayload(newData)
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                        .WithRetainFlag()
                        .Build();

                    if (mqttClient?.IsConnected == true)
                    {
                        try
                        {
                            await mqttClient.PublishAsync(message, CancellationToken.None);
                            Log.Logger.Information("Data transmitted via MQTT.");
                        }
                        catch (MqttCommunicationException ex)
                        {
                            Log.Logger.Error($"Failed to send MQTT message: {ex.Message}");
                            if (newData != null)
                            {
                                messageCache.Enqueue(newData);
                            }
                        }
                    }
                    else
                    {
                        if (newData != null)
                        {
                            Log.Logger.Information("Write Change to Cache");
                            messageCache.Enqueue(newData);
                        }
                    }

                    // Re-register dependency with retry mechanism
                    await RetryRegisterSqlDependency();
                }
                else
                {
                    Log.Logger.Error("SQL server shutdown detected. Attempting to re-register dependency...");
                    // Re-register dependency with retry mechanism
                    await RetryRegisterSqlDependency();
                }
                break;

            case SqlNotificationType.Subscribe:
                if (e.Info == SqlNotificationInfo.Error)
                {
                    Log.Logger.Error("SQL server shutdown detected. Attempting to re-register dependency...");
                    await RetryRegisterSqlDependency();
                }
                break;

            case SqlNotificationType.Unknown:
                Log.Logger.Information("Unknown SQL notification type received.");
                break;

            default:
                Log.Logger.Error($"Unhandled SQL notification type: {e.Type}, Info: {e.Info}, Source: {e.Source}");
                break;
        }
    }

    private static async Task RetryRegisterSqlDependency()
    {
        int retryCount = 0;
        const int maxRetries = 5;
        const int delayBetweenRetries = 5000; // 5 seconds

        while (retryCount < maxRetries)
        {
            try
            {
                await RegisterSqlDependency();
                break; // Exit the loop if successful
            }
            catch (SqlException ex)
            {
                retryCount++;
                Log.Logger.Error($"SQL connection error during re-registration: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Log.Logger.Error("Max retry attempts reached. Exiting...");
                    Environment.Exit(1); // Exit the program gracefully
                }
                await Task.Delay(delayBetweenRetries);
            }
        }
    }

    private static async Task<string?> FetchNewData()
    {
        int retryCount = 0;
        const int maxRetries = 5;
        const int delayBetweenRetries = 5000; // 5 seconds

        while (retryCount < maxRetries)
        {
            try
            {
                var results = new List<Dictionary<string, object>>();
                var timestamp = DateTime.UtcNow;

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    using (SqlCommand command = new SqlCommand(DataQuery, connection))
                    {
                        await connection.OpenAsync();
                        using (SqlDataReader reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var row = new Dictionary<string, object>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row[reader.GetName(i)] = reader.GetValue(i);
                                }
                                results.Add(row);
                            }
                        }
                    }
                }

                var payload = new
                {
                    Timestamp = timestamp,
                    RecordCount = results.Count,
                    Data = results
                };

                return JsonSerializer.Serialize(payload);
            }
            catch (SqlException ex)
            {
                retryCount++;
                Log.Logger.Error($"SQL query error: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Log.Logger.Error("Max retry attempts reached. Returning null...");
                    return null;
                }
                await Task.Delay(delayBetweenRetries);
            }
        }
        return null;
    }

    private static async Task SendCachedMessages()
    {
        while (messageCache.Count > 0)
        {
            var cachedMessage = messageCache.Dequeue();
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(MqttTopic)
                .WithPayload(cachedMessage)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                .WithRetainFlag()
                .Build();

            try
            {
                await mqttClient.PublishAsync(message, CancellationToken.None);
                Log.Logger.Information("Cached data transmitted via MQTT.");
            }
            catch (MqttCommunicationException ex)
            {
                Log.Logger.Error($"Failed to send cached MQTT message: {ex.Message}");
                messageCache.Enqueue(cachedMessage);
                break;
            }
        }
    }
}

