using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Exceptions;


class Program
{
    private static string connectionString = "Server=localhost;Database=pubs;Integrated Security=True;TrustServerCertificate=True;";
    private static string query = "SELECT au_id, au_lname, au_fname FROM [dbo].[authors]";
    private static IMqttClient? mqttClient;
    private static MqttClientOptions? mqttOptions;
    private static Queue<string> messageCache = new Queue<string>();

    static async Task Main(string[] args)
    {
        // Initialize MQTT client
        var factory = new MqttClientFactory();
        mqttClient = factory.CreateMqttClient();
        mqttOptions = new MqttClientOptionsBuilder()
            .WithClientId("ClientID")
            .WithTcpServer("192.168.88.1", 1883)
            .Build();

        mqttClient.DisconnectedAsync += async e =>
        {
            Console.WriteLine("Disconnected from MQTT server. Attempting to reconnect...");
            while (!mqttClient.IsConnected)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5)); // Wait before reconnecting
                    await mqttClient.ConnectAsync(mqttOptions, CancellationToken.None);
                    Console.WriteLine("Reconnected to MQTT server.");
                    await SendCachedMessages();
                }
                catch (MqttCommunicationException ex)
                {
                    Console.WriteLine($"Reconnection failed: {ex.Message}");
                }
            }
        };

        try
        {
            await mqttClient.ConnectAsync(mqttOptions, CancellationToken.None);
        }
        catch (MqttCommunicationException ex)
        {
            Console.WriteLine($"Failed to connect to MQTT server: {ex.Message}");
            return;
        }

        // Set up SQL dependency with retry mechanism
        await StartSqlDependencyWithRetry();

        Console.WriteLine("Listening for database changes...");
        await Task.Delay(Timeout.Infinite); //wait infinitive, but handle all events

        // Clean up
        SqlDependency.Stop(connectionString);
        await mqttClient.DisconnectAsync();
    }

    private static SqlDependency? dependency;

    private static async Task StartSqlDependencyWithRetry()
    {
        int retryCount = 0;
        const int maxRetries = 5;
        const int delayBetweenRetries = 5000; // 5 seconds

        while (retryCount < maxRetries)
        {
            try
            {
                SqlDependency.Start(connectionString);
                await RegisterSqlDependency();
                break; // Exit the loop if successful
            }
            catch (Exception ex)
            {
                retryCount++;
                Console.WriteLine($"Failed to start SQL dependency: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Console.WriteLine("Max retry attempts reached. Exiting...");
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
                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        dependency = new SqlDependency(command);
                        dependency.OnChange += new OnChangeEventHandler(OnDatabaseChange);

                        await connection.OpenAsync();
                        await command.ExecuteReaderAsync();
                        if (retryCount > 0) { Console.WriteLine("Connection to SQL server re-established"); }
                    }
                }
                break; // Exit the loop if successful
            }
            catch (SqlException ex)
            {
                retryCount++;
                Console.WriteLine($"SQL connection error: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Console.WriteLine("Max retry attempts reached. Exiting...");
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
                    Console.WriteLine("Database change detected!");

                    // Fetch new data
                    string? newData = await FetchNewData();

                    // Transmit new data via MQTT
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic("MSQL/Data")
                        .WithPayload(newData)
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                        .WithRetainFlag()
                        .Build();

                    if (mqttClient?.IsConnected == true)
                    {
                        try
                        {
                            await mqttClient.PublishAsync(message, CancellationToken.None);
                            Console.WriteLine("Data transmitted via MQTT.");
                        }
                        catch (MqttCommunicationException ex)
                        {
                            Console.WriteLine($"Failed to send MQTT message: {ex.Message}");
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
                            Console.WriteLine("Write Change to Cache");
                            messageCache.Enqueue(newData);
                        }
                    }

                    // Re-register dependency with retry mechanism
                    await RetryRegisterSqlDependency();
                }
                else
                {
                    Console.WriteLine("SQL server shutdown detected. Attempting to re-register dependency...");
                    // Re-register dependency with retry mechanism
                    await RetryRegisterSqlDependency();
                }
                break;

            case SqlNotificationType.Subscribe:
                if (e.Info == SqlNotificationInfo.Error)
                {
                    Console.WriteLine("SQL server shutdown detected. Attempting to re-register dependency...");
                    await RetryRegisterSqlDependency();
                }
                break;

            case SqlNotificationType.Unknown:
                Console.WriteLine("Unknown SQL notification type received.");
                break;

            default:
                Console.WriteLine($"Unhandled SQL notification type: {e.Type}, Info: {e.Info}, Source: {e.Source}");
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
                Console.WriteLine($"SQL connection error during re-registration: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Console.WriteLine("Max retry attempts reached. Exiting...");
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
                    using (SqlCommand command = new SqlCommand(query, connection))
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
                Console.WriteLine($"SQL query error: {ex.Message}. Retrying {retryCount}/{maxRetries}...");
                if (retryCount >= maxRetries)
                {
                    Console.WriteLine("Max retry attempts reached. Returning null...");
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
                .WithTopic("MSQL/Data")
                .WithPayload(cachedMessage)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                .WithRetainFlag()
                .Build();

            try
            {
                await mqttClient.PublishAsync(message, CancellationToken.None);
                Console.WriteLine("Cached data transmitted via MQTT.");
            }
            catch (MqttCommunicationException ex)
            {
                Console.WriteLine($"Failed to send cached MQTT message: {ex.Message}");
                messageCache.Enqueue(cachedMessage);
                break;
            }
        }
    }
}

