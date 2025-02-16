using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using MQTTnet;

class Program
{
    private static string connectionString = "Server=localhost;Database=pubs;User Id=engineer;Password=engineer;TrustServerCertificate=True;";
    private static string query = "SELECT au_id, au_lname, au_fname FROM [dbo].[authors]";
    private static IMqttClient? mqttClient;

    static async Task Main(string[] args)
    {
        // Initialize MQTT client
        var factory = new MqttClientFactory();
        mqttClient = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithClientId("ClientID")
            .WithTcpServer("192.168.88.1", 1883)
            .Build();

        await mqttClient.ConnectAsync(options, CancellationToken.None);

        // Set up SQL dependency
        SqlDependency.Start(connectionString);
        await RegisterSqlDependency();

        Console.WriteLine("Listening for database changes...");
        Console.ReadLine();

        // Clean up
        SqlDependency.Stop(connectionString);
        await mqttClient.DisconnectAsync();
    }

    private static SqlDependency? dependency;

    private static async Task RegisterSqlDependency()
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                dependency = new SqlDependency(command);
                dependency.OnChange += new OnChangeEventHandler(OnDatabaseChange);

                await connection.OpenAsync();
                await command.ExecuteReaderAsync();
            }
        }
    }

    private static async void OnDatabaseChange(object sender, SqlNotificationEventArgs e)
    {
        if (e.Type == SqlNotificationType.Change)
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
                await mqttClient.PublishAsync(message, CancellationToken.None);
                Console.WriteLine("Data transmitted via MQTT.");
            }

            // Re-register dependency
            await RegisterSqlDependency();
        }
    }

    private static async Task<string?> FetchNewData()
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
}