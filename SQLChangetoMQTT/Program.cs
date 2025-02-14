using System;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using MQTTnet;

class Program
{
    private static string connectionString = "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;";
    private static string query = "YourComplexSQLQueryHere";
    private static IMqttClient? mqttClient;

    static async Task Main(string[] args)
    {
        // Initialize MQTT client
        var factory = new MqttClientFactory();
        mqttClient = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithClientId("ClientID")
            .WithTcpServer("broker.hivemq.com", 1883)
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

    private static async Task RegisterSqlDependency()
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                SqlDependency dependency = new SqlDependency(command);
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
                .WithTopic("your/topic")
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
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                await connection.OpenAsync();
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    // Assuming the query returns a single row and single column
                    if (await reader.ReadAsync())
                    {
                        return reader[0]?.ToString();
                    }
                }
            }
        }
        return null;
    }
}
