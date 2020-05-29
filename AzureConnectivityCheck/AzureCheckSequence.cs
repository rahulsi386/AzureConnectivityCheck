using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Azure.ServiceBus;
using System.Data.SqlClient;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.IO;
using System.Threading;
using Azure.Security.KeyVault.Secrets;
using Azure.Identity;
using Azure.Storage.Blobs;
using System.Collections.Immutable;
using System.Linq;

namespace AzureConnectivityCheck
{
    public class AzureCheckSequence
    {
        public static EventHubClient _eventHubClient;
        public static string _EventHubConString;
        public static string _EventHubName;

        public static ITopicClient _topicClient;
        public static string _ServiceBusConString;
        public static string _TopicName;
        public static string _TopicSubscriptionName;
        

        public static string _SqlServerName;
        public static string _SqlUserId;
        public static string _SqlPassword;
        public static string _SqlDbName;

        public static string _StorageContainerName;
        public static string _StorageConnectionString;

        private static string _KeyVaultClientId;
        private static string _KeyVaultClientSecret;
        private static string _KeyVaultDNSName;
        private static string _KeyVaultSecretName;
        private static SecretClient _secretClient;
  
        private static string _TenantId;

        //private static string _StorageAccountClientId;
        //private static string _StorageAccountClientSecret;

        private static string _JsonString = string.Empty;
        

        static AzureCheckSequence()
        {
            try
            {

                //Specify below path for Debug version
                _JsonString = File.ReadAllText("..\\..\\..\\AzureConfig.json");

                //Specify below path for Release version
                //_JsonString = File.ReadAllText(".\\AzureConfig.json");

                AzureConStringJsonType jsonConfigObject = JsonConvert.DeserializeObject<AzureConStringJsonType>(_JsonString);

                _EventHubConString = jsonConfigObject.EventHubConString;
                _EventHubName = jsonConfigObject.EventHubName;
                _ServiceBusConString = jsonConfigObject.ServiceBusConString;
                _TopicName = jsonConfigObject.TopicName;
                _TopicSubscriptionName = jsonConfigObject.TopicSubscriptionName;
                _SqlServerName = jsonConfigObject.SqlServerName;
                _SqlUserId = jsonConfigObject.SqlUserId;
                _SqlPassword = jsonConfigObject.SqlPassword;
                _SqlDbName = jsonConfigObject.SqlDbName;
                _StorageContainerName = jsonConfigObject.StorageContainerName;
                _StorageConnectionString = jsonConfigObject.StorageConnectionString;
                _KeyVaultClientId = jsonConfigObject.KeyVaultClientId;
                _KeyVaultClientSecret = jsonConfigObject.KeyVaultClientSecret;
                _KeyVaultDNSName = jsonConfigObject.KeyVaultDNSName;
                _KeyVaultSecretName = jsonConfigObject.KeyVaultSecretName;
                _TenantId = jsonConfigObject.TenantId;
                
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }
        }

        public static async Task Main(string[] args)
        {            
            Console.Title = "Just for Testing - Azure Connectivity | Not Optimized -RAHULSI";
            string SelectedChoice;
            string ConfirmChoice;
            Console.ForegroundColor = ConsoleColor.Cyan;            
            Console.WriteLine("1. Check connectivity to KeyVault");
            Console.WriteLine("2. Check connectivity to Azure Storage");
            Console.WriteLine("3. Check connectivity from Azure SQL Server");
            Console.WriteLine("4. Check Connectivity to EventHub Namespace");
            Console.WriteLine("5. Check Connectivity to EventHub to SQL DB");
            Console.WriteLine("6. Check connectivity to ServiceBus Namespace");
            Console.WriteLine("7. Check connectivity from ServiceBus Topic to SQL DB");            
            Console.WriteLine("8. RUN ALL CHECK SEQUENCE");                                    
            Console.WriteLine("**********************************************************");
            Console.ResetColor();
            
            do
            {
                Console.Write("\nEnter operation number to perform check: ");
                SelectedChoice = Console.ReadLine();
                Console.Write("Do you want to continue (Y/N):");
                ConfirmChoice = Console.ReadLine();                
                switch (SelectedChoice)
                {
                    case "1":
                        TestKeyVault();                        
                        break;
                    case "2":
                        TestAzureStorage();                        
                        break;
                    case "3":
                        TestAzureSql();                        
                        break;
                    case "4":
                        TestEventHubAsync().GetAwaiter().GetResult();                        
                        break;
                    case "5":
                        TestEventHubToSqlAsync().GetAwaiter().GetResult();                        
                        break;
                    case "6":
                        TestServiceBusTopicAsync().GetAwaiter().GetResult();                        
                        break;
                    case "7":
                        SBTopicMessageProcessor.TestSbTopicToSqlAsync().GetAwaiter().GetResult();                        
                        break;
                    case "8":
                        Console.ForegroundColor = ConsoleColor.DarkGreen;
                        Console.WriteLine("Initiating Azure connectivity check sequence...");
                        Console.ResetColor();
                        TestKeyVault();
                        TestAzureStorage();
                        TestAzureSql();
                        TestEventHubAsync().GetAwaiter().GetResult();
                        TestEventHubToSqlAsync().GetAwaiter().GetResult();
                        TestServiceBusTopicAsync().GetAwaiter().GetResult();
                        SBTopicMessageProcessor.TestSbTopicToSqlAsync().GetAwaiter().GetResult();
                        CleanUpTestResources();                        
                        break;
                }
            } while (ConfirmChoice == "Y" || ConfirmChoice == "y" || ConfirmChoice == "Yes" || ConfirmChoice == "yes");
        }

        public static void CleanUpTestResources()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Initiating test resource cleanup sequence...");
            Console.ForegroundColor = ConsoleColor.Yellow;
            try
            {
                Console.WriteLine("Deleting test Secret key from Key vault");

                Console.WriteLine("\tCleaning up SQL Db");
                using (SqlConnection sqlCon = new SqlConnection(SqlConString()))
                {
                    sqlCon.Open();                    
                    ExecuteTSqlNonQuery(sqlCon, DropSQLTable());
                    Console.WriteLine("\tDeleted table dbo.employee table from database.");
                    sqlCon.Close();                    
                }
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }
        }

        public static void ErrorGrabber(string errorMessage)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"An error occurred: {errorMessage}");
            Console.ResetColor();
        }

        private static void TestKeyVault()
        {
            try
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Connecting to Key Vault");
                Console.ForegroundColor = ConsoleColor.Yellow;
                _secretClient = new SecretClient(vaultUri: new Uri(_KeyVaultDNSName), credential: new ClientSecretCredential(_TenantId, _KeyVaultClientId, _KeyVaultClientSecret));
                Console.WriteLine($"\tSuccessfully connected to Key Vault: {_secretClient.VaultUri}");
                Console.WriteLine("\tAdding a test secret key-value pair in the vault");
                KeyVaultSecret secret = new KeyVaultSecret(_KeyVaultSecretName, Guid.NewGuid().ToString());
                //KeyVaultSecret secret = new KeyVaultSecret("LearningRGAppSecret", "_23Eqlb6_006~p10iEOtL..30~yS8sksz4");
                secret.Properties.Enabled = true;
                _secretClient.SetSecret(secret);
                Console.WriteLine("\tAdded a test secret to the Key Vault");
                Console.WriteLine("\tFetching the test secret from the Key Vault");
                var secretVal=_secretClient.GetSecret("newsecret");
                Console.WriteLine($"\tSecretKey: {secretVal.Value.Name};\tSecretValue: {secretVal.Value.Value}");
                Console.ResetColor();
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }
        }

        private static void TestAzureStorage()
        {
            try
            {
                //var StorageAccountUri = @"https://funcappstoragerps.blob.core.windows.net";
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Connecting to Azure Storage Account...");
                BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString: _StorageConnectionString);
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"\tSuccessfully connected to Azure Storage Account: {blobServiceClient.AccountName}");
                //var blobProp = blobServiceClient.GetProperties();
                //Console.WriteLine($"\tGetting Blob Properties:\n\tCors Count: {blobProp.Value.Cors.Count}\n\tLogging Version: {blobProp.Value.Logging.Version}");
                var blobContainer = blobServiceClient.GetBlobContainers();
                Console.WriteLine($"\tInitial total Blob Containers count: {blobContainer.Count()}");
                Console.WriteLine("\tCreating a new Test Blob Container");
                blobServiceClient.CreateBlobContainer("visualstudio-container");
                Console.WriteLine($"\tUpdated total Blob containers count: {blobServiceClient.GetBlobContainers().Count()}");
                Console.WriteLine("\tSuccessfully created a new container");
                Console.WriteLine("\tDeleting newly created Blob Container");
                Thread.Sleep(10000);
                blobServiceClient.DeleteBlobContainer("visualstudio-container");
                Console.WriteLine($"\tTotal container count after deletion: {blobServiceClient.GetBlobContainers().Count()}");
                Console.WriteLine("\tSuccessfully deleted test blob container");
                Console.ResetColor();
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }
        }
        private static async Task TestEventHubAsync()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Connecting to EventHub");
            Console.ResetColor();
            try
            {               
                var connectionStringBuilder = new EventHubsConnectionStringBuilder(_EventHubConString)
                {
                    EntityPath = _EventHubName
                };
                _eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                Console.WriteLine($"\tSuccessfully Connected to {_eventHubClient.EventHubName}");
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"\tAdding messages to EventHub: {_eventHubClient.EventHubName}");
                for (int i = 0; i < 10; i++)
                {
                    var message = $"MSFT EventHub Test- {i}";
                    await _eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                    Console.WriteLine($"\tAdded message '{message}'");
                }
                Console.ResetColor();
                await _eventHubClient.CloseAsync();
                Console.WriteLine($"\tClosed connection to {_eventHubClient.EventHubName}");
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }            
        }

        private static async Task TestServiceBusTopicAsync()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Connecting to ServiceBus Namespace");
            Console.ResetColor();
            try
            {
                
                _topicClient = new TopicClient(_ServiceBusConString, _TopicName);
                Console.WriteLine($"\tSuccessfully Connected to {_topicClient.TopicName}");
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"\tAdding messages to ServiceBus Topic: {_topicClient.TopicName}");
                for (int i = 0; i < 10; i++)
                {
                    var message = $"MSFT SB Topic Test -{i}";
                    await _topicClient.SendAsync(new Message(Encoding.UTF8.GetBytes(message)));
                    Console.WriteLine($"\tAdded message '{message}'");
                }
                Console.ResetColor();
                await _topicClient.CloseAsync();
                Console.WriteLine($"\tClosed connection to {_topicClient.TopicName}");
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }
        }

        private static string CreateSQLTable()
        {
            return @"
                DROP TABLE IF EXISTS dbo.employee;
                CREATE TABLE dbo.employee
                (
                    EmpId uniqueIdentifier not null default NewId() PRIMARY KEY,
                    EmpName nvarchar(25) not null
                );";
        }

        private static string DropSQLTable()
        {
            return @"
                DROP TABLE IF EXISTS dbo.employee;";
        }
        public static string InsertIntoSQLTable()
        {
            return @"
               INSERT INTO dbo.employee (EmpName) 
               VALUES
                (@empName);";
        }

        private static string SelectSqlQuery()
        {
            return @"
               SELECT * FROM dbo.employee;";
        }

        private static string ListExistingTables()
        {
            return @"
              select table_name from information_schema.tables;";
        }

        public static void ExecuteTSqlNonQuery(SqlConnection con, string SqlOperation, string parameterName=null, string parameterValue=null)
        {
            try
            {
                using (var command = new SqlCommand(SqlOperation, con))
                {
                    if (parameterName != null && parameterValue != null)
                    {
                        command.Parameters.AddWithValue(parameterName, parameterValue);
                    }
                    int rowsAffected = command.ExecuteNonQuery();
                    Console.WriteLine($"\t {rowsAffected} rows affected");
                }
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }
        }

        private static void ExecuteSelectSqlQuery(SqlConnection con, string selectStatement)
        {
            string tSql = selectStatement;
            try
            {
                using (var command = new SqlCommand(tSql, con))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        int totalColumn = reader.FieldCount;
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        while (reader.Read())
                        {
                            for (int i = 0; i < totalColumn; i++)
                            {
                                Console.Write($"\t{reader.GetValue(i)}\t");
                            }
                        }
                        Console.ResetColor();
                        Console.WriteLine();
                    }
                }
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
                
            }
        }

        public static string SqlConString()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = $"{_SqlServerName}.database.windows.net";
            builder.UserID = _SqlUserId;
            builder.Password = _SqlPassword;
            builder.InitialCatalog = _SqlDbName;

            return builder.ConnectionString;
        }
        private static void TestAzureSql()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Connecting to Azure Sql database");
            Console.ResetColor();
            try
            {
                using (SqlConnection sqlCon = new SqlConnection(SqlConString()))
                {
                    sqlCon.Open();
                    Console.WriteLine($"\tSuccessfully connected to {sqlCon.Database}");
                    Console.WriteLine($"\tGetting existing tables from database: {sqlCon.Database}");
                    ExecuteSelectSqlQuery(sqlCon, ListExistingTables());
                    Console.WriteLine($"\tCreating new table dbo.employee in {sqlCon.Database}");
                    ExecuteTSqlNonQuery(sqlCon, CreateSQLTable());
                    Console.WriteLine("\tSuccessfully created table");
                    Console.WriteLine("\tInserting records in the dbo.employee table");
                    ExecuteTSqlNonQuery(sqlCon, InsertIntoSQLTable(), "@empName", "RPS");
                    Console.WriteLine("\tSelecting records from dbo.employee table");
                    ExecuteSelectSqlQuery(sqlCon, SelectSqlQuery());
                    Console.WriteLine("\tCleaning up test resources...");
                    ExecuteTSqlNonQuery(sqlCon, DropSQLTable());
                    Console.WriteLine("\tDeleted table dbo.employee table from database.");
                    sqlCon.Close();
                    Console.WriteLine("\tConnection to database closed");
                }
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }            
        }

        private static async Task TestEventHubToSqlAsync()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Registering Event processor to read messages from EventHub...");
            Console.ResetColor();
            try
            {
                if (_EventHubName != null && _EventHubConString != null && _StorageConnectionString != null && _StorageContainerName != null)
                {
                    var eventProcessorHost = new EventProcessorHost(
                        _EventHubName,
                        PartitionReceiver.DefaultConsumerGroupName,
                        _EventHubConString,
                        _StorageConnectionString,
                        _StorageContainerName);

                    
                    await eventProcessorHost.RegisterEventProcessorAsync<QadEventProcessor>();
                    // Console.ReadLine();
                    Thread.Sleep(20000);
                    await eventProcessorHost.UnregisterEventProcessorAsync();
                }
                else
                {
                    if (string.IsNullOrEmpty(_EventHubName))
                        Console.WriteLine("EventHub Name cannot be empty");
                    else if (string.IsNullOrEmpty(_EventHubConString))
                        Console.WriteLine("EventHub connection string cannot be empty");
                    else if (string.IsNullOrEmpty(_StorageConnectionString))
                        Console.WriteLine("Storage account connection string cannot be empty");
                    else if (string.IsNullOrEmpty(_StorageContainerName))
                        Console.WriteLine("Blob container name cannot be empty");
                }
            }
            catch(Exception ex)
            {
                ErrorGrabber(ex.Message);
            }
        }        
    }

    public class QadEventProcessor : IEventProcessor
    {
        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine($"\tProcessor shutting down. Partition Id: {context.PartitionId}; Reason: {reason}");
            return Task.CompletedTask;
        }
        public Task OpenAsync(PartitionContext context)
        {
            Console.WriteLine($"\tEvent processor initialized...Partition Id: {context.PartitionId}");
            return Task.CompletedTask;
        }
        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            Console.WriteLine($"Error on partition. Partition Id: {context.PartitionId}; Error: {error.Message}");
            return Task.CompletedTask;
        }
        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"\tReading messages from Event hub...");
            Console.WriteLine("\tConnecting to Azure Sql database");
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = $"{AzureCheckSequence._SqlServerName}.database.windows.net";
            builder.UserID = AzureCheckSequence._SqlUserId;
            builder.Password = AzureCheckSequence._SqlPassword;
            builder.InitialCatalog = AzureCheckSequence._SqlDbName;
            try
            {
                using (SqlConnection sqlCon = new SqlConnection(builder.ConnectionString))
                {
                    sqlCon.Open();
                    Console.WriteLine($"\tSuccessfully connected to {sqlCon.Database}");
                    Console.WriteLine($"\tInserting messages to Sql server database table: {sqlCon.Database}\\dbo.employee");
                    foreach (var eventData in messages)
                    {
                        var data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                        Console.WriteLine($"\tAdding message: '{data}'");                                                                        
                        AzureCheckSequence.ExecuteTSqlNonQuery(sqlCon, AzureCheckSequence.InsertIntoSQLTable(), "@empName", data.ToString());                       
                    }
                }
                Console.ResetColor();
            }
            catch(Exception ex)
            {
                AzureCheckSequence.ErrorGrabber(ex.Message);
            }
            return context.CheckpointAsync();
        }
    }

    public class AzureConStringJsonType
    {
        public string EventHubConString { get; set; }
        public string EventHubName { get; set; }    
        public string ServiceBusConString { get; set; }
        public string TopicName { get; set; }
        public string TopicSubscriptionName { get; set; }       
        public string SqlServerName { get; set; }
        public string SqlDbName { get; set; }
        public string SqlUserId { get; set; }
        public string SqlPassword { get; set; }    
        public string StorageConnectionString { get; set; }
        public string StorageContainerName { get; set; }
        public string KeyVaultClientId { get; set; }
        public string KeyVaultClientSecret { get; set; }
        public string KeyVaultDNSName { get; set; }
        public string KeyVaultSecretName { get; set; }
        public string TenantId { get; set; }
    }
}
