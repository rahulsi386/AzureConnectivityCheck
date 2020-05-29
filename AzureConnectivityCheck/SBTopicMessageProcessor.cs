using System;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;


namespace AzureConnectivityCheck
{
    class SBTopicMessageProcessor
    {
        private static ISubscriptionClient subscriptionClient;
        private static SqlConnection sqlCon;
        public static async Task TestSbTopicToSqlAsync()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Beginning to read messages from ServiceBus Topic...");           
            try
            {
                subscriptionClient = new SubscriptionClient(AzureCheckSequence._ServiceBusConString, AzureCheckSequence._TopicName, AzureCheckSequence._TopicSubscriptionName);
                var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
                {
                    MaxConcurrentCalls = 1,
                    AutoComplete = false

                };

                sqlCon = new SqlConnection(AzureCheckSequence.SqlConString());
                sqlCon.Open();
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"\tWriting messages from ServiceBus Topic: '{subscriptionClient.TopicPath}' to Sql Database: '{sqlCon.Database}'");
                subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
                //Console.ReadKey();
                Thread.Sleep(10000);
                await subscriptionClient.CloseAsync();
                sqlCon.Close();
                sqlCon.Dispose();
                Console.WriteLine("\tTest Succeeded!");
                Console.ResetColor();
            }
            catch(Exception ex)
            {
                AzureCheckSequence.ErrorGrabber(ex.Message);
            }
        }
        public static async Task ProcessMessagesAsync(Message message, CancellationToken cancellationToken)
        {
            Console.WriteLine($"\tAdding Message: '{Encoding.UTF8.GetString(message.Body)}'");
            try
            {
                AzureCheckSequence.ExecuteTSqlNonQuery(sqlCon, AzureCheckSequence.InsertIntoSQLTable(), "@empName", Encoding.UTF8.GetString(message.Body));
                await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
            }
            catch(Exception ex)
            {
                AzureCheckSequence.ErrorGrabber(ex.Message);
            }           
        }

        public static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Topic message handler encountered an exception: {exceptionReceivedEventArgs.Exception}");
            return Task.CompletedTask;
        }
    }
}
