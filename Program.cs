using Microsoft.Azure.ServiceBus;
using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBus
{
    class Program
    {
        const string ServiceBusConnectionString = "";
        const string QueueName = "myqueue";
        static IQueueClient queueClient;
        static Stopwatch watch = new Stopwatch();

        static void  Main(string[] args)
        {
            int loopCounterForReceive = 0;

            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            for (int i = 0; i < 5; i++)
            {

                Console.WriteLine("\n 1. Send message to queue \n 2. Read message from queue \n (once you choose this option messages will be received as and when they are avilable in queue");

                string input = Console.ReadLine();

                if (Convert.ToInt32(input) == 1)
                {
                    Console.WriteLine("Please enter the message to send");
                    string messge = Console.ReadLine();
                    watch.Start();
                    // Send message to queue.
                    SendMessageAsync(messge).GetAwaiter().GetResult();
                    watch.Stop();
                    Console.WriteLine("total time taken to send all messages to service bus is " + watch.Elapsed.Seconds + " Seconds");
                   
                }
                else if (Convert.ToInt32(input) == 2)
                {
                    if (loopCounterForReceive == 0)
                    {
                        Console.WriteLine("Consumer will start receiving messges from queue now");

                        RegisterOnMessageHandlerAndReceiveMessages();
                    }
                    loopCounterForReceive++;
                }

                Console.ReadLine();

            }


            queueClient.CloseAsync().GetAwaiter().GetResult();


        }

        static async Task SendMessageAsync(string messageContent)
        {
            try
            {
                for (int i = 0; i < 1000; i++)
                {
                    // Create a new message to send to the queue.
                    string messageBody = $"Message {messageContent + i.ToString()}";
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    // Write the body of the message to the console.
                    Console.WriteLine($"Sending message: {messageBody}");

                    // Send the message to the queue.
                    //await queueClient.SendAsync(message);

                    queueClient.SendAsync(message);
                }
                   
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 50,

                // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                AutoComplete = false
            };

            // Register the function that processes messages.
            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message.
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");

            // Complete the message so that it is not received again.
            // This can be done only if the queue Client is created in ReceiveMode.PeekLock mode (which is the default).
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been closed, you can choose to not call CompleteAsync() or AbandonAsync() etc.
            // to avoid unnecessary exceptions.
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}
