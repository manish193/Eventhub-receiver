using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace eventhub_receiver_app
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;
        private readonly string _eventHubNamespace = "vanquish-ns-applayer-ppe.servicebus.windows.net"; // Replace with your Event Hub namespace
        private readonly string _eventHubName = "appinsights"; // Replace with your Event Hub name
        private readonly string _tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47"; // Replace with the tenant ID of Event Hub
        private readonly string _appClientId = "b619c750-1fd6-460d-9aaa-b9d71d28719f"; // Replace with the client ID of the MSI

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function("Function1")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("Azure Function HTTP Triggered - Starting Event Hub Consumer");
            StringBuilder receivedMessages = new StringBuilder();

            try
            {
                // Authenticate using Managed Identity for cross-tenant access
                var credential = new ClientAssertionCredential(
                    _tenantId,
                    _appClientId,
                    async (token) =>
                    {
                        var tokenRequestContext = new Azure.Core.TokenRequestContext(new[] { "https://eventhubs.azure.net/.default" });
                        var accessToken = await new ManagedIdentityCredential().GetTokenAsync(tokenRequestContext);
                        return accessToken.Token;
                    });

                // Create Event Hub Consumer Client using MSI Authentication
                await using var consumer = new EventHubConsumerClient(
                    EventHubConsumerClient.DefaultConsumerGroupName,
                    _eventHubNamespace,
                    _eventHubName,
                    credential);

                CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)); // Run for 10 seconds
                int counter = 0;

                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(cts.Token))
                {
                    if (partitionEvent.Data != null)
                    {
                        string eventData = Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray());
                        receivedMessages.AppendLine(eventData);
                        _logger.LogInformation($"Received Event: {eventData}");

                        counter++;
                        if (counter >= 5) break; // Stop after 5 events max

                        await Task.Delay(2000); // Wait 2 seconds before reading the next event
                    }
                }

                return new OkObjectResult($"Received events:\n{receivedMessages}");
            }
            catch (TaskCanceledException)
            {
                _logger.LogInformation("Event consumption stopped after 10 seconds.");
                return new OkObjectResult($"Event consumption stopped after 10 seconds: {receivedMessages.ToString().Length}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error receiving events: {ex.Message}");
                return new StatusCodeResult(500);
            }
        }
    }
}

// ================== COMMENTED CONNECTION STRING VERSION ==================
// using System;
// using System.Text;
// using System.Threading;
// using System.Threading.Tasks;
// using Azure.Messaging.EventHubs;
// using Azure.Messaging.EventHubs.Consumer;
// using Microsoft.AspNetCore.Http;
// using Microsoft.AspNetCore.Mvc;
// using Microsoft.Azure.Functions.Worker;
// using Microsoft.Extensions.Logging;

// namespace eventhub_receiver_app
// {
//     public class Function1
//     {
//         private readonly ILogger<Function1> _logger;
//         private readonly string _eventHubName = "appinsights"; // Replace with your Event Hub name

//         public Function1(ILogger<Function1> logger)
//         {
//             _logger = logger;
//         }

//         [Function("Function1")]
//         public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
//         {
//             _logger.LogInformation("Azure Function HTTP Triggered - Starting Event Hub Consumer");
//             StringBuilder receivedMessages = new StringBuilder();

//             try
//             {
//                 await using var consumer = new EventHubConsumerClient(
//                     EventHubConsumerClient.DefaultConsumerGroupName,
//                     _eventHubConnectionString,
//                     _eventHubName);

//                 CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)); // Run for 10 seconds
//                 int counter = 0;

//                 await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(cts.Token))
//                 {
//                     if (partitionEvent.Data != null)
//                     {
//                         string eventData = Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray());
//                         receivedMessages.AppendLine(eventData);
//                         _logger.LogInformation($"Received Event: {eventData}");

//                         counter++;
//                         if (counter >= 5) break; // Stop after 5 events max

//                         await Task.Delay(2000); // Wait 2 seconds before reading the next event
//                     }
//                 }

//                 return new OkObjectResult($"Received events:\n{receivedMessages}");
//             }
//             catch (TaskCanceledException)
//             {
//                 _logger.LogInformation("Event consumption stopped after 10 seconds.");
//                 return new OkObjectResult($"Event consumption stopped after 10 seconds: {receivedMessages.ToString().Length}");
//             }
//             catch (Exception ex)
//             {
//                 _logger.LogError($"Error receiving events: {ex.Message}");
//                 return new StatusCodeResult(500);
//             }
//         }
//     }
// }
