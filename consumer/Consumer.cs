using Confluent.Kafka;
using System;

using System.Threading;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;


using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;

using OpenTelemetry.Resources;
using OpenTelemetry.Trace;



class Consumer
{

 //  private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    private static readonly KafkaContextPropagator Propagator = new KafkaContextPropagator();

    private static readonly ActivitySource ActivitySource = new(nameof(Consumer));
    private static  ILogger<Consumer> _logger;

    static void Main(string[] args)
    {
     

    
        IConfiguration  configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        var kafkaConsumerConfig = new KafkaConsumerConfig();
        configuration.GetSection("KafkaConsumerConfig").Bind(kafkaConsumerConfig);
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaConsumerConfig.BootstrapServers,
            ClientId = kafkaConsumerConfig.ClientId,
            Acks = (Acks)System.Enum.Parse(typeof(Acks), kafkaConsumerConfig.Acks, ignoreCase: true),
            // Set other properties as needed
            GroupId = kafkaConsumerConfig.GroupId,
            AutoOffsetReset  = kafkaConsumerConfig.getAutoOffsetReset()
       
        };
        // setting to this format so we can understand incoming  traceparetn headers 
        Activity.DefaultIdFormat = ActivityIdFormat.W3C;
        const string topic = "purchases";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };
        using var tracerProvider = Sdk.CreateTracerProviderBuilder()
               .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("kafka-consumer-service"))
               .AddSource(nameof(Consumer)) // Add the namespace used for logging in your Kafka producer code
               .AddConsoleExporter() // Optional: For exporting traces to the console (useful for testing)
                           .AddOtlpExporter(options => options.Endpoint = new Uri("http://localhost:4317")).Build();

        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddFilter("Microsoft", LogLevel.Warning)
                .AddFilter("System", LogLevel.Warning)
                .AddFilter("LoggingConsoleApp.Program", LogLevel.Debug)
                .AddConsole();
        });
        _logger = loggerFactory.CreateLogger<Consumer>();

        using (var consumer = new ConsumerBuilder<string, string>(
            consumerConfig).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cts.Token);
                    _logger.LogInformation($"Consumed event from topic {topic} with key {cr.Message.Key,-10} and value {cr.Message.Value} , and headers {cr.Message.ToString}  ");

                    // Print headers
                    var headers = cr.Message.Headers;
                    //var parentContext = Propagator.Extract(default, cr.Message.Headers);
                    //   var context = Propagator.Extract(PropagationContext.Current, headers, (headers, name) => headers.GetValues(name));


                    PropagationContext parentContext = Propagator.Extract(default, headers, GetHeaderValues);

                    //Baggage.Current = extractedContext.Baggage;
                    var activityName = $"receive";
                    //           using var activity = ActivitySource.StartActivity(activityName, ActivityKind.Consumer, extractedContext.ActivityContext);
                    //          activity?.SetTag("message", cr.Message.Value);
                    byte[] traceId = null;
                    byte[] spanId = null;
                    string traceFlagsString = "01"; // TraceFlags can be 0 or 1 (recorded or not)
        
                    foreach (var header in cr.Message.Headers)
                    {
                        _logger.LogInformation($"Header: {header.Key} = {Encoding.UTF8.GetString(header.GetValueBytes())}");
                        if (header.Key == "Activity.TraceId")
                        {
                            traceId = header.GetValueBytes();

                        }
                        if (header.Key == "Activity.SpanId")
                        {
                            spanId = header.GetValueBytes();
                        }
                    }

              
          
                     _logger.LogInformation($"traceId: {traceId}");
                    var startTimestamp = DateTime.UtcNow;
                    var endTimestamp = startTimestamp.AddSeconds(new Random().Next(1, 10));
                    /**
                     * The below code demostrates pulling the context manually without the custom Propagator
                     * 
                     * if (traceId != null)
                        parentContext = new ActivityContext(
                                                           ActivityTraceId.CreateFromString(Encoding.UTF8.GetString(traceId)),
                                                           ActivitySpanId.CreateRandom(),
                                                           ActivityTraceFlags.Recorded, isRemote: true);
                    else
                      // if not found in header, create a new one
                        parentContext = new ActivityContext(
                                 ActivityTraceId.CreateRandom(),
                                 ActivitySpanId.CreateRandom(),
                                 ActivityTraceFlags.Recorded, isRemote: true);
                    **/
                    _logger.LogInformation($"{parentContext.ActivityContext.TraceId} {parentContext.ActivityContext.SpanId} {parentContext.ActivityContext.IsRemote} {parentContext.ActivityContext.TraceFlags}");
                    //   var parentActivity = ActivitySource.StartActivity("Receive", ActivityKind.Consumer, parentContext);

                    using (var activity = ActivitySource.StartActivity("Receive", ActivityKind.Consumer, parentContext.ActivityContext))
                    {
                        _logger.LogInformation($"{activity?.Context.TraceId} {activity?.Context.SpanId} {activity?.Context.TraceFlags}");

                        activity?.SetTag("message", cr.Message.Value);
                        activity?.SetTag("message33", cr.Message.Value);

                        Thread.Sleep(new Random().Next(200, 1000));
                        // Your activity code here...


                        using (var secondactivty = ActivitySource.StartActivity("ReceiveOtherActivity", ActivityKind.Internal, parentContext.ActivityContext))
                        {
                            _logger.LogInformation($"{secondactivty?.Context.TraceId} {secondactivty?.Context.SpanId} {secondactivty?.Context.TraceFlags}");

                            secondactivty?.SetTag("other", cr.Message.Value);
                            secondactivty?.SetTag("other2", cr.Message.Value);
                            Thread.Sleep(new Random().Next(200, 3000));
                            // Your activity code here...
                        }
                        Thread.Sleep(new Random().Next(200, 1000));
                    }
                    //   using var parentActivity = ActivitySource.StartActivity(activityName,  ActivityKind.Consumer);

                    Thread.Sleep(new Random().Next(200, 3000));
                    // need to stop and commit activity  if not in using scope
                    //parentActivity?.Stop();
                    _logger.LogInformation($"tester");
                }
            }
            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed.
            }
            finally
            {
                consumer.Close();
            }
        }
    }
    private static IEnumerable<string> GetHeaderValues(Headers headers, string key)
    {
        // Implement the logic to extract values from Kafka headers based on the key.
        // For example, you can use LINQ or any other method to find the values associated with the key.
        var values = new List<string>();
        _logger.LogInformation("Pulling key  " + key + " ");
        foreach (var header in headers)
        {
            if (header.Key == key)
            {
                _logger.LogInformation(">>>> header.Key  found " + header.Key + "="+Encoding.UTF8.GetString(header.GetValueBytes()));
                values.Add(Encoding.UTF8.GetString(header.GetValueBytes()));
            }
        }

        return values;
    }

}

// https://github.com/open-telemetry/opentelemetry-dotnet/blob/dacc532d51ca0f3775160b84fa6d7d9403a8ccde/examples/MicroserviceExample/Utils/Messaging/MessageReceiver.cs#L56
//var parentContext = Propagator.Extract(default, ea.BasicProperties, this.ExtractTraceContextFromBasicProperties);
//           Baggage.Current = parentContext.Baggage;
//
// Start an activity with a name following the semantic convention of the OpenTelemetry messaging specification.
// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#span-name
//          var activityName = $"{ea.RoutingKey} receive";