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
using System.Collections;

class Consumer
{

    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;
    private static readonly ActivitySource ActivitySource = new(nameof(Consumer));

    static void Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(args[0])
            .Build();

        configuration["group.id"] = "kafka-dotnet-getting-started";
        configuration["auto.offset.reset"] = "earliest";

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

        using (var consumer = new ConsumerBuilder<string, string>(
            configuration.AsEnumerable()).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed event from topic {topic} with key {cr.Message.Key,-10} and value {cr.Message.Value} , and headers {cr.Message.ToString}  ");

                    // Print headers
                    var headers = cr.Message.Headers;
                    //var parentContext = Propagator.Extract(default, cr.Message.Headers);
                    //   var context = Propagator.Extract(PropagationContext.Current, headers, (headers, name) => headers.GetValues(name));


                    //var extractedContext = Propagator.Extract(default, headers, (h, key) => GetHeaderValues(h, key));

                    //Baggage.Current = extractedContext.Baggage;
                    var activityName = $"receive";
                    //           using var activity = ActivitySource.StartActivity(activityName, ActivityKind.Consumer, extractedContext.ActivityContext);
                    //          activity?.SetTag("message", cr.Message.Value);
                    byte[] traceId = null;
                    byte[] spanId = null;
                    string traceFlagsString = "01"; // TraceFlags can be 0 or 1 (recorded or not)

                    foreach (var header in cr.Message.Headers)
                    {
                        Console.WriteLine($"Header: {header.Key} = {Encoding.UTF8.GetString(header.GetValueBytes())}");
                        if (header.Key == "Activity.TraceId")
                        {
                            traceId = header.GetValueBytes();

                        }
                        if (header.Key == "Activity.SpanId")
                        {
                            spanId = header.GetValueBytes();

                        }
                    }

              
          
                       Console.WriteLine($"traceId: {traceId}");
                    var startTimestamp = DateTime.UtcNow;
                    var endTimestamp = startTimestamp.AddSeconds(new Random().Next(1, 10));
                    ActivityContext parentContext;
                    if (traceId != null)
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
                    Console.WriteLine($"{parentContext.TraceId} {parentContext.SpanId} {parentContext.IsRemote} {parentContext.TraceFlags}");
                    //   var parentActivity = ActivitySource.StartActivity("Receive", ActivityKind.Consumer, parentContext);

                    using (var activity = ActivitySource.StartActivity("Receive", ActivityKind.Consumer, parentContext))
                    {
                        Console.WriteLine($"{activity?.Context.TraceId} {activity?.Context.SpanId} {activity?.Context.TraceFlags}");

                        activity?.SetTag("message", cr.Message.Value);
                        activity?.SetTag("message33", cr.Message.Value);

                        Thread.Sleep(new Random().Next(200, 1000));
                        // Your activity code here...


                        using (var activity2 = ActivitySource.StartActivity("ReceiveOtherActivity", ActivityKind.Internal, parentContext))
                        {
                            Console.WriteLine($"{activity2?.Context.TraceId} {activity2?.Context.SpanId} {activity2?.Context.TraceFlags}");

                            activity2?.SetTag("other", cr.Message.Value);
                            activity2?.SetTag("other2", cr.Message.Value);
                            Thread.Sleep(new Random().Next(200, 3000));
                            // Your activity code here...
                        }
                        Thread.Sleep(new Random().Next(200, 1000));
                    }
                    //   using var parentActivity = ActivitySource.StartActivity(activityName,  ActivityKind.Consumer);

                    Thread.Sleep(new Random().Next(200, 3000));
                    //parentActivity?.Stop();
                    Console.WriteLine($"tester");
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

        foreach (var header in headers)
        {
            if (header.Key == key)
            {
                values.Add(header.GetValueBytes().ToString());
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