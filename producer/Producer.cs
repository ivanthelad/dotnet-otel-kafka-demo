using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Collections.Generic;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using OpenTelemetry;
using OpenTelemetry.Logs;
class Producer
{
    private static readonly ActivitySource ActivitySource = new(nameof(Producer));
    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    private readonly ILogger<Producer> logger;
    class SendKafkaMessage
    {
        private readonly ILogger<SendKafkaMessage> logger;
        IConfiguration configuration;
        public string Key { get; set; }
        public string Value { get; set; }
        public SendKafkaMessage(string file)
        {
            configuration = new ConfigurationBuilder()
            .AddIniFile(file)
            .Build();
        }


        public void sendEvent(string topic)
        {
            using (var producer = new ProducerBuilder<string, string>(
                configuration.AsEnumerable()).Build())
            {
                var numProduced = 0;
                Random rnd = new Random();
                const int numMessages = 10;
                string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
                string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };
                for (int i = 0; i < numMessages; ++i)
                {
                    var user = users[rnd.Next(users.Length)];
                    var item = items[rnd.Next(items.Length)];

                    producer.Produce(topic, new Message<string, string> { Key = user, Value = item },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Console.WriteLine($"Produced event to topic {topic}: key = {user,-10} value = {item}");
                                numProduced += 1;
                            }
                        });
                }

                producer.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
            }
        }


        private void InjectHeader(Headers headers, string key, string value)
        {
            try
            {
                if (headers == null)
                {
                    headers = new Headers();
                }

                headers.Add(key, Encoding.UTF8.GetBytes(value));
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Failed to inject trace context.");
            }
        }

        public void sendOtelTracedMessage(String topic)
        {

            var activityName = "send";


            // builder.AddOtlpExporter(options => options.Endpoint = new Uri("http://localhost:4317"));
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            using var tracerProvider = Sdk.CreateTracerProviderBuilder()
                   .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("kafka-producer-service"))
                   .AddSource(nameof(Producer))
                   .AddAspNetCoreInstrumentation()// Add the namespace used for logging in your Kafka producer code
                     .AddConsoleExporter().AddOtlpExporter().Build();
            using
             (var activity = ActivitySource.StartActivity(activityName, ActivityKind.Producer))
            {
                // Depending on Sampling (and whether a listener is registered or not), the
                // activity above may not be created.
                // If it is created, then propagate its context.
                // If it is not created, the propagate the Current context,
                // if any.
                ActivityContext contextToInject = default;
                if (activity != null)
                {
                    contextToInject = activity.Context;
                }
                else if (Activity.Current != null)
                {
                    contextToInject = Activity.Current.Context;
                }

                activity?.SetTag("greeting", "Hello World! From Producer");

                //   AppContext1  -> kafka(Headers, ) -> AppContext2
                using (var producer = new ProducerBuilder<string, string>(
                      configuration.AsEnumerable()).Build())
                {
                    var numProduced = 0;
                    Random rnd = new Random();
                    const int numMessages = 1;
                    string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
                    string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };
                    for (int i = 0; i < numMessages; ++i)
                    {
                        var user = users[rnd.Next(users.Length)];
                        var item = items[rnd.Next(items.Length)];

                        var header = new Headers();
                        header.Add("A.Entry", Encoding.UTF8.GetBytes("a simpple entry "));

                       // Propagator.Inject(new PropagationContext(contextToInject, Baggage.Current), header, this.InjectHeader);
                        // Inject the context into the Kafka message headers
                        header.Add("Activity.TraceId", Encoding.UTF8.GetBytes(contextToInject.TraceId.ToString()));
                        header.Add("Activity.SpanId", Encoding.UTF8.GetBytes(contextToInject.SpanId.ToString()));
                        activity?.AddEvent(new ActivityEvent("Example Event added to trace"));
                        //   header.Add("Activity.ParentTraci", contextToInject.TraceId.ToHexString());
                        Console.WriteLine($"Injecting trace context into Kafka message headers: {header.ToString()}");
                        producer.Produce(topic, new Message<string, string> { Key = user, Value = item, Headers = header },
                            (deliveryReport) =>
                            {
                                if (deliveryReport.Error.Code != ErrorCode.NoError)
                                {
                                    Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                                }
                                else
                                {
                                    Console.WriteLine($"Produced event to topic {topic}: key = {user,-10} value = {item}");
                                    numProduced += 1;
                                }
                            });
                    }

                    producer.Flush(TimeSpan.FromSeconds(10));
                    Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
                }
            }
        }


    }

    static void Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }



        SendKafkaMessage sendKafkaMessage = new SendKafkaMessage(args[0]);


        sendKafkaMessage.sendOtelTracedMessage("purchases");

    }

}