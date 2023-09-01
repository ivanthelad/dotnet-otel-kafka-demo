using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Context.Propagation;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Collections.Generic;

using OpenTelemetry;
using Microsoft.Extensions.Logging.Console;
using Microsoft.AspNetCore.Builder;

class Producer
{
    private static readonly ActivitySource ActivitySource = new(nameof(Producer));
    private static readonly KafkaContextPropagator Propagator = new KafkaContextPropagator();

    private static readonly ILogger<Producer> _logger;

    class SendKafkaMessage
    {
        private ProducerConfig producerConfig;
        private readonly ILogger<SendKafkaMessage> _logger;
        IConfiguration configuration;
        public string Key { get; set; }
        public string Value { get; set; }


        public SendKafkaMessage()

        {
            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddFilter("LoggingConsoleApp.Program", LogLevel.Debug)
                    .AddConsole();
            });
            _logger = loggerFactory.CreateLogger<SendKafkaMessage>();

            configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();


            var kafkaProducerConfig = new KafkaProducerConfig();
            configuration.GetSection("KafkaProducerConfig").Bind(kafkaProducerConfig);
            producerConfig = new ProducerConfig
            {
                BootstrapServers = kafkaProducerConfig.BootstrapServers,
                ClientId = kafkaProducerConfig.ClientId,
                Acks = (Acks)Enum.Parse(typeof(Acks), kafkaProducerConfig.Acks, ignoreCase: true),
                // Set other properties as needed
            };
        }




        private void InjectHeader(Headers headers, string key, string value)
        {
            _logger.LogInformation("value and key = "+ value+ ", "+key);
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
                this._logger.LogError(ex, "Failed to inject trace context.");
            }
        }

        public void sendOtelTracedMessage(String topic, String message)
        {

            var activityName = "send";
            _logger.LogInformation("Sending a Otel traced message ");

            // builder.AddOtlpExporter(options => options.Endpoint = new Uri("http://localhost:4317"));
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            _logger.LogInformation("Starting the Kafka producer");
     //       OpenTelemetry.Sdk.SetDefaultTextMapPropagator(new CompositeTextMapPropagator(new TextMapPropagator[]
     //    {
      //             new BaggagePropagator(),
      //          new TraceContextPropagator(),
      //   }));

            var resourceBuilder =
    ResourceBuilder
        .CreateDefault()
        .AddService(serviceName: "kafka-producer-service", serviceVersion: "1.1")
        .AddAttributes(new Dictionary<string, object>
        {
            ["environment.name"] = "production",
            ["team.name"] = "backend"
        });

            using var tracerProvider = Sdk.CreateTracerProviderBuilder()
                   .SetResourceBuilder(resourceBuilder)
                   .AddSource(nameof(Producer))
                   .AddAspNetCoreInstrumentation()// Add the namespace used for logging in your Kafka producer code
                     .AddConsoleExporter().AddOtlpExporter().Build();
            _logger.LogInformation("Starting the Kafka producer");
            using
             (var activity = ActivitySource.StartActivity(activityName, ActivityKind.Producer))
            {

                ActivityContext contextToInject = default;
                if (activity != null)
                {
                    contextToInject = activity.Context;
                }
                else if (Activity.Current != null)
                {
                    contextToInject = Activity.Current.Context;
                }

                activity?.SetTag("greeting", message);
                using (var producer = new ProducerBuilder<string, string>(
                      producerConfig).Build())
                {
                    var numProduced = 0;
                    Random rnd = new Random();
                    string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
                    string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };
                    var user = users[rnd.Next(users.Length)];
                    var item = items[rnd.Next(items.Length)];
                    _logger.LogInformation($"Sending message to topic {topic}: key = {user,-10} value = {item}");

                    var header = new Headers();
                    header.Add("A.Entry", Encoding.UTF8.GetBytes("a simpple entry "));
                     Propagator.Inject(new PropagationContext(contextToInject, Baggage.Current), header, this.InjectHeader);
                     
                    // Inject the context into the Kafka message headers
                    //   header.Add("Activity.TraceId", Encoding.UTF8.GetBytes(contextToInject.TraceId.ToString()));
                    //   header.Add("Activity.SpanId", Encoding.UTF8.GetBytes(contextToInject.SpanId.ToString()));
                    _logger.LogInformation($"Injecting trace context into Kafka message headers: {header.ToString()}");
                    _logger.LogInformation(contextToInject.TraceId.ToString());
                    _logger.LogInformation(contextToInject.SpanId.ToString());
                    activity?.AddEvent(new ActivityEvent(user));
                    activity?.AddEvent(new ActivityEvent(item));
                    activity?.AddTag("messaging.system", "kafka");
                    activity?.AddTag("messaging.destination", topic);


                    Message<string, string> msg = new Message<string, string> { Key = user, Value = item, Headers = header };
                                       int messagePayloadBytes = Encoding.UTF8.GetByteCount(msg.Value.ToString());
                    activity?.AddTag("messaging.message_payload_size_bytes", messagePayloadBytes.ToString());
                    producer.Produce(topic, msg,
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                _logger.LogInformation($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                _logger.LogInformation($"Produced event to topic {topic}: key = {user,-10} value = {item}",msg.Headers);
                                numProduced += 1;
                            }
                        });


                    producer.Flush(TimeSpan.FromSeconds(10));
                    _logger.LogInformation($"{numProduced} messages were produced to topic {topic}");
                }
            }
        }
    }
    private class MyTraceContextPropagator : TextMapPropagator
    {
        public override ISet<string> Fields => new HashSet<string>();

        public override PropagationContext Extract<T>(PropagationContext context, T carrier, Func<T, string, IEnumerable<string>> getter)
        {
            var tid = ActivityTraceId.CreateFromBytes(Guid.Empty.ToByteArray());
            var ctx = new ActivityContext(tid, ActivitySpanId.CreateRandom(), context.ActivityContext.TraceFlags, context.ActivityContext.TraceState, isRemote: true);
            return new PropagationContext(ctx, context.Baggage);
        }

        public override void Inject<T>(PropagationContext context, T carrier, Action<T, string, string> setter)
        {
            return;
        }
    }
    static void Main(string[] args)
    {
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole(i => i.ColorBehavior = LoggerColorBehavior.Disabled);
        });

        var logger = loggerFactory.CreateLogger<Producer>();
    

        WebApplication.CreateBuilder(args);
        SendKafkaMessage sendKafkaMessage = new SendKafkaMessage();



        sendKafkaMessage.sendOtelTracedMessage("purchases", args[0]);

    }

}