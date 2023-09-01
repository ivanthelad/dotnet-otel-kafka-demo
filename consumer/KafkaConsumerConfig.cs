using System;

namespace consumer;

public class KafkaConsumerConfig
{
    public string BootstrapServers { get; set; }
    public string ClientId { get; set; }
    public string Acks { get; set; }
    public string GroupId { get; set; }
    public string AutoOffsetReset { get; set; }

    public Confluent.Kafka.AutoOffsetReset getAutoOffsetReset()
    {
        if (Enum.TryParse(AutoOffsetReset, ignoreCase: true, out Confluent.Kafka.AutoOffsetReset result)) 
            return result;
        else
            return Confluent.Kafka.AutoOffsetReset.Earliest;
    }
}