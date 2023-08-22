# dotnet-otel-kafka-demo
Simple app to demonstrate how to perform otel tracing in .net across Kafka topics. The example simply sets the otel headers in the the kafka event. The consumer extracts these and continues the trace 




## Prep Demo
The following create the demo setup 
 
### Start  kafka Broker 
```
cd deploy/kafka 
docker compose up -d 
```
### Create topic 
```
 docker compose exec broker \
  kafka-topics --create \
    --topic purchases \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 
```

### Deploy otel 
taken from here. Commenting out the client and server aspects 
https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/examples/demo 
```
cd deploy/otel 
docker compose up -d 
```


### build and run producer
This creates a single message on kafka 
```
cd producer 
dotnet build
dotnet run $(pwd)/../getting-started.properties
```

### Build and run consumer
 ```
cd consumer 
dotnet build
dotnet run $(pwd)/../getting-started.properties
```


### View Results in Jaeger 
The results can be see in the jaeger UI which was started in the OTEL docker compose up. Go to http://127.0.0.1:16686/
* 


## Notes 
* https://learn.microsoft.com/en-us/dotnet/core/diagnostics/distributed-tracing-instrumentation-walkthroughs
* Would be ideal to user "Propagator.Inject(new PropagationContext(contextToInject, Baggage.Current), header, this.InjectHeader);" but for now i could not get the approaches i tried to work