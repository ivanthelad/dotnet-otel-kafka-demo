using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace  producer;

/// <summary>
/// A text map propagator for W3C trace context. See https://w3c.github.io/trace-context/.
/// </summary>
public class KafkaContextPropagator : TextMapPropagator
{
    private const string TraceParent = "traceparent";

    private const string TraceState = "tracestate";

    private const string SpanIdKey = "Activity.SpanId";
    private const string TraceIdKey = "Activity.TraceId";


    private readonly ILogger<KafkaContextPropagator> _logger;

    public KafkaContextPropagator()
    {
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddFilter("Microsoft", LogLevel.Warning)
                .AddFilter("System", LogLevel.Warning)
                .AddFilter("LoggingConsoleApp.Program", LogLevel.Debug)
                .AddConsole();
        });
        _logger = loggerFactory.CreateLogger<KafkaContextPropagator>();
    }
    /// <inheritdoc />
    public override ISet<string> Fields => new HashSet<string> { TraceState, TraceParent, TraceIdKey, SpanIdKey };

    /// <inheritdoc />
    public override PropagationContext Extract<T>(PropagationContext context, T carrier, Func<T, string, IEnumerable<string>> getter)
    {
        if (context.ActivityContext.IsValid())
        {
            return context;
        }
        if (carrier == null)
        {
            _logger.LogError("Failed to extract context, carrier not defined ");
            return context;
        }
        if (getter == null)
        {
            _logger.LogError("Failed to extract context, getter not defined ");
            return context;
        }
        try
        {
            IEnumerable<string> traceIdEnumerable = getter(carrier, TraceIdKey);
            if (traceIdEnumerable == null || traceIdEnumerable.Count() != 1)
            {
                return context;
            }
            
            ActivityTraceId traceId = ActivityTraceId.CreateFromString(traceIdEnumerable.First());
           
        
            IEnumerable<string> spanIdEnumerable = getter(carrier, SpanIdKey);
            ActivitySpanId spanId;
            if (spanIdEnumerable != null && spanIdEnumerable.Any())
            {
                spanId = ActivitySpanId.CreateFromString(spanIdEnumerable.First());
            }
            else
                return context;
            //TODO look into propagating the tracestate and trace flages
            return new PropagationContext(new ActivityContext(traceId, spanId,ActivityTraceFlags.Recorded, null, isRemote: true), context.Baggage);
        }
        catch (Exception ex)
        {
          _logger.LogError("Failed to extract context, Exception "+ex);

            return context;
        }
    }

    /// <inheritdoc />
    public override void Inject<T>(PropagationContext context, T carrier, Action<T, string, string> setter)
    {
        if (context.ActivityContext.TraceId == default(ActivityTraceId) || context.ActivityContext.SpanId == default(ActivitySpanId))
        {
            _logger.LogError("Failed to extract context, invalid context ");

            return;
        }
        if (carrier == null)
        {
            _logger.LogError("Failed to extract context,  null carrier ");
            return;
        }
        if (setter == null)
        {
            _logger.LogError("Failed to extract context, null setter");

            return;
        }
        string text = "00-" + context.ActivityContext.TraceId.ToHexString() + "-" + context.ActivityContext.SpanId.ToHexString();
        text += (((context.ActivityContext.TraceFlags & ActivityTraceFlags.Recorded) != 0) ? "-01" : "-00");
        setter(carrier, "traceparent", text);
        /*
         * Explicitly picking up the traceid and spanids
         */
        setter(carrier,TraceIdKey, context.ActivityContext.TraceId.ToHexString());
        setter(carrier,SpanIdKey, context.ActivityContext.SpanId.ToHexString());
        string traceState = context.ActivityContext.TraceState;
        if (traceState != null && traceState.Length > 0)
        {
            setter(carrier, "tracestate", traceState);
        }
    }

  

}
