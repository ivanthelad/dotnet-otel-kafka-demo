using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using OpenTelemetry.Internal;
/// <summary>
/// A text map propagator for W3C trace context. See https://w3c.github.io/trace-context/.
/// </summary>
public class KafkaContextPropagator : TextMapPropagator
{
    private const string TraceParent = "traceparent";

    private const string TraceState = "tracestate";

    private const string spanid = "Activity.SpanId";
    private const string traceid = "Activity.TraceId";

    private static readonly int VersionPrefixIdLength = "00-".Length;

    private static readonly int TraceIdLength = "0af7651916cd43dd8448eb211c80319c".Length;

    private static readonly int VersionAndTraceIdLength = "00-0af7651916cd43dd8448eb211c80319c-".Length;

    private static readonly int SpanIdLength = "00f067aa0ba902b7".Length;

    private static readonly int VersionAndTraceIdAndSpanIdLength = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-".Length;

    private static readonly int OptionsLength = "00".Length;

    private static readonly int TraceparentLengthV0 = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-00".Length;

    private static readonly int TraceStateKeyMaxLength = 256;

    private static readonly int TraceStateKeyTenantMaxLength = 241;

    private static readonly int TraceStateKeyVendorMaxLength = 14;

    private static readonly int TraceStateValueMaxLength = 256;
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
    public override ISet<string> Fields => new HashSet<string> { "tracestate", "traceparent", "Activity.TraceId", "Activity.SpanId" };

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
            IEnumerable<string> enumerable = getter(carrier, "Activity.TraceId");
            if (enumerable == null || enumerable.Count() != 1)
            {
                return context;
            }
            
            ActivityTraceId traceId = ActivityTraceId.CreateFromString(enumerable.First());
           
        
            IEnumerable<string> enumerable2 = getter(carrier, "Activity.SpanId");
            ActivitySpanId spanId;
            if (enumerable2 != null && enumerable2.Any())
            {
                spanId = ActivitySpanId.CreateFromString(enumerable2.First());
            }
            else
                return context;
            //TODO look into propagating the tracestate and trace flages
            return new PropagationContext(new ActivityContext(traceId, spanId,ActivityTraceFlags.Recorded, null, isRemote: true), context.Baggage);
        }
        catch (Exception ex)
        {
          //  OpenTelemetryApiEventSource.Log.ActivityContextExtractException("TraceContextPropagator", ex);
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
        setter(carrier,"Activity.TraceId", context.ActivityContext.TraceId.ToHexString());
        setter(carrier,"Activity.SpanId", context.ActivityContext.SpanId.ToHexString());
        string traceState = context.ActivityContext.TraceState;
        if (traceState != null && traceState.Length > 0)
        {
            setter(carrier, "tracestate", traceState);
        }
    }

  

}
