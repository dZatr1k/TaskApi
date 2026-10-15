using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace TaskApi;

#region Contract

/// <summary>Payload published to exchange `task.events` with routing key `task.completed`.</summary>
public record TaskCompletedEvent(Guid TaskId, string Title, DateTimeOffset CompletedAt, Priority Priority);

/// <summary>
/// Fire-and-forget publisher. Implementations must never block the caller and must never throw:
/// a broker outage is logged, not propagated (the task is already completed in the DB at this point).
/// </summary>
public interface ITaskEventPublisher
{
    void Publish(TaskCompletedEvent evt);
}

public sealed class RabbitMqOptions
{
    public const string Section = "RabbitMq";

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string Exchange { get; set; } = "task.events";
    public string RoutingKey { get; set; } = "task.completed";
}

#endregion

#region RabbitMQ publisher

/// <summary>
/// Publishes task events to RabbitMQ off the request thread.
///
/// How it works:
///  - <see cref="Publish"/> only enqueues into an in-memory channel and returns immediately.
///  - A single background loop drains the queue and publishes (with publisher confirms, since
///    waiting for them here costs the HTTP caller nothing).
///  - If the broker is down, the event is logged with its full payload and dropped; the connection
///    is re-established lazily on the next event.
///  - On shutdown the queue is closed for writing, in-flight events get up to <see cref="DrainTimeout"/>
///    to go out, then channel and connection are closed.
///
/// HACK: events that are still queued when the drain timeout hits are lost (logged). A proper fix
/// would be an outbox table written in the same transaction as the task update.
/// </summary>
public sealed class RabbitMqPublisher(IOptions<RabbitMqOptions> options, ILogger<RabbitMqPublisher> logger)
    : BackgroundService, ITaskEventPublisher
{
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(10); // connect + publish + confirm

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly RabbitMqOptions _options = options.Value;

    private readonly Channel<TaskCompletedEvent> _queue =
        Channel.CreateUnbounded<TaskCompletedEvent>(new UnboundedChannelOptions { SingleReader = true });

    // Cancelled only when the drain timeout expires during shutdown; aborts a stuck connect/publish.
    private readonly CancellationTokenSource _hardStop = new();

    // CloseAsync can be reached from the worker loop (after a failed publish) and from StopAsync at the same time.
    private readonly SemaphoreSlim _closeLock = new(1, 1);

    private IConnection? _connection;
    private IChannel? _channel;

    public void Publish(TaskCompletedEvent evt)
    {
        if (!_queue.Writer.TryWrite(evt))
            logger.LogWarning("Publisher is shutting down, dropping task.completed for {TaskId}", evt.TaskId);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Deliberately ignores stoppingToken: StopAsync completes the writer instead, so this loop
        // ends naturally once the queue is drained (or when _hardStop fires after the timeout).
        try
        {
            await foreach (var evt in _queue.Reader.ReadAllAsync(_hardStop.Token))
                await PublishCoreAsync(evt); // never throws, except when _hardStop fires
        }
        catch (OperationCanceledException) when (_hardStop.IsCancellationRequested)
        {
            // drain timeout hit, exit quietly; StopAsync has already logged it
        }
    }

    private async Task PublishCoreAsync(TaskCompletedEvent evt)
    {
        // Per-event budget so a broker that accepts the connection but never confirms cannot wedge the loop.
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_hardStop.Token);
        timeout.CancelAfter(PublishTimeout);
        var ct = timeout.Token;

        // Wire format matches the spec: camelCase, priority as string, completedAt in UTC ("...Z").
        var payload = JsonSerializer.SerializeToUtf8Bytes(new
        {
            taskId = evt.TaskId,
            title = evt.Title,
            completedAt = evt.CompletedAt.UtcDateTime,
            priority = evt.Priority
        }, JsonOptions);

        try
        {
            var channel = await GetChannelAsync(ct);

            var props = new BasicProperties
            {
                ContentType = "application/json",
                DeliveryMode = DeliveryModes.Persistent,
                MessageId = Guid.NewGuid().ToString(),
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
            };

            await channel.BasicPublishAsync(_options.Exchange, _options.RoutingKey, mandatory: false, props, payload, ct);

            logger.LogInformation("Published task.completed for {TaskId}", evt.TaskId);
        }
        catch (OperationCanceledException) when (_hardStop.IsCancellationRequested)
        {
            throw; // shutdown, handled by the caller
        }
        catch (Exception ex) // includes the per-event timeout and any stray OperationCanceledException from the client
        {
            // Fail silently for the API caller, loudly in the log: keep the payload so it can be replayed by hand.
            logger.LogError(ex, "RabbitMQ publish failed, task.completed for {TaskId} was NOT sent. Payload: {Payload}",
                evt.TaskId, System.Text.Encoding.UTF8.GetString(payload));

            await CloseAsync(); // throw the broken connection away, reconnect on next event
        }
    }

    private async Task<IChannel> GetChannelAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true })
            return _channel;

        await CloseAsync();

        var factory = new ConnectionFactory
        {
            HostName = _options.Host,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            ClientProvidedName = "TaskApi",
            RequestedConnectionTimeout = TimeSpan.FromSeconds(5)
        };

        _connection = await factory.CreateConnectionAsync(ct);
        _channel = await _connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            ct);

        await _channel.ExchangeDeclareAsync(_options.Exchange, ExchangeType.Topic, durable: true, autoDelete: false,
            cancellationToken: ct);

        logger.LogInformation("Connected to RabbitMQ at {Host}:{Port}, exchange '{Exchange}' declared",
            _options.Host, _options.Port, _options.Exchange);

        return _channel;
    }

    #region Graceful shutdown

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete(); // no new events; the loop exits once the queue is empty

        if (ExecuteTask is not null)
        {
            try
            {
                await ExecuteTask.WaitAsync(DrainTimeout, cancellationToken);
            }
            catch (TimeoutException)
            {
                logger.LogWarning("RabbitMQ drain timed out after {Timeout}s, {Count} event(s) dropped",
                    DrainTimeout.TotalSeconds, _queue.Reader.Count);
            }
            catch (OperationCanceledException)
            {
                logger.LogWarning("Host forced shutdown, {Count} event(s) dropped", _queue.Reader.Count);
            }

            if (!ExecuteTask.IsCompleted)
            {
                // Abort whatever the worker is stuck on and let it unwind before touching the connection.
                await _hardStop.CancelAsync();
                try { await ExecuteTask.WaitAsync(TimeSpan.FromSeconds(1)); } catch (Exception) { /* exiting anyway */ }
            }
        }

        await CloseAsync();
        await base.StopAsync(cancellationToken);
    }

    private async Task CloseAsync()
    {
        try
        {
            await _closeLock.WaitAsync();
        }
        catch (ObjectDisposedException)
        {
            return; // service already disposed by the host, nothing left to close
        }

        try
        {
            // A broker that stopped answering must not stall shutdown on a polite close handshake.
            using var closeTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            if (_channel is not null)
            {
                if (_channel.IsOpen) await _channel.CloseAsync(cancellationToken: closeTimeout.Token);
                await _channel.DisposeAsync();
            }

            if (_connection is not null)
            {
                if (_connection.IsOpen) await _connection.CloseAsync(cancellationToken: closeTimeout.Token);
                await _connection.DisposeAsync();
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Error while closing RabbitMQ connection (ignored)");
        }
        finally
        {
            _channel = null;
            _connection = null;
            _closeLock.Release();
        }
    }

    public override void Dispose()
    {
        _hardStop.Dispose();
        _closeLock.Dispose();
        base.Dispose();
    }

    #endregion
}

#endregion
