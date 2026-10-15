using System.Text;
using System.Threading.Channels;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;

namespace TaskApi.Tests;

#region Test host

/// <summary>
/// Real PostgreSQL and RabbitMQ in Docker (Testcontainers) + the API in-process.
/// Requires a running Docker daemon. Shared by all tests in a class (IClassFixture).
/// </summary>
public class TaskApiFixture : WebApplicationFactory<Program>, IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    private readonly RabbitMqContainer _rabbit = new RabbitMqBuilder()
        .WithImage("rabbitmq:3-management-alpine")
        .Build();

    /// <summary>amqp://user:pass@host:port/ — for the test consumer.</summary>
    public string RabbitConnectionString => _rabbit.GetConnectionString();

    public Task InitializeAsync() => Task.WhenAll(_postgres.StartAsync(), _rabbit.StartAsync());

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("ConnectionStrings:Postgres", _postgres.GetConnectionString());

        var amqp = new Uri(_rabbit.GetConnectionString());
        var credentials = amqp.UserInfo.Split(':');
        builder.UseSetting("RabbitMq:Host", amqp.Host);
        builder.UseSetting("RabbitMq:Port", amqp.Port.ToString());
        builder.UseSetting("RabbitMq:UserName", credentials[0]);
        builder.UseSetting("RabbitMq:Password", credentials[1]);
    }

    async Task IAsyncLifetime.DisposeAsync()
    {
        await base.DisposeAsync(); // stops the host -> runs RabbitMqPublisher's graceful shutdown
        await _rabbit.DisposeAsync();
        await _postgres.DisposeAsync();
    }
}

#endregion

#region Test consumer

/// <summary>
/// Binds a throw-away exclusive queue to `task.events` / `task.completed` and collects what arrives.
/// Must be started before the API publishes, otherwise the broker drops the message (no queue bound).
/// </summary>
public sealed class TestConsumer : IAsyncDisposable
{
    public record Delivery(string RoutingKey, string Body);

    private readonly IConnection _connection;
    private readonly IChannel _channel;
    private readonly Channel<Delivery> _received = Channel.CreateUnbounded<Delivery>();

    private TestConsumer(IConnection connection, IChannel channel)
    {
        _connection = connection;
        _channel = channel;
    }

    public static async Task<TestConsumer> StartAsync(string connectionString,
        string exchange = "task.events", string routingKey = "task.completed")
    {
        var connection = await new ConnectionFactory { Uri = new Uri(connectionString) }.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        // Same parameters as the API declares, otherwise RabbitMQ rejects the redeclare.
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Topic, durable: true, autoDelete: false);
        var queue = (await channel.QueueDeclareAsync()).QueueName; // exclusive, auto-delete
        await channel.QueueBindAsync(queue, exchange, routingKey);

        var consumer = new TestConsumer(connection, channel);
        var eventing = new AsyncEventingBasicConsumer(channel);
        eventing.ReceivedAsync += (_, ea) =>
        {
            consumer._received.Writer.TryWrite(new Delivery(ea.RoutingKey, Encoding.UTF8.GetString(ea.Body.Span)));
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync(queue, autoAck: true, eventing);

        return consumer;
    }

    public async Task<Delivery> WaitForMessageAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return await _received.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException($"No message received within {timeout.TotalSeconds}s");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _channel.DisposeAsync();
        await _connection.DisposeAsync();
    }
}

#endregion
