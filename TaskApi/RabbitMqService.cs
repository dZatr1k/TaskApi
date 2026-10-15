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

#endregion

#region Logging stub

/// <summary>
/// Iteration 1 placeholder: writes the event to the log instead of RabbitMQ.
/// Replaced by the real RabbitMQ publisher in the next iteration; kept for tests.
/// </summary>
public sealed class LoggingTaskEventPublisher(ILogger<LoggingTaskEventPublisher> logger) : ITaskEventPublisher
{
    public void Publish(TaskCompletedEvent evt) =>
        logger.LogInformation("[stub] task.completed: {TaskId} \"{Title}\" at {CompletedAt} ({Priority})",
            evt.TaskId, evt.Title, evt.CompletedAt, evt.Priority);
}

#endregion
