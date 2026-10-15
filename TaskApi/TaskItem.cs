using System.Text.Json.Serialization;

namespace TaskApi;

public enum Priority
{
    Low,
    Medium,
    High
}

public class TaskItem
{
    public Guid Id { get; set; }

    /// <summary>Not null, not empty, max 200 characters (validated in the endpoint + DB constraint).</summary>
    public string Title { get; set; } = null!;

    public bool IsCompleted { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>Set only when the task gets completed, never on creation.</summary>
    public DateTimeOffset? CompletedAt { get; set; }

    public Priority Priority { get; set; } = Priority.Medium;

    /// <summary>
    /// Optimistic concurrency token. PostgreSQL has no `rowversion` type, so Npgsql maps
    /// <c>IsRowVersion()</c> on a <see cref="uint"/> to the system column <c>xmin</c>,
    /// which changes on every row update. Hidden from API responses.
    /// </summary>
    [JsonIgnore]
    public uint RowVersion { get; set; }
}

#region API contracts

/// <summary>POST /tasks body. Priority is optional and defaults to Medium.</summary>
public record CreateTaskRequest(string? Title, Priority? Priority);

public enum TaskSortBy
{
    CreatedAt,
    Title,
    Priority
}

/// <summary>
/// GET /tasks query string. Everything is optional: no parameters = all tasks, newest first.
/// Bound with [AsParameters]. Priority/SortBy are strings because the built-in enum binding is
/// case-sensitive ("high" would be rejected); they are parsed case-insensitively in the endpoint.
/// </summary>
public record TaskQuery(
    bool? IsCompleted,
    string? Priority,
    string? SortBy,
    bool? Desc,
    int? Offset,
    int? Limit);

#endregion
