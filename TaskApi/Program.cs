using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using TaskApi;

var builder = WebApplication.CreateBuilder(args);

#region Services

// Enums travel as strings in JSON ("High"); an unknown value fails model binding -> 400.
builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

builder.Services.AddDbContext<TaskDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

// One singleton serves both roles: the endpoints enqueue through ITaskEventPublisher,
// the host runs it as a background service that drains the queue into RabbitMQ.
builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection(RabbitMqOptions.Section));
builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddSingleton<ITaskEventPublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<RabbitMqPublisher>());

// Unhandled exceptions and bare status codes come back as RFC 7807 problem+json instead of HTML/empty bodies.
builder.Services.AddProblemDetails();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

#endregion

var app = builder.Build();

#region Database migration on startup

// Keeps "docker compose up && dotnet run" a one-step setup. Fine for a test assignment;
// in production migrations would run as a separate deploy step.
using (var scope = app.Services.CreateScope())
{
    await scope.ServiceProvider.GetRequiredService<TaskDbContext>().Database.MigrateAsync();
}

#endregion

#region Middleware

// In Development Minimal APIs throw BadHttpRequestException on binding failures (ThrowOnBadRequest);
// keep those as 400 instead of letting the handler turn them into 500.
app.UseExceptionHandler(new ExceptionHandlerOptions
{
    StatusCodeSelector = ex => ex is BadHttpRequestException bad ? bad.StatusCode : StatusCodes.Status500InternalServerError
});
app.UseStatusCodePages();

app.UseSwagger();
app.UseSwaggerUI(); // http://localhost:<port>/swagger

#endregion

#region Endpoints

var tasks = app.MapGroup("/tasks").WithTags("Tasks");

// POST /tasks
tasks.MapPost("", async (CreateTaskRequest request, TaskDbContext db) =>
{
    var title = request.Title?.Trim();

    if (string.IsNullOrEmpty(title))
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            [nameof(request.Title)] = ["Title must not be empty."]
        });

    if (title.Length > 200)
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            [nameof(request.Title)] = ["Title must be at most 200 characters."]
        });

    var task = new TaskItem
    {
        Id = Guid.NewGuid(),
        Title = title,
        IsCompleted = false,
        CreatedAt = DateTimeOffset.UtcNow,
        CompletedAt = null, // explicitly: only set on completion
        Priority = request.Priority ?? Priority.Medium
    };

    db.Tasks.Add(task);
    await db.SaveChangesAsync();

    return Results.Created($"/tasks/{task.Id}", task);
})
.Produces<TaskItem>(StatusCodes.Status201Created)
.ProducesValidationProblem();

// GET /tasks
// Without parameters returns ALL tasks, newest first (as the spec asks). Filtering, sorting and
// offset/limit paging are opt-in via query string; X-Total-Count carries the count before paging.
tasks.MapGet("", async ([AsParameters] TaskQuery query, TaskDbContext db, HttpResponse response) =>
{
    var errors = new Dictionary<string, string[]>();

    if (!TryParseEnum<Priority>(query.Priority, out var priority))
        errors[nameof(query.Priority)] = [$"Priority must be one of: {string.Join(", ", Enum.GetNames<Priority>())}."];

    if (!TryParseEnum<TaskSortBy>(query.SortBy, out var sortBy))
        errors[nameof(query.SortBy)] = [$"SortBy must be one of: {string.Join(", ", Enum.GetNames<TaskSortBy>())}."];

    if (query.Offset is < 0)
        errors[nameof(query.Offset)] = ["Offset must be >= 0."];

    if (query.Limit is < 1 or > 500)
        errors[nameof(query.Limit)] = ["Limit must be between 1 and 500."];

    if (errors.Count > 0)
        return Results.ValidationProblem(errors);

    var items = db.Tasks.AsNoTracking();

    if (query.IsCompleted is { } isCompleted)
        items = items.Where(t => t.IsCompleted == isCompleted);

    if (priority is { } p)
        items = items.Where(t => t.Priority == p);

    var total = await items.CountAsync();

    var desc = query.Desc ?? true;
    items = (sortBy, desc) switch
    {
        (TaskSortBy.Title, true) => items.OrderByDescending(t => t.Title),
        (TaskSortBy.Title, false) => items.OrderBy(t => t.Title),
        // Priority is stored as text, so order by rank (translates to CASE WHEN), not alphabetically.
        (TaskSortBy.Priority, true) => items.OrderByDescending(PriorityRank).ThenByDescending(t => t.CreatedAt),
        (TaskSortBy.Priority, false) => items.OrderBy(PriorityRank).ThenByDescending(t => t.CreatedAt),
        (_, false) => items.OrderBy(t => t.CreatedAt),
        _ => items.OrderByDescending(t => t.CreatedAt)
    };

    if (query.Offset is > 0)
        items = items.Skip(query.Offset.Value);

    if (query.Limit is { } limit)
        items = items.Take(limit);

    response.Headers["X-Total-Count"] = total.ToString();
    return Results.Ok(await items.ToListAsync());
})
.Produces<List<TaskItem>>()
.ProducesValidationProblem();

// PUT /tasks/{id}/complete
tasks.MapPut("/{id}/complete", async (Guid id, TaskDbContext db, ITaskEventPublisher publisher) =>
{
    var task = await db.Tasks.FindAsync(id);
    if (task is null)
        return Results.NotFound();

    // Fast path: already completed by an earlier request.
    if (task.IsCompleted)
        return Results.Problem(statusCode: StatusCodes.Status409Conflict, detail: "Task is already completed.");

    task.IsCompleted = true;
    task.CompletedAt = DateTimeOffset.UtcNow;

    try
    {
        await db.SaveChangesAsync();
    }
    catch (DbUpdateConcurrencyException)
    {
        // Race: another request completed (or deleted) this row between our read and our write.
        // The UPDATE ... WHERE xmin = @original matched 0 rows, so nothing was overwritten.
        return Results.Problem(statusCode: StatusCodes.Status409Conflict, detail: "Task was modified concurrently.");
    }

    // Published strictly AFTER the DB commit: if the DB fails, no message is sent for nothing.
    publisher.Publish(new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt.Value, task.Priority));

    return Results.Ok(task);
})
.Produces<TaskItem>()
.ProducesProblem(StatusCodes.Status404NotFound)
.ProducesProblem(StatusCodes.Status409Conflict);

// DELETE /tasks/{id}
tasks.MapDelete("/{id}", async (Guid id, TaskDbContext db) =>
{
    var deleted = await db.Tasks.Where(t => t.Id == id).ExecuteDeleteAsync();
    return deleted == 0 ? Results.NotFound() : Results.NoContent();
})
.Produces(StatusCodes.Status204NoContent)
.ProducesProblem(StatusCodes.Status404NotFound);

#endregion

app.Run();

#region Helpers

// Also exposes the entry point to WebApplicationFactory in the integration tests.
public partial class Program
{
    /// <summary>Low=0, Medium=1, High=2 as a translatable expression (CASE WHEN in SQL).</summary>
    private static readonly System.Linq.Expressions.Expression<Func<TaskItem, int>> PriorityRank =
        t => t.Priority == Priority.High ? 2 : t.Priority == Priority.Medium ? 1 : 0;

    /// <summary>
    /// Case-insensitive enum parse by name only; null/empty input is valid and yields null.
    /// (Enum.TryParse would also accept "2" and "Low,High", which is not what a query string should allow.)
    /// </summary>
    private static bool TryParseEnum<TEnum>(string? value, out TEnum? result) where TEnum : struct, Enum
    {
        result = null;
        if (string.IsNullOrWhiteSpace(value))
            return true;

        var name = Enum.GetNames<TEnum>().FirstOrDefault(n => n.Equals(value.Trim(), StringComparison.OrdinalIgnoreCase));
        if (name is null)
            return false;

        result = Enum.Parse<TEnum>(name);
        return true;
    }
}

#endregion
