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

#region Endpoints

var tasks = app.MapGroup("/tasks");

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
});

// GET /tasks
tasks.MapGet("", async (TaskDbContext db) =>
    await db.Tasks.AsNoTracking().OrderByDescending(t => t.CreatedAt).ToListAsync());

// PUT /tasks/{id}/complete
tasks.MapPut("/{id}/complete", async (Guid id, TaskDbContext db, ITaskEventPublisher publisher) =>
{
    var task = await db.Tasks.FindAsync(id);
    if (task is null)
        return Results.NotFound();

    // Fast path: already completed by an earlier request.
    if (task.IsCompleted)
        return Results.Conflict(new { error = "Task is already completed." });

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
        return Results.Conflict(new { error = "Task was modified concurrently." });
    }

    // Published strictly AFTER the DB commit: if the DB fails, no message is sent for nothing.
    publisher.Publish(new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt.Value, task.Priority));

    return Results.Ok(task);
});

// DELETE /tasks/{id}
tasks.MapDelete("/{id}", async (Guid id, TaskDbContext db) =>
{
    var deleted = await db.Tasks.Where(t => t.Id == id).ExecuteDeleteAsync();
    return deleted == 0 ? Results.NotFound() : Results.NoContent();
});

#endregion

app.Run();

// Exposes the entry point to WebApplicationFactory in the integration tests.
public partial class Program;
