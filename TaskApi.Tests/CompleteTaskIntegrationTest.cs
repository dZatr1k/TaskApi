using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace TaskApi.Tests;

/// <summary>Shape of the API response; Priority kept as string to assert the wire format.</summary>
public record TaskDto(Guid Id, string Title, bool IsCompleted, DateTimeOffset CreatedAt, DateTimeOffset? CompletedAt, string Priority);

public sealed class CompleteTaskIntegrationTest(TaskApiFixture app) : IClassFixture<TaskApiFixture>
{
    [Fact]
    public async Task Create_then_complete_publishes_task_completed_to_RabbitMq()
    {
        await using var rabbit = await TestConsumer.StartAsync(app.RabbitConnectionString);
        var client = app.CreateClient();

        // 1. Create
        var createResponse = await client.PostAsJsonAsync("/tasks", new { title = "Buy milk", priority = "High" });
        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);

        var created = (await createResponse.Content.ReadFromJsonAsync<TaskDto>())!;
        Assert.Equal("Buy milk", created.Title);
        Assert.Equal("High", created.Priority);
        Assert.False(created.IsCompleted);
        Assert.Null(created.CompletedAt); // must NOT be set on creation

        // 2. Complete
        var completeResponse = await client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        var completed = (await completeResponse.Content.ReadFromJsonAsync<TaskDto>())!;
        Assert.True(completed.IsCompleted);
        Assert.NotNull(completed.CompletedAt);

        // 3. RabbitMQ received the message (publish is async, so wait a bit)
        var message = await rabbit.WaitForMessageAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("task.completed", message.RoutingKey);

        using var json = JsonDocument.Parse(message.Body);
        var root = json.RootElement;
        Assert.Equal(created.Id, root.GetProperty("taskId").GetGuid());
        Assert.Equal("Buy milk", root.GetProperty("title").GetString());
        Assert.Equal("High", root.GetProperty("priority").GetString());
        Assert.Equal(completed.CompletedAt.Value, root.GetProperty("completedAt").GetDateTimeOffset());
    }

    [Fact]
    public async Task Concurrent_complete_calls_yield_one_200_and_the_rest_409()
    {
        var client = app.CreateClient();
        var created = (await (await client.PostAsJsonAsync("/tasks", new { title = "Race me" }))
            .Content.ReadFromJsonAsync<TaskDto>())!;

        var responses = await Task.WhenAll(Enumerable.Range(0, 10)
            .Select(_ => client.PutAsync($"/tasks/{created.Id}/complete", null)));

        var statuses = responses.Select(r => r.StatusCode).ToList();
        Assert.Equal(1, statuses.Count(s => s == HttpStatusCode.OK));
        Assert.Equal(9, statuses.Count(s => s == HttpStatusCode.Conflict));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public async Task Create_with_invalid_title_returns_400(string? title)
    {
        var client = app.CreateClient();

        var response = await client.PostAsJsonAsync("/tasks", new { title });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Create_with_title_over_200_chars_returns_400()
    {
        var client = app.CreateClient();

        var response = await client.PostAsJsonAsync("/tasks", new { title = new string('a', 201) });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }
}
