using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace TaskApi.Tests;

/// <summary>
/// Own fixture instance (IClassFixture is per class): this test stops the host, so it must not share it.
/// </summary>
public sealed class GracefulShutdownTest(TaskApiFixture app) : IClassFixture<TaskApiFixture>
{
    [Fact]
    public async Task Event_enqueued_right_before_shutdown_is_still_published()
    {
        await using var rabbit = await TestConsumer.StartAsync(app.RabbitConnectionString);
        var client = app.CreateClient();

        var created = (await (await client.PostAsJsonAsync("/tasks", new { title = "Last one out" }))
            .Content.ReadFromJsonAsync<TaskDto>())!;

        var completeResponse = await client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        // Same path as Ctrl+C / SIGTERM: ConsoleLifetime calls StopApplication(). At this point the event is
        // at best mid-connect in the background worker; the drain window must let it go out.
        var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lifetime.ApplicationStopped.Register(() => stopped.TrySetResult());

        lifetime.StopApplication();
        await stopped.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var message = await rabbit.WaitForMessageAsync(TimeSpan.FromSeconds(5));
        using var json = JsonDocument.Parse(message.Body);
        Assert.Equal(created.Id, json.RootElement.GetProperty("taskId").GetGuid());
    }
}
