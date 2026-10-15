using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;

namespace TaskApi.Tests;

/// <summary>Same as <see cref="TaskApiFixture"/>, but the API is pointed at a port where nothing listens.</summary>
public sealed class BrokerDownFixture : TaskApiFixture
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);
        builder.UseSetting("RabbitMq:Host", "127.0.0.1");
        builder.UseSetting("RabbitMq:Port", "1"); // nothing listens here -> connection refused
    }
}

public sealed class RabbitMqOutageTest(BrokerDownFixture app) : IClassFixture<BrokerDownFixture>
{
    [Fact]
    public async Task Complete_still_succeeds_when_RabbitMq_is_unreachable()
    {
        var client = app.CreateClient();
        var created = (await (await client.PostAsJsonAsync("/tasks", new { title = "Offline broker" }))
            .Content.ReadFromJsonAsync<TaskDto>())!;

        var response = await client.PutAsync($"/tasks/{created.Id}/complete", null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Completion is persisted even though the publish failed (it is only logged).
        var all = (await client.GetFromJsonAsync<List<TaskDto>>("/tasks"))!;
        var stored = Assert.Single(all, t => t.Id == created.Id);
        Assert.True(stored.IsCompleted);
        Assert.NotNull(stored.CompletedAt);
    }
}
