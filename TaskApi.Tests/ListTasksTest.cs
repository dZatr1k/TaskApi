using System.Net;
using System.Net.Http.Json;

namespace TaskApi.Tests;

/// <summary>Own fixture (fresh database) so the expected ordering is not polluted by other test classes.</summary>
public sealed class ListTasksTest(TaskApiFixture app) : IClassFixture<TaskApiFixture>
{
    [Fact]
    public async Task Filters_sorts_and_pages()
    {
        var client = app.CreateClient();
        foreach (var (title, priority) in new[] { ("b", "Low"), ("c", "High"), ("a", "Medium") })
            Assert.Equal(HttpStatusCode.Created,
                (await client.PostAsJsonAsync("/tasks", new { title, priority })).StatusCode);

        // Default: everything, newest first.
        var all = (await client.GetFromJsonAsync<List<TaskDto>>("/tasks"))!;
        Assert.Equal(["a", "c", "b"], all.Select(t => t.Title));

        // By id (the Location header of POST points here), unknown id -> 404.
        var one = await client.GetFromJsonAsync<TaskDto>($"/tasks/{all[0].Id}");
        Assert.Equal("a", one!.Title);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync($"/tasks/{Guid.NewGuid()}")).StatusCode);

        // Sort by priority rank, not alphabetically (High > Medium > Low).
        var byPriority = (await client.GetFromJsonAsync<List<TaskDto>>("/tasks?sortBy=priority"))!;
        Assert.Equal(["High", "Medium", "Low"], byPriority.Select(t => t.Priority));

        // Case-insensitive filter.
        var high = (await client.GetFromJsonAsync<List<TaskDto>>("/tasks?priority=high"))!;
        Assert.Equal("c", Assert.Single(high).Title);

        // Paging: slice of the title-sorted list, total count in the header.
        var page = await client.GetAsync("/tasks?sortBy=title&desc=false&offset=1&limit=1");
        Assert.Equal("3", page.Headers.GetValues("X-Total-Count").Single());
        Assert.Equal("b", Assert.Single((await page.Content.ReadFromJsonAsync<List<TaskDto>>())!).Title);

        // Bad query values -> 400, all errors at once.
        var bad = await client.GetAsync("/tasks?priority=urgent&limit=0");
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        var body = await bad.Content.ReadAsStringAsync();
        Assert.Contains("Priority", body);
        Assert.Contains("Limit", body);
    }
}
