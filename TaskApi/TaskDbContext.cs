using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public class TaskDbContext(DbContextOptions<TaskDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var task = modelBuilder.Entity<TaskItem>();

        task.ToTable("tasks");
        task.HasKey(t => t.Id);

        task.Property(t => t.Title)
            .IsRequired()
            .HasMaxLength(200);

        // Stored as text ("Low"/"Medium"/"High") so the table is readable without a lookup.
        task.Property(t => t.Priority)
            .HasConversion<string>()
            .HasMaxLength(16);

        // Concurrency token -> PostgreSQL system column xmin (see TaskItem.RowVersion).
        task.Property(t => t.RowVersion)
            .IsRowVersion();

        task.HasIndex(t => t.CreatedAt);
    }
}
