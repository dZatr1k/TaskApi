// Each test class spins up its own PostgreSQL + RabbitMQ containers and an in-process host.
// Running the classes in parallel occasionally failed during fixture cleanup (ObjectDisposedException
// while two hosts were shutting down at once) and doubles the Docker load for no real gain at this size.
// HACK: not root-caused; sequential execution makes the suite deterministic, which matters more here.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
