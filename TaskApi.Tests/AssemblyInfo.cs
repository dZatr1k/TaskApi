// Each test class spins up its own PostgreSQL + RabbitMQ containers and an in-process host.
// Running the classes in parallel once failed during fixture cleanup (ObjectDisposedException while two
// hosts were shutting down at the same time). That happened right before RabbitMqPublisher.CloseAsync got
// its guard against an already-disposed semaphore, and did not reproduce in three parallel runs afterwards.
// HACK: kept sequential anyway — deterministic, and half the containers at once on a laptop/CI runner.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
