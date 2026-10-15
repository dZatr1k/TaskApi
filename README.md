# TaskApi

[![CI](https://github.com/dZatr1k/TaskApi/actions/workflows/ci.yml/badge.svg)](https://github.com/dZatr1k/TaskApi/actions/workflows/ci.yml)

Тестовое задание: .NET 9 Minimal API + PostgreSQL (EF Core) + RabbitMQ (publish only).

## Запуск

```bash
docker compose up -d          # PostgreSQL :5432 и RabbitMQ :5672 (UI: http://localhost:15672, guest/guest)
cd TaskApi && dotnet run      # миграции применяются при старте
```

Swagger UI: http://localhost:5220/swagger (порт — см. `Properties/launchSettings.json`).
Готовые запросы — в `TaskApi/TaskApi.http`.

Полностью в Docker (API на http://localhost:8080):

```bash
docker compose --profile app up -d --build
```

Строки подключения — в `TaskApi/appsettings.json`.

## Тесты

```bash
dotnet test
```

Нужен запущенный Docker: тесты через Testcontainers поднимают свои PostgreSQL и RabbitMQ (те же образы, что в
`docker-compose.yml`), API стартует in-process через `WebApplicationFactory`. Остановка фабрики гоняет и
graceful shutdown publisher'а.

| Тест (`TaskApi.Tests/CompleteTaskIntegrationTest.cs`)        | Что проверяет                                                   |
|--------------------------------------------------------------|-----------------------------------------------------------------|
| `Create_then_complete_publishes_task_completed_to_RabbitMq`  | создать → завершить → `TestConsumer` получил сообщение с нужными полями |
| `Concurrent_complete_calls_yield_one_200_and_the_rest_409`   | 10 параллельных `/complete`: ровно один 200, остальные 409      |
| `Create_with_invalid_title_returns_400` / `..._over_200_chars` | валидация Title                                               |
| `RabbitMqOutageTest` (отдельная фикстура, брокер на мёртвом порту) | `/complete` возвращает 200 и задача сохранена, даже если публикация упала |
| `GracefulShutdownTest` (отдельная фикстура) | `/complete` и сразу `StopApplication()` (путь Ctrl+C) — сообщение всё равно доходит до RabbitMQ |
| `ListTasksTest` (отдельная фикстура) | порядок по умолчанию, сортировка по рангу приоритета, фильтр, offset/limit + `X-Total-Count`, 400 на мусор в query |

## API

| Метод  | Эндпоинт               | Ответы                                   |
|--------|------------------------|------------------------------------------|
| POST   | `/tasks`               | 201 + задача; 400 при невалидном Title/Priority |
| GET    | `/tasks`               | 200 + все задачи (новые первыми); опционально `?isCompleted=&priority=&sortBy=createdAt\|title\|priority&desc=&offset=&limit=`, заголовок `X-Total-Count` |
| PUT    | `/tasks/{id}/complete` | 200 + задача; 404; 409 если уже завершена или гонка |
| DELETE | `/tasks/{id}`          | 204; 404                                 |

```bash
curl -X POST localhost:5220/tasks -H 'Content-Type: application/json' -d '{"title":"Buy milk","priority":"High"}'
curl -X PUT  localhost:5220/tasks/<id>/complete
```

## Где что лежит

| Файл                           | Что внутри                                                   |
|--------------------------------|--------------------------------------------------------------|
| `TaskApi/Program.cs`           | DI, миграция при старте, все эндпоинты (по `#region`)        |
| `TaskApi/TaskItem.cs`          | сущность, enum `Priority`, DTO запроса/ответа и query        |
| `TaskApi/TaskDbContext.cs`     | маппинг, RowVersion через `xmin`                             |
| `TaskApi/RabbitMqService.cs`   | контракт события и публикатор                                |
| `TaskApi/Migrations/`          | EF Core миграции                                             |
| `TaskApi.Tests/TaskApiFixture.cs` | Testcontainers + `WebApplicationFactory`, `TestConsumer` для RabbitMQ |
| `TaskApi.Tests/CompleteTaskIntegrationTest.cs` | интеграционные тесты                            |

## Решения

- **Конкурентность.** В PostgreSQL нет типа `rowversion`, поэтому concurrency token — системный столбец `xmin`
  (`IsRowVersion()` на `uint` в Npgsql). Второй из двух одновременных `/complete` получает
  `DbUpdateConcurrencyException` → 409. Повторный вызов на уже завершённой задаче — тоже 409, без похода в UPDATE.
- **CompletedAt** выставляется только в `/complete`, при создании всегда `null`.
- **Публикация в RabbitMQ** происходит строго после `SaveChangesAsync`: если БД упала, сообщение не уходит.
- Валидация Title — обычными `if`, остальное (enum, guid) — привязкой модели (400 от фреймворка).
- **RabbitMQ** (`RabbitMqService.cs`): `Publish()` только кладёт событие в in-memory `Channel<T>` и возвращается,
  HTTP-запрос брокера не ждёт. Один фоновый `BackgroundService` дренирует очередь и публикует в exchange
  `task.events` (topic, durable) с routing key `task.completed`, с publisher confirms — раз это уже не на
  пути запроса, подождать ack дёшево. Если брокер недоступен — ошибка и полный payload пишутся в лог,
  событие отбрасывается, соединение пересоздаётся на следующем событии.
- **Graceful shutdown**: на остановке очередь закрывается на запись, воркеру даётся 5 с дослать то, что уже
  в очереди, затем канал и соединение закрываются. Проверено: Ctrl+C сразу после `/complete` — сообщение
  доходит, процесс завершается.
- `// HACK`: события, не успевшие уйти за 5 с, теряются (с логом). Правильно — outbox-таблица в той же
  транзакции, что и завершение задачи.
- Защита воркера: таймаут 10 с на одну публикацию (connect + publish + confirm), чужие
  `OperationCanceledException` из клиента не роняют цикл (иначе `BackgroundService` остановил бы хост),
  закрытие канала/соединения ограничено 2 с и сериализовано семафором.

## Что доделал бы при наличии времени

- **Outbox** вместо in-memory очереди: запись события в таблицу в той же транзакции, что и `IsCompleted = true`,
  и отдельный воркер-отправитель. Сейчас при падении процесса между commit и publish событие теряется.
- Разобраться, почему два параллельных `WebApplicationFactory` иногда падают на cleanup
  (`ObjectDisposedException`) — пока классы тестов выполняются последовательно
  (`TaskApi.Tests/AssemblyInfo.cs`).
- Миграции как отдельный шаг деплоя, а не при старте приложения.

## Итерации

1. ✅ Ядро API: сущность, БД, эндпоинты, валидация, 409 через RowVersion. Публикатор — заглушка в лог.
2. ✅ Реальный RabbitMQ publisher: неблокирующая публикация, fail silently, graceful shutdown.
3. ✅ Интеграционные тесты на Testcontainers: создать → завершить → сообщение в RabbitMQ; 409 на гонке; 400 на Title.
4. ✅ Бонусы: фильтрация/сортировка/offset-limit в GET, ошибки в формате ProblemDetails (RFC 7807), Swagger UI.
5. ✅ CI на GitHub Actions (тесты на Testcontainers в ubuntu-runner).
6. ✅ Тест «брокер недоступен → задача всё равно завершена».
7. ✅ Dockerfile + compose-профиль `app`, `TaskApi.http`.
8. ✅ Hardening publisher после ревью (таймауты, чужие OCE, сериализованный Close), строгий парсинг enum в query.
9. ✅ Тест graceful shutdown.
10. ✅ Тест фильтрации/сортировки/пагинации GET /tasks.
11. ✅ Ответные DTO (`TaskResponse`) вместо отдачи сущности.
