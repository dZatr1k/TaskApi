# TaskApi

Тестовое задание: .NET 9 Minimal API + PostgreSQL (EF Core) + RabbitMQ (publish only).

## Запуск

```bash
docker compose up -d          # PostgreSQL :5432 и RabbitMQ :5672 (UI: http://localhost:15672, guest/guest)
cd TaskApi && dotnet run      # миграции применяются при старте
```

Строки подключения — в `TaskApi/appsettings.json`.

## API

| Метод  | Эндпоинт               | Ответы                                   |
|--------|------------------------|------------------------------------------|
| POST   | `/tasks`               | 201 + задача; 400 при невалидном Title/Priority |
| GET    | `/tasks`               | 200 + все задачи (новые первыми)         |
| PUT    | `/tasks/{id}/complete` | 200 + задача; 404; 409 если уже завершена или гонка |
| DELETE | `/tasks/{id}`          | 204; 404                                 |

```bash
curl -X POST localhost:5000/tasks -H 'Content-Type: application/json' -d '{"title":"Buy milk","priority":"High"}'
curl -X PUT  localhost:5000/tasks/<id>/complete
```

## Где что лежит

| Файл                           | Что внутри                                                   |
|--------------------------------|--------------------------------------------------------------|
| `TaskApi/Program.cs`           | DI, миграция при старте, все эндпоинты (по `#region`)        |
| `TaskApi/TaskItem.cs`          | сущность, enum `Priority`, DTO запроса                       |
| `TaskApi/TaskDbContext.cs`     | маппинг, RowVersion через `xmin`                             |
| `TaskApi/RabbitMqService.cs`   | контракт события и публикатор                                |
| `TaskApi/Migrations/`          | EF Core миграции                                             |

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

## Итерации

1. ✅ Ядро API: сущность, БД, эндпоинты, валидация, 409 через RowVersion. Публикатор — заглушка в лог.
2. ✅ Реальный RabbitMQ publisher: неблокирующая публикация, fail silently, graceful shutdown.
3. ⏳ Интеграционный тест: создать → завершить → проверить сообщение.
4. ⏳ Бонусы: пагинация/фильтрация, Swagger.
