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

## Итерации

1. ✅ Ядро API: сущность, БД, эндпоинты, валидация, 409 через RowVersion. Публикатор — заглушка в лог.
2. ⏳ Реальный RabbitMQ publisher: неблокирующая публикация, fail silently, graceful shutdown.
3. ⏳ Интеграционный тест: создать → завершить → проверить сообщение.
4. ⏳ Бонусы: пагинация/фильтрация, Swagger.
