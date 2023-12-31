# Проектная работа по аналитическим базам данных
### Используемые технологии: Airflow, Python, Vertica, SQL, Postgres, Pandas, Docker

## Описание задачи
Чтобы привлечь новых пользователей, маркетологи хотят разместить на сторонних сайтах рекламу сообществ с высокой активностью. Вам нужно определить группы, в которых начала общаться большая часть их участников. В терминологии маркетинга их бы назвали пабликами с высокой конверсией в первое сообщение.
В группе А конверсия выше, чем в Б. Хотя в группе А сейчас общается только 40 пользователей соцсети, а в Б — 50, доля активных в А выше, ведь в ней всего 50 человек. В то время как в группе Б сообщения написали уже 50 участников, но это лишь половина от общего количества — 100. Значит, если в обе группы вступит одинаковое число людей, эффективнее сработает сообщество А, потому что оно лучше вовлекает своих участников. Получается, что для рекламы соцсети стоит выбрать группу А и другие паблики с высокой конверсией. Ваша задача — выявить и перечислить маркетологам такие сообщества.

## План работ
1. Загрузить данные из S3
2. Создать таблицу group_log в Vertica
3. Загрузить данные в Vertica
4. Создать таблицу связи
5. Создать скрипты миграции в таблицу связи
6. Создать и наполнить сателлит
7. Подготовить CTE для ответов бизнесу
7.1. Подготовить CTE user_group_messages
7.2. Подготовить CTE user_group_log
7.3. Написать запрос и ответить на вопрос бизнеса
