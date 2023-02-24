# Проектная работа по организации Data Lake

## Задачи проекта

1. В рамках развития соцсети реализовать систему рекомендации друзей.
2. Помочь запустить монетизацию соцсети - провести геоаналитику.

## Описание задач

### Рекомендация друзей

Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:

* состоят в одном канале,
* раньше никогда не переписывались,
* находятся не дальше 1 км друг от друга.

### Геоаналитика

* Выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки.
* Посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей.
* Определить, как часто пользователи путешествуют, и какие города выбирают.

## Описание данных

### Raw Data

Слой сырых данных (AS IS).

#### Таблица событий

Таблица событий находится в HDFS по пути: /user/master/data/geo/events.

Структура таблицы событий:

**root** <br>
&nbsp;|-- **event**: struct (nullable = true) - структура события; <br>
&nbsp;|&nbsp;&nbsp;|-- **admins**: array (nullable = true) - массив пользователей-админов канала; <br>
&nbsp;|&nbsp;&nbsp;|&nbsp;&nbsp;|-- **element**: long (containsNull = true) - идентификатор пользователя; <br>
&nbsp;|&nbsp;&nbsp;|-- **channel_id**: long (nullable = true) - идентификатор канала; <br>
&nbsp;|&nbsp;&nbsp;|-- **datetime**: string (nullable = true) - дата и время сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **media**: struct (nullable = true) - информация о вложении; <br>
&nbsp;|&nbsp;&nbsp;|&nbsp;&nbsp;|-- **media_type**: string (nullable = true) - тип вложения; <br>
&nbsp;|&nbsp;&nbsp;|&nbsp;&nbsp;|-- **src**: string (nullable = true) - ссылка на вложение; <br>
&nbsp;|&nbsp;&nbsp;|-- **message**: string (nullable = true) - текст сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_channel_to**: long (nullable = true) - идентификатор канала сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_from**: long (nullable = true) - идентификатор отправителя; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_group**: long (nullable = true) - идентификатор группы; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_id**: long (nullable = true) - идентификатор сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_to**: long (nullable = true) - идентификатор получателя; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_ts**: string (nullable = true) - метка времени сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **reaction_from**: string (nullable = true) - идентификатор пользователя реакции; <br>
&nbsp;|&nbsp;&nbsp;|-- **reaction_type**: string (nullable = true) - тип реакции [like, dislike]; <br>
&nbsp;|&nbsp;&nbsp;|-- **subscription_channel**: long (nullable = true) - идентификатор подписки на канал; <br>
&nbsp;|&nbsp;&nbsp;|-- **subscription_user**: string (nullable = true) - идентификатор подписки на пользователя; <br>
&nbsp;|&nbsp;&nbsp;|-- **tags**: array (nullable = true) - массив тегов сообщения; <br>
&nbsp;|&nbsp;&nbsp;|&nbsp;&nbsp;|-- **element**: string (containsNull = true) - значение тега; <br>
&nbsp;|&nbsp;&nbsp;|-- **user**: string (nullable = true) - идентификатор пользователя подписки; <br>
&nbsp;|-- **event_type**: string (nullable = true) - тип события [message, subscription, reaction]; <br>
&nbsp;|-- **lat**: double (nullable = true) - географическая широта события; <br>
&nbsp;|-- **lon**: double (nullable = true) - географическая долгота события; <br>
&nbsp;|-- **date**: date (nullable = true) - дата события; <br>

#### Таблица городов

Таблица координат городов Австралии находит я в файле geo.csv

Структура таблицы городов:

* **id** - идентификатор города;
* **city** - наименование города;
* **lat** - географическая широта города;
* **lng** - географическая долгота города.

### ODS

Слой предобработанных данных.
Расположен в HDFS по пути /user/qwertyqwer/data/pre_processed.
Формат данных Apache Parquet.

#### Города

Расположение: /user/qwertyqwer/data/pre_processed/cities

Структура таблицы:

**root** <br>
&nbsp;|-- **city_id**: integer (nullable = true) <br>
&nbsp;|-- **city_name**: string (nullable = true) <br>
&nbsp;|-- **city_lat**: double (nullable = true) <br>
&nbsp;|-- **city_lon**: double (nullable = true) <br>

#### События

Расположение: /user/qwertyqwer/data/pre_processed/events

Структура таблицы:

**root** <br>
&nbsp;|-- **event**: struct (nullable = true) - структура события; <br>
&nbsp;|&nbsp;&nbsp;|-- **admins**: array (nullable = true) - массив пользователей-админов канала; <br>
&nbsp;|&nbsp;&nbsp;|&nbsp;&nbsp;|-- **element**: long (containsNull = true) - идентификатор пользователя; <br>
&nbsp;|&nbsp;&nbsp;|-- **channel_id**: long (nullable = true) - идентификатор канала; <br>
&nbsp;|&nbsp;&nbsp;|-- **datetime**: string (nullable = true) - дата и время сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **media**: struct (nullable = true) - информация о вложении; <br>
&nbsp;|&nbsp;&nbsp;|&nbsp;&nbsp;|-- **media_type**: string (nullable = true) - тип вложения; <br>
&nbsp;|&nbsp;&nbsp;|&nbsp;&nbsp;|-- **src**: string (nullable = true) - ссылка на вложение; <br>
&nbsp;|&nbsp;&nbsp;|-- **message**: string (nullable = true) - текст сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_channel_to**: long (nullable = true) - идентификатор канала сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_from**: long (nullable = true) - идентификатор отправителя; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_group**: long (nullable = true) - идентификатор группы; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_id**: long (nullable = true) - идентификатор сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_to**: long (nullable = true) - идентификатор получателя; <br>
&nbsp;|&nbsp;&nbsp;|-- **message_ts**: string (nullable = true) - метка времени сообщения; <br>
&nbsp;|&nbsp;&nbsp;|-- **reaction_from**: string (nullable = true) - идентификатор пользователя реакции; <br>
&nbsp;|&nbsp;&nbsp;|-- **reaction_type**: string (nullable = true) - тип реакции [like, dislike]; <br>
&nbsp;|&nbsp;&nbsp;|-- **subscription_channel**: long (nullable = true) - идентификатор подписки на канал; <br>
&nbsp;|&nbsp;&nbsp;|-- **subscription_user**: string (nullable = true) - идентификатор подписки на пользователя; <br>
&nbsp;|&nbsp;&nbsp;|-- **tags**: array (nullable = true) - массив тегов сообщения; <br>
&nbsp;|&nbsp;&nbsp;|&nbsp;&nbsp;|-- **element**: string (containsNull = true) - значение тега; <br>
&nbsp;|&nbsp;&nbsp;|-- **user**: string (nullable = true) - идентификатор пользователя подписки; <br>
&nbsp;|-- **event_type**: string (nullable = true) - тип события [message, subscription, reaction]; <br>
&nbsp;|-- **lat**: double (nullable = true) - географическая широта события; <br>
&nbsp;|-- **lon**: double (nullable = true) - географическая долгота события; <br>
&nbsp;|-- **date**: date (nullable = true) - дата события; <br>
&nbsp;|-- **city_id**: integer (nullable = true) - идентификатор ближайшего города. <br>

Партиции: **date**, **event_type**

### Data Sandbox

Слой песочницы - для хранения промежуточных расчетов.
Расположен в HDFS по пути /user/qwertyqwer/data/analytics.
Формат данных Apache Parquet.

#### Кросс пользователи

Таблица хранит декартово произведение активных пользователей по городам.
Из таблицы исключаются пары пользователей между которыми было хотя бы одно сообщение.

Расположение: /user/qwertyqwer/data/analytics/cross_users

Структура таблицы:

**root** <br>
&nbsp;|-- **left_user**: long (nullable = true) - идентификатор первого пользователя; <br>
&nbsp;|-- **right_user**: long (nullable = true) - идентификатор второго пользователя; <br>
&nbsp;|-- **left_lat**: double (nullable = true) - координата последнего сообщения первого пользователя; <br>
&nbsp;|-- **left_lon**: double (nullable = true) - координата последнего сообщения первого пользователя; <br>
&nbsp;|-- **right_lat**: double (nullable = true) - координата последнего сообщения второго пользователя; <br>
&nbsp;|-- **right_lon**: double (nullable = true) - координата последнего сообщения второго пользователя; <br>
&nbsp;|-- **city_id**: integer (nullable = true) - город из которого было отправлено последнее сообщение обоих
пользователей. <br>

Партиции: **city_id**

#### Ближайшие пользователи

Таблица рассчитывается на основании кросс пользователей.
Считается дистанция между координатами последних сообщений пользователей.
Выбираются только те пары пользователей, дистанция между которыми меньше или равна 1 км.

Расположение: /user/qwertyqwer/data/analytics/nearest_users

Структура таблицы:

**root** <br>
|-- **left_user**: long (nullable = true) - идентификатор первого пользователя; <br>
|-- **right_user**: long (nullable = true) - идентификатор второго пользователя; <br>
|-- **dist**: string (nullable = true) - расстояние между координатами последнего отправленного сообщения первого и
второго пользователя; <br>
|-- **city_id**: integer (nullable = true) - город из которого было отправлено последнее сообщение обоих
пользователей. <br>

Партиции: **city_id**

### Data Marts

Слой витрин данных.
Расположен в HDFS по пути /user/qwertyqwer/data/prod.
Формат данных Apache Parquet.

#### Витрина локации пользователей

Расположение витрины: /user/qwertyqwer/data/prod/users_locations

Структура витрины:

* **user_id** — идентификатор пользователя.
* **act_city** — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
* **home_city** — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
* **travel_count** — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это
  считается за отдельное посещение.
* **travel_array** — список городов в порядке посещения.
* **local_time** — местное время. Время последнего события пользователя, о котором у нас есть данные с учётом таймзоны
  геопозициии этого события.

#### Витрина статистика по геозонам

Расположение витрины: /user/qwertyqwer/data/prod/geo_zones_stats

Структура витрины:

* **month** — месяц расчёта;
* **week** — неделя расчёта;
* **zone_id** — идентификатор зоны (города);
* **week_message** — количество сообщений за неделю;
* **week_reaction** — количество реакций за неделю;
* **week_subscription** — количество подписок за неделю;
* **week_user** — количество регистраций за неделю;
* **month_message** — количество сообщений за месяц;
* **month_reaction** — количество реакций за месяц;
* **month_subscription** — количество подписок за месяц;
* **month_user** — количество регистраций за месяц.

#### Витрина рекомендации друзей

Расположение витрины: /user/qwertyqwer/data/prod/friends_recommendations

Структура витрины:

* **user_left** — первый пользователь;
* **user_right** — второй пользователь;
* **processed_dttm** — дата расчёта витрины;
* **zone_id** — идентификатор зоны (города);
* **local_time** — локальное время.

## ETL

Порядок работы ETL скрипта:

1. Сперва необходимо найти локацию каждого события. Ежедневно запускаем скрипт events_nearest_cities.py.
   Чтобы заполнить историю, передаем в DAG параметр catchup = True.
2. Считаем витрину локации пользователей, запуская скрипт users_locations.py каждый день.
3. Считаем витрину статистики по геозонам, запуская скрипт geo_zones_stats.py каждый день.
4. Считаем витрину рекомендации друзей:
    1. Сначала найдем всех кросс-пользователей по городам из последних сообщений. Запускаем скрипт
       cross_users_by_last_message.py и записываем результат в таблицу кросс-пользователей песочницы.
    2. Затем посчитаем расстояние по таблице кросс-пользователей. Отфильтруем пары пользователей с расстоянием больше 1
       км. Запустив nearest_users_from_cross_users.py Запишем результат в таблицу ближайших пользователей в песочнице.
    3. Прочитаем таблицу ближайших пользователей. Найдем множество уникальных подписок пользователей. Присоединим
       множества подписок к левому и правому пользователю. Отфильтруем пары пользователей с пересекающимися
       множествами подписок. Запишем результат в витрину рекомендации друзей. За всё это отвечает скрипт
       friends_recommendation.py
