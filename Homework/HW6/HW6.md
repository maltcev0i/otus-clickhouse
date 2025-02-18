# **Задание**

## Создаем БД и таблицы
```
clickhouse1.nokvag.ru :) CREATE DATABASE imdb;

CREATE DATABASE imdb

Query id: f12c0e6b-3b7a-4d1b-986f-744c6517c7a4

Ok.

0 rows in set. Elapsed: 0.004 sec.

clickhouse1.nokvag.ru :) CREATE TABLE imdb.actors
(
    id         UInt32,
    first_name String,
    last_name  String,
    gender     FixedString(1)
) ENGINE = MergeTree ORDER BY (id, first_name, last_name, gender);


CREATE TABLE imdb.actors
(
    `id` UInt32,
    `first_name` String,
    `last_name` String,
    `gender` FixedString(1)
)
ENGINE = MergeTree
ORDER BY (id, first_name, last_name, gender)

Query id: cd2b9ffa-eec3-4098-b419-96e89f99b506

Ok.

0 rows in set. Elapsed: 0.005 sec.

clickhouse1.nokvag.ru :) CREATE TABLE imdb.genres
(
    movie_id UInt32,
    genre    String
) ENGINE = MergeTree ORDER BY (movie_id, genre);


CREATE TABLE imdb.genres
(
    `movie_id` UInt32,
    `genre` String
)
ENGINE = MergeTree
ORDER BY (movie_id, genre)

Query id: bbb4ba75-1689-44b2-ac8e-ce1da19b3503

Ok.

0 rows in set. Elapsed: 0.007 sec.

clickhouse1.nokvag.ru :) CREATE TABLE imdb.movies
(
    id   UInt32,
    name String,
    year UInt32,
    rank Float32 DEFAULT 0
) ENGINE = MergeTree ORDER BY (id, name, year);


CREATE TABLE imdb.movies
(
    `id` UInt32,
    `name` String,
    `year` UInt32,
    `rank` Float32 DEFAULT 0
)
ENGINE = MergeTree
ORDER BY (id, name, year)

Query id: c707e010-456a-40c0-a685-238c43461f1a

Ok.

0 rows in set. Elapsed: 0.005 sec.

clickhouse1.nokvag.ru :) CREATE TABLE imdb.roles
(
    actor_id   UInt32,
    movie_id   UInt32,
    role       String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY (actor_id, movie_id);


CREATE TABLE imdb.roles
(
    `actor_id` UInt32,
    `movie_id` UInt32,
    `role` String,
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (actor_id, movie_id)

Query id: a01ad52f-22ec-458d-a4d5-9ab314a46168

Ok.

0 rows in set. Elapsed: 0.007 sec.

clickhouse1.nokvag.ru :)
```

## Вставляем тестовые данные 

```
clickhouse1.nokvag.ru :) INSERT INTO imdb.actors
SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/imdb/imdb_ijs_actors.tsv.gz',
'TSVWithNames');


INSERT INTO imdb.actors SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/imdb/imdb_ijs_actors.tsv.gz', 'TSVWithNames')

Query id: d10985ea-115f-491b-9d4e-eb8996d1b4af

Ok.

0 rows in set. Elapsed: 3.008 sec. Processed 817.72 thousand rows, 23.39 MB (271.86 thousand rows/s., 7.78 MB/s.)
Peak memory usage: 56.94 MiB.

clickhouse1.nokvag.ru :) INSERT INTO imdb.genres
SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/imdb/imdb_ijs_movies_genres.tsv.gz',
'TSVWithNames');


INSERT INTO imdb.genres SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/imdb/imdb_ijs_movies_genres.tsv.gz', 'TSVWithNames')

Query id: f10716c0-e8bc-4067-9bb1-5fbccff06081

Ok.

0 rows in set. Elapsed: 1.004 sec. Processed 395.12 thousand rows, 1.16 MB (393.37 thousand rows/s., 1.16 MB/s.)
Peak memory usage: 17.79 MiB.

clickhouse1.nokvag.ru :) INSERT INTO imdb.movies
SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/imdb/imdb_ijs_movies.tsv.gz',
'TSVWithNames');


INSERT INTO imdb.movies SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/imdb/imdb_ijs_movies.tsv.gz', 'TSVWithNames')

Query id: 472fa0fa-4f66-463e-b492-d96b27b0bddb

Ok.

0 rows in set. Elapsed: 2.853 sec. Processed 388.27 thousand rows, 4.84 MB (136.11 thousand rows/s., 1.70 MB/s.)
Peak memory usage: 30.19 MiB.

clickhouse1.nokvag.ru :) INSERT INTO imdb.roles(actor_id, movie_id, role)
SELECT actor_id, movie_id, role
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/imdb/imdb_ijs_roles.tsv.gz',
'TSVWithNames');


INSERT INTO imdb.roles (actor_id, movie_id, role) SELECT
    actor_id,
    movie_id,
    role
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/imdb/imdb_ijs_roles.tsv.gz', 'TSVWithNames')

Query id: dd5289c0-e7ce-493a-8944-7875ede60efb

Ok.

0 rows in set. Elapsed: 7.214 sec. Processed 3.43 million rows, 83.58 MB (475.77 thousand rows/s., 11.59 MB/s.)
Peak memory usage: 129.00 MiB.
```

## Построение запросов


### Найти жанры для каждого фильма
Перечисляем жанры в массиве для каждого фильма
```
SELECT
    movies.name,
    groupArray(genres.genre) AS genres
FROM movies
LEFT JOIN genres ON movies.id = genres.movie_id
GROUP BY name
LIMIT 10

Query id: a17f3def-be92-4095-9b77-901654c3d884

    ┌─name─────┬─genres─────────────┐
 1. │ Merlusse │ ['Comedy','']      │
 2. │ Grammos  │ ['']               │
 3. │ L + R    │ ['']               │
 4. │ Istll    │ ['']               │
 5. │ Agency   │ ['Drama']          │
 6. │ Kakka    │ ['']               │
 7. │ Eclosion │ ['Short']          │
 8. │ 421      │ ['Short']          │
 9. │ Macr     │ ['']               │
10. │ Turksib  │ ['Documentary',''] │
    └──────────┴────────────────────┘

10 rows in set. Elapsed: 0.072 sec. Processed 783.39 thousand rows, 19.95 MB (10.88 million rows/s., 277.12 MB/s.)
Peak memory usage: 147.58 MiB.

```

### Запросить все фильмы, у которых нет жанра
Джойним по id и фильтруем по пустому полю genre
```
clickhouse1.nokvag.ru :) SELECT m.name, g.genre
FROM movies m
LEFT JOIN genres g ON g.movie_id = m.id
WHERE g.genre = ''
LIMIT 10;

SELECT
    m.name,
    g.genre
FROM movies AS m
LEFT JOIN genres AS g ON g.movie_id = m.id
WHERE g.genre = ''
LIMIT 10

Query id: 0877610a-8512-4609-852a-e191756ffde8

    ┌─name───────────────────────────┬─genre─┐
 1. │ Vaiki Vanna Vasantham          │       │
 2. │ Vain laulajapoikia             │       │
 3. │ Vainajan vaivat                │       │
 4. │ Vaincre  Olympie               │       │
 5. │ Vaines recherches              │       │
 6. │ Vainqueur de la course pdestre │       │
 7. │ Vainqueur, Le                  │       │
 8. │ Vairam                         │       │
 9. │ Vaisakha Rathiri               │       │
10. │ Vaishakh Vanwa                 │       │
    └────────────────────────────────┴───────┘

10 rows in set. Elapsed: 0.050 sec. Processed 640.88 thousand rows, 15.39 MB (12.88 million rows/s., 309.29 MB/s.)
Peak memory usage: 75.12 MiB.
```
### Объединить каждую строку из таблицы “Фильмы” с каждой строкой из таблицы “Жанры”

```
clickhouse1.nokvag.ru :) SELECT count()
FROM movies m
CROSS JOIN genres g
LIMIT 10

SELECT count()
FROM movies AS m
CROSS JOIN genres AS g
LIMIT 10

Query id: f881fe43-ce3e-46c4-a7f5-286749165de8

   ┌──────count()─┐
1. │ 153412459011 │ -- 153.41 billion
   └──────────────┘

1 row in set. Elapsed: 26.362 sec. Processed 783.39 thousand rows, 3.13 MB (29.72 thousand rows/s., 118.87 KB/s.)
Peak memory usage: 3.78 MiB.

```
### Найти жанры для каждого фильма, НЕ используя INNER JOIN

```
clickhouse1.nokvag.ru :) SELECT m.name,m.id,g.movie_id, groupArray(g.genre) AS genres
FROM movies m, genres g
WHERE g.movie_id = m.id
GROUP BY m.name,m.id,g.movie_id
LIMIT 10

SELECT
    m.name,
    m.id,
    g.movie_id,
    groupArray(g.genre) AS genres
FROM movies AS m, genres AS g
WHERE g.movie_id = m.id
GROUP BY
    m.name,
    m.id,
    g.movie_id
LIMIT 10

Query id: 49905097-126e-4866-ae6a-ab0e930768f6

    ┌─name─────────────────────┬─────id─┬─movie_id─┬─genres────────────────────────────────┐
 1. │ Amolador, O              │  14591 │    14591 │ ['Documentary','Short']               │
 2. │ Sethurama Iyer CBI       │ 295122 │   295122 │ ['Mystery','Thriller']                │
 3. │ Tao of Steve, The        │ 324729 │   324729 │ ['Comedy','Romance']                  │
 4. │ Noctivagant              │ 234463 │   234463 │ ['Action','Drama','Fantasy','Sci-Fi'] │
 5. │ Hou chuang               │ 150715 │   150715 │ ['Drama']                             │
 6. │ Engao mortal             │ 101321 │   101321 │ ['Action']                            │
 7. │ Fausses confidences, Les │ 110057 │   110057 │ ['Drama']                             │
 8. │ Suicide Battalion        │ 318441 │   318441 │ ['War']                               │
 9. │ Abgedreht                │   4828 │     4828 │ ['Documentary']                       │
10. │ Broken Spell, The        │  47561 │    47561 │ ['Comedy','Short']                    │
    └──────────────────────────┴────────┴──────────┴───────────────────────────────────────┘

10 rows in set. Elapsed: 0.076 sec. Processed 783.39 thousand rows, 19.95 MB (10.27 million rows/s., 261.58 MB/s.)
Peak memory usage: 150.26 MiB.

clickhouse1.nokvag.ru :)
```
### Найти всех актеров и актрис, снявшихся в фильме в 2023 году


```
SELECT *
FROM actors AS a
INNER JOIN (SELECT DISTINCT actor_id AS id FROM roles
    WHERE movie_id IN (SELECT id
        FROM movies
        WHERE year = 2023)
) AS t USING (id)
LIMIT 10

Query id: 83bda8ac-38cc-43e7-b096-816f352fb9bc

Ok.

0 rows in set. Elapsed: 0.008 sec. Processed 584.88 thousand rows, 8.65 MB (77.09 million rows/s., 1.14 GB/s.)
Peak memory usage: 4.30 MiB.
```
### апросить все фильмы, у которых нет жанра, через ANTI JOIN

```
SELECT
    m.name,
    groupArray(g.genre) AS genres
FROM movies AS m
ANTI LEFT JOIN genres AS g ON g.movie_id = m.id
GROUP BY m.name
LIMIT 10

Query id: 56d3dcdb-4a64-4752-8954-429ddd4c15ea

    ┌─name─────┬─genres─┐
 1. │ Merlusse │ ['']   │
 2. │ Grammos  │ ['']   │
 3. │ L + R    │ ['']   │
 4. │ Istll    │ ['']   │
 5. │ Kakka    │ ['']   │
 6. │ Macr     │ ['']   │
 7. │ Turksib  │ ['']   │
 8. │ Zonk!    │ ['']   │
 9. │ Tramonto │ ['']   │
10. │ 813      │ ['']   │
    └──────────┴────────┘

10 rows in set. Elapsed: 0.043 sec. Processed 783.39 thousand rows, 19.95 MB (18.40 million rows/s., 468.59 MB/s.)
Peak memory usage: 83.06 MiB.

```
