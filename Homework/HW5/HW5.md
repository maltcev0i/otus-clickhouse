# **Задание**
## Задание 1
По полям sign и version вспоминаем про движок VersionedCollapsingMergeTree из вебинара
```
CREATE TABLE tbl1
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID;

INSERT INTO tbl1 VALUES (4324182021466249494, 5, 146, -1, 1);
INSERT INTO tbl1 VALUES (4324182021466249494, 5, 146, 1, 1),(4324182021466249494, 6, 185, 1, 2);

SELECT * FROM tbl1;
   ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
1. │ 4324182021466249494 │         5 │      146 │   -1 │       1 │
   └─────────────────────┴───────────┴──────────┴──────┴─────────┘
   ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
2. │ 4324182021466249494 │         5 │      146 │    1 │       1 │
3. │ 4324182021466249494 │         6 │      185 │    1 │       2 │
   └─────────────────────┴───────────┴──────────┴──────┴─────────┘
SELECT * FROM tbl1 FINAL;

   ┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
1. │ 4324182021466249494 │         6 │      185 │    1 │       2 │
   └─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

## Задание 2

Первые ключи сложились по значениям - summing движок
```
CREATE TABLE tbl2
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree
ORDER BY key;

INSERT INTO tbl2 Values(1,1),(1,2),(2,1);

select * from tbl2;

   ┌─key─┬─value─┐
1. │   1 │     3 │
2. │   2 │     1 │
   └─────┴───────┘

```

## Задание 3


```
CREATE TABLE tbl3
(
    `id` Int32,
    `status` String,
    `price` String,
    `comment` String
)
ENGINE = <ENGINE>
PRIMARY KEY (id)
ORDER BY (id, status);

INSERT INTO tbl3 VALUES (23, 'success', '1000', 'Confirmed');
INSERT INTO tbl3 VALUES (23, 'success', '2000', 'Cancelled'); 

SELECT * from tbl3 WHERE id=23;
   ┌─id─┬─status──┬─price─┬─comment───┐
1. │ 23 │ success │ 1000  │ Confirmed │
   └────┴─────────┴───────┴───────────┘
   ┌─id─┬─status──┬─price─┬─comment───┐
2. │ 23 │ success │ 2000  │ Cancelled │
   └────┴─────────┴───────┴───────────┘

SELECT * from tbl3 FINAL WHERE id=23;

   ┌─id─┬─status──┬─price─┬─comment───┐
1. │ 23 │ success │ 2000  │ Cancelled │
   └────┴─────────┴───────┴───────────┘

```
## Задание 4

```
CREATE TABLE tbl4
(   CounterID UInt8,
    StartDate Date,
    UserID UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(StartDate) 
ORDER BY (CounterID, StartDate);

INSERT INTO tbl4 VALUES(0, '2019-11-11', 1);
INSERT INTO tbl4 VALUES(1, '2019-11-12', 1);

CREATE TABLE tbl5
(   CounterID UInt8,
    StartDate Date,
    UserID AggregateFunction(uniq, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(StartDate)
ORDER BY (CounterID, StartDate);

INSERT INTO tbl5
select CounterID, StartDate, uniqState(UserID)
from tbl4
group by CounterID, StartDate;

Ok.
Error on processing query: Code: 53. DB::Exception: Cannot convert UInt64 to AggregateFunction(uniq, UInt64): While executing ValuesBlockInputFormat: data for INSERT was parsed from query. (TYPE_MISMATCH) (version 24.9.2.42 (official build))

SELECT uniqMerge(UserID) AS state 
FROM tbl5 
GROUP BY CounterID, StartDate;


   ┌─state─┐
1. │     1 │
2. │     1 │
   └───────┘

```
По наименованию поля догадываемся об использовании агрегативного движка

## Задание 5
Sign поле подсказывает, что это collaps
```
CREATE TABLE tbl6
(
    `id` Int32,
    `status` String,
    `price` String,
    `comment` String,
    `sign` Int8
)
ENGINE = CollapsingMergeTree(sign)
PRIMARY KEY (id)
ORDER BY (id, status);


INSERT INTO tbl6 VALUES (23, 'success', '1000', 'Confirmed', 1);
INSERT INTO tbl6 VALUES (23, 'success', '1000', 'Confirmed', -1), (23, 'success', '2000', 'Cancelled', 1);

SELECT * FROM tbl6;
   ┌─id─┬─status──┬─price─┬─comment───┬─sign─┐
1. │ 23 │ success │ 1000  │ Confirmed │    1 │
   └────┴─────────┴───────┴───────────┴──────┘
   ┌─id─┬─status──┬─price─┬─comment───┬─sign─┐
2. │ 23 │ success │ 1000  │ Confirmed │   -1 │
3. │ 23 │ success │ 2000  │ Cancelled │    1 │
   └────┴─────────┴───────┴───────────┴──────┘

SELECT * FROM tbl6 FINAL;
   ┌─id─┬─status──┬─price─┬─comment───┬─sign─┐
1. │ 23 │ success │ 2000  │ Cancelled │    1 │
   └────┴─────────┴───────┴───────────┴──────┘

```