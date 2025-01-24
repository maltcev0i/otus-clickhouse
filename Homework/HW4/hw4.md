## **Выполнение 1 варианта**
### Агрегатные функции
#### **Запрос**

SELECT
    SUM(quantity * price) AS total_revenue,
    AVG(quantity * price) AS average_revenue_per_transaction,
    SUM(quantity) AS total_products_sold
FROM transactions

#### **Результат вывода запроса**

| total_revenue     | average_revenue_per_transaction | total_products_sold |
|--------------------|---------------------------------|----------------------|
| 380812.1902208328 | 1447.9550958967027             | 1489                |
### Функции для работы с типами данных
#### **Запрос**
SELECT
    toString(transaction_date) AS transaction_date_str,
    toYear(transaction_date) AS year,
    toMonth(transaction_date) AS month,
    round(price) AS rounded_price,
    toString(transaction_id) AS transaction_id_str
FROM transactions
LIMIT 10
#### **Результат вывода запроса**
| transaction_date_str | year | month | rounded_price | transaction_id_str |
|-----------------------|------|-------|---------------|--------------------|
| 2023-11-11           | 2023 |    11 |           177 | 1                  |
| 2023-10-30           | 2023 |    10 |           217 | 2                  |
| 2022-06-05           | 2022 |     6 |           188 | 3                  |
| 2023-05-23           | 2023 |     5 |           255 | 4                  |
| 2022-02-07           | 2022 |     2 |           174 | 5                  |
| 2022-05-10           | 2022 |     5 |           426 | 6                  |
| 2023-05-30           | 2023 |     5 |           412 | 7                  |
| 2023-08-28           | 2023 |     8 |            57 | 8                  |
| 2022-12-06           | 2022 |    12 |           481 | 9                  |
| 2022-04-13           | 2022 |     4 |           320 | 10                 |

### User-Defined Functions (UDFs)

#### **Запрос**
CREATE FUNCTION calculate_total_price AS (quantity, price) -> (quantity * price)

SELECT
    name,
    create_query
FROM system.functions
WHERE origin = 'SQLUserDefined'
#### **Результат вывода запроса**
| name                  | create_query                                                                      |
|-----------------------|-----------------------------------------------------------------------------------|
| calculate_total_price | CREATE FUNCTION calculate_total_price AS (quantity, price) -> (quantity * price) |


#### **Запрос**
SELECT
    transaction_id,
    calculate_total_price(quantity, price) AS total_price
FROM transactions
LIMIT 10
#### **Результат вывода запроса**

| transaction_id | total_price         |
|----------------|---------------------|
| 1              | 707.47998046875     |
| 2              | 868.2000122070312   |
| 3              | 188.42999267578125  |
| 4              | 1021.7999877929688  |
| 5              | 1217.3700256347656  |
| 6              | 2553.2400512695312  |
| 7              | 2060.2499389648438  |
| 8              | 114.4800033569336   |
| 9              | 2402.949981689453   |
| 10             | 1917.659912109375   |


#### **Запрос**
 CREATE FUNCTION classify_transaction AS (total_price, threshold) ->
    IF(total_price > threshold, 'high-value', 'low-value')

SELECT
    name,
    create_query
FROM system.functions
WHERE origin = 'SQLUserDefined'

#### **Результат вывода запроса**

| name                  | create_query                                                                                                             |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------|
| classify_transaction  | CREATE FUNCTION classify_transaction AS (total_price, threshold) -> if(total_price > threshold, 'high-value', 'low-value') |
| calculate_total_price | CREATE FUNCTION calculate_total_price AS (quantity, price) -> (quantity * price)                                        |

#### **Запрос**
SELECT
    transaction_id,
    total_price,
    classify_transaction(total_price, 1000) AS transaction_category
FROM
(
    SELECT
        transaction_id,
        calculate_total_price(quantity, price) AS total_price
    FROM transactions
)
LIMIT 10
#### **Результат вывода запроса**
| transaction_id | total_price         | transaction_category |
|----------------|---------------------|-----------------------|
| 1              | 707.47998046875     | low-value            |
| 2              | 868.2000122070312   | low-value            |
| 3              | 188.42999267578125  | low-value            |
| 4              | 1021.7999877929688  | high-value           |
| 5              | 1217.3700256347656  | high-value           |
| 6              | 2553.2400512695312  | high-value           |
| 7              | 2060.2499389648438  | high-value           |
| 8              | 114.4800033569336   | low-value            |
| 9              | 2402.949981689453   | high-value           |
| 10             | 1917.659912109375   | high-value           |
