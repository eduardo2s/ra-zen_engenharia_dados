# Raízen Data Engineering Test

This is my solution for the raízen data engineering test, using Python, Docker and Airflow.

## Goals

The proposed objective is to extract data from tables like the following from `xls` file:
 
![Pivot Table](https://github.com/raizen-analytics/data-engineering-test/raw/master/images/pivot.png)

The developed pipeline is meant to extract and structure the underlying data of two of these tables:

-   Sales of oil derivative fuels by UF and product
-   Sales of diesel by UF and type

## Schema

Data should be stored in the following format:

| Column       | Type        |
| ------------ | ----------- |
| `year_month` | `date`      |
| `uf`         | `string`    |
| `product`    | `string`    |
| `unit`       | `string`    |
| `volume`     | `double`    |
| `created_at` | `timestamp` |

## How to execute it
`git clone https://github.com/eduardo2s/raizen_engenharia_dados`

`cd airflow-docker`

`docker-compose build`

`docker-compose up -d`

`access on browser http://localhost:8080`
