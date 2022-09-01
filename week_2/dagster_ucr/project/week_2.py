from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
    required_resource_keys={"s3"},
)
def get_s3_data(context):
    output = list()
    reader =  context.resources.s3.get_data(key_name=context.op_config["s3_key"])
    for row in reader:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"high_stock": Out(dagster_type=Aggregation)},
    description="Find stock with the greatest 'high' value"
)
def process_data(stocks):
    highest_stock = sorted(stocks, key=lambda stock: stock.high, reverse=True)[0]
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op(
    ins={"high_stock": In(dagster_type=Aggregation)},
    description="Put results on redis",
    required_resource_keys={"redis"}
)
def put_redis_data(context, high_stock):
    context.resources.redis.put_data(
        name=str(high_stock.date),
        value=str(high_stock.high)
    )


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    high_stock = process_data(stocks)
    put_redis_data(high_stock)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
