from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


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
def week_3_pipeline():
    stocks = get_s3_data()
    high_stock = process_data(stocks)
    put_redis_data(high_stock)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(
    partition_keys=[str(i) for i in range(1,11)]
)
def docker_config(partition_key: str):
    cfg = docker.copy()
    cfg["ops"]["get_s3_data"]["config"]["s3_key"] = f"prefix/stock_{partition_key}.csv"
    return cfg


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = ScheduleDefinition(
    job=local_week_3_pipeline,
    cron_schedule="*/15 * * * *",
)

docker_week_3_schedule = ScheduleDefinition(
    job=docker_week_3_pipeline,
    cron_schedule="0 * * * *",
)


@sensor(
    job=docker_week_3_pipeline,
    minimum_interval_seconds=30
)
def docker_week_3_sensor(context):
    new_s3_keys = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://host.docker.internal:4566",
    )
    if not new_s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for s3_key in new_s3_keys:
        cfg = docker.copy()
        cfg["ops"]["get_s3_data"]["config"]["s3_key"] = s3_key
        yield RunRequest(
            run_key=s3_key,
            run_config=cfg
        )

