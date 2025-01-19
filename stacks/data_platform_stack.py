### stacks/data_platform_stack.py ###
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
    aws_iam as iam,
    Duration,
    SecretValue,
    aws_s3 as s3,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct
from stacks.lambda_layer import create_dependencies_layer


class DataPlatformStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket for raw data
        self.raw_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            bucket_name=f"{construct_id.lower()}-raw-data",
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(90),
                        )
                    ]
                )
            ],
        )

        # Create S3 bucket for processed data
        self.processed_bucket = s3.Bucket(
            self,
            "ProcessedDataBucket",
            bucket_name=f"{construct_id.lower()}-processed-data",
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        dependencies_layer = create_dependencies_layer(
            scope=self, id="FinanceDependencies"
        )

        self.create_market_data_lambda(dependencies_layer)

    def create_market_data_lambda(self, layer: _lambda.LayerVersion) -> None:
        """Create Lambda function for market data."""
        yahoo_finance_lambda_fn = _lambda.Function(
            self,
            "YahooFinanceETL",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="market_data.yahoo_finance.handler",
            code=_lambda.Code.from_asset("lambda"),
            layers=[layer],
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={"BUCKET_NAME": self.raw_bucket.bucket_name},
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

        # Add CloudWatch Logs permissions
        yahoo_finance_lambda_fn.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        # Output the actual function name
        CfnOutput(
            self,
            "YahooFinanceLambdaName",
            value=yahoo_finance_lambda_fn.function_name,
            description="Yahoo Finance Lambda Function Name",
        )

        self.raw_bucket.grant_read_write(yahoo_finance_lambda_fn)

        # Daily schedule at market close
        events.Rule(
            self,
            "MarketDataSchedule",
            schedule=events.Schedule.rate(Duration.hours(1)),
            targets=[targets.LambdaFunction(yahoo_finance_lambda_fn)],
        )
