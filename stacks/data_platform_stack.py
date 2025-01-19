### stacks/data_platform_stack.py ###
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
    aws_iam as iam,
    aws_glue as glue,
    Duration,
    SecretValue,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
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

        # Create or use existing S3 bucket for Glue scripts
        bucket_name = f"{construct_id.lower()}-glue-scripts"
        try:
            # Try to use existing bucket
            self.scripts_bucket = s3.Bucket.from_bucket_name(
                self, "GlueScriptsBucket", bucket_name
            )
        except:
            # Create new bucket if it doesn't exist
            self.scripts_bucket = s3.Bucket(
                self,
                "GlueScriptsBucket",
                bucket_name=bucket_name,
                versioned=True,
                removal_policy=RemovalPolicy.RETAIN,
                auto_delete_objects=False,
            )

        dependencies_layer = create_dependencies_layer(
            scope=self, id="FinanceDependencies"
        )

        self.create_market_data_lambda(dependencies_layer)
        self.create_edgar_finance_lambda(dependencies_layer)
        self.create_data_processing_job()

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

    def create_edgar_finance_lambda(self, layer: _lambda.LayerVersion) -> None:
        """Create Lambda function for EDGAR financial data."""
        edgar_finance_lambda_fn = _lambda.Function(
            self,
            "EDGARFinanceETL",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="company_financials.edgar_finance.handler",
            code=_lambda.Code.from_asset("lambda"),
            layers=[layer],
            timeout=Duration.minutes(15),  # Longer timeout as EDGAR API can be slower
            memory_size=512,
            environment={
                "BUCKET_NAME": self.raw_bucket.bucket_name,
            },
        )

        # Add CloudWatch Logs permissions
        edgar_finance_lambda_fn.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        # Output the actual function name
        CfnOutput(
            self,
            "EDGARFinanceLambdaName",
            value=edgar_finance_lambda_fn.function_name,
            description="EDGAR Finance Lambda Function Name",
        )

        # Grant S3 permissions
        self.raw_bucket.grant_read_write(edgar_finance_lambda_fn)

        # Daily schedule (once per day is enough for financial statements)
        events.Rule(
            self,
            "EDGARFinanceSchedule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(edgar_finance_lambda_fn)],
        )

    def create_data_processing_job(self) -> None:
        """Create Glue Job for data processing."""
        # Deploy Glue script to S3
        s3deploy.BucketDeployment(
            self,
            "DeployGlueScript",
            sources=[s3deploy.Source.asset("glue_scripts")],
            destination_bucket=self.raw_bucket,
            destination_key_prefix="glue-scripts",
        )

        # Create CloudWatch Log Group for Glue job
        glue_log_group = logs.LogGroup(
            self,
            "GlueJobLogGroup",
            log_group_name=f"/aws-glue/jobs/finance-data-processing-job",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Create IAM role for Glue
        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        # Grant CloudWatch Logs permissions to Glue role
        glue_log_group.grant_write(glue_role)

        # Grant S3 permissions to Glue role
        self.raw_bucket.grant_read(glue_role)
        self.processed_bucket.grant_read_write(glue_role)

        # Create Glue Job with extra Python libraries
        glue_job = glue.CfnJob(
            self,
            "DataProcessingJob",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{self.scripts_bucket.bucket_name}/process_finance_data.py",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            # Specify custom image
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            image_uri=f"{account}.dkr.ecr.{region}.amazonaws.com/custom-glue-image:latest",
        )

        # Output the CloudWatch Log Group name
        CfnOutput(
            self,
            "GlueJobLogGroupName",
            value=glue_log_group.log_group_name,
            description="Glue Job CloudWatch Log Group",
        )

        # Output the Glue Job name
        CfnOutput(self, "GlueJobName", value=glue_job.name, description="Glue Job Name")

        # Grant Glue role access to scripts bucket
        self.scripts_bucket.grant_read(glue_role)
