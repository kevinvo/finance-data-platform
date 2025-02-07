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
    CfnResource,
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
        """Create Glue Job and Crawler for data processing."""
        # Deploy Glue script to S3
        s3deploy.BucketDeployment(
            self,
            "DeployGlueScript",
            sources=[s3deploy.Source.asset("glue_scripts")],
            destination_bucket=self.scripts_bucket,
            destination_key_prefix="",
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
            name="finance-data-processing-job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{self.scripts_bucket.bucket_name}/process_finance_data.py",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=120,
            max_retries=2,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            default_arguments={
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-language": "python",
                "--TempDir": f"s3://{self.raw_bucket.bucket_name}/temporary/",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{self.raw_bucket.bucket_name}/spark-logs/",
                "--continuous-log-logGroup": glue_log_group.log_group_name,
                "--enable-job-insights": "true",
                "--additional-python-modules": "requests==2.31.0,yfinance==0.2.36",
            },
        )

        # Output the CloudWatch Log Group name
        CfnOutput(
            self,
            "GlueJobLogGroupName",
            value=glue_log_group.log_group_name,
            description="Glue Job CloudWatch Log Group",
        )

        # Output the Glue Job name using ref instead of name
        CfnOutput(self, "GlueJobName", value=glue_job.ref, description="Glue Job Name")

        # Grant Glue role access to scripts bucket
        self.scripts_bucket.grant_read(glue_role)

        # Create crawler with the same role
        self.create_glue_crawler(glue_role)

    def create_glue_crawler(self, glue_role: iam.Role) -> None:
        """Create Glue Crawler for processed data."""
        # Create Glue Database
        glue_database = glue.CfnDatabase(
            self,
            "FinanceDatabase",
            catalog_id=Stack.of(self).account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="finance_data_catalog",
                description="Database for processed finance data",
            ),
        )

        # Create Glue Crawler
        finance_crawler = glue.CfnCrawler(
            self,
            "FinanceDataCrawler",
            name="finance-data-crawler",
            role=glue_role.role_arn,
            database_name=glue_database.ref,
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 */12 * * ? *)"  # Run every 12 hours
            ),
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.processed_bucket.bucket_name}/combined_finance_data"
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG", update_behavior="UPDATE_IN_DATABASE"
            ),
            configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"},"Tables":{"AddOrUpdateBehavior":"MergeNewColumns"}}}',
        )

        # Add crawler outputs
        CfnOutput(
            self,
            "GlueDatabaseName",
            value=glue_database.ref,
            description="Glue Database Name",
        )

        CfnOutput(
            self,
            "GlueCrawlerName",
            value=finance_crawler.ref,
            description="Glue Crawler Name",
        )

        # Add crawler permissions to Glue role
        glue_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetPartition",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetWorkGroup",
                    "athena:CreateWorkGroup",
                    "athena:BatchGetQueryExecution",
                ],
                resources=[
                    f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog",
                    f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:database/{glue_database.ref}",
                    f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:table/{glue_database.ref}/*",
                    f"arn:aws:athena:{Stack.of(self).region}:{Stack.of(self).account}:workgroup/*",
                ],
            )
        )

        # Create S3 bucket for Athena query results
        athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name=f"{self.stack_name.lower()}-athena-results",
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(7)  # Clean up old query results
                )
            ],
        )

        # Grant Athena access to results bucket
        athena_results_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("athena.amazonaws.com")],
                actions=[
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:PutObject",
                ],
                resources=[
                    athena_results_bucket.bucket_arn,
                    f"{athena_results_bucket.bucket_arn}/*",
                ],
            )
        )

        # Output Athena results bucket name with a different ID
        CfnOutput(
            self,
            "AthenaResultsBucketName",
            value=athena_results_bucket.bucket_name,
            description="Bucket for Athena query results",
        )

        # Create Athena workgroup using CloudFormation
        athena_workgroup = CfnResource(
            self,
            "FinanceAnalysisWorkGroup",
            type="AWS::Athena::WorkGroup",
            properties={
                "Name": "finance-analysis",
                "Description": "Workgroup for analyzing finance data",
                "State": "ENABLED",
                "WorkGroupConfiguration": {
                    "ResultConfiguration": {
                        "OutputLocation": f"s3://{athena_results_bucket.bucket_name}/query-results/"
                    },
                    "EnforceWorkGroupConfiguration": True,
                    "PublishCloudWatchMetricsEnabled": True,
                    "BytesScannedCutoffPerQuery": 1073741824,
                },
            },
        )

        # Make sure workgroup is created after the bucket
        athena_workgroup.node.add_dependency(athena_results_bucket)

        # Output the workgroup name
        CfnOutput(
            self,
            "AthenaWorkGroupName",
            value="finance-analysis",
            description="Athena Workgroup Name",
        )
