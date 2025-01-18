from aws_cdk import App
from stacks.data_platform_stack import DataPlatformStack

app = App()
DataPlatformStack(app, "FinanceDataPlatform")
app.synth()
