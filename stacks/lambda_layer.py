from aws_cdk import aws_lambda as _lambda
from constructs import Construct


def create_dependencies_layer(scope: Construct, id: str) -> _lambda.LayerVersion:
    return _lambda.LayerVersion(
        scope,
        id,
        code=_lambda.Code.from_asset("layer"),
        compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
        description="Dependencies for Finance Data Platform",
    )
