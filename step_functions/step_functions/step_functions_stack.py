import pathlib

from aws_cdk import core
from aws_cdk.aws_iam import PolicyStatement, Effect, ManagedPolicy, ServicePrincipal, Role
from aws_cdk.aws_lambda import AssetCode, LayerVersion, Function, Runtime
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_stepfunctions import Task, StateMachine, Parallel
from aws_cdk.aws_stepfunctions_tasks import InvokeFunction, StartExecution

from settings import AWS_SCIPY_ARN


class StepFunctionsStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.lambda_path_base = pathlib.Path(__file__).parents[0].joinpath('lambda_function')

        self.bucket = self.create_s3_bucket()
        self.managed_policy = self.create_managed_policy()
        self.role = self.create_role()
        self.first_lambda = self.create_first_lambda()
        self.second_lambda = self.create_other_lambda('second')
        self.third_lambda = self.create_other_lambda('third')
        self.error_lambda = self.create_other_lambda('error')

        self.sub_state_machine = self.create_sub_state_machine()
        self.main_state_machine = self.create_main_state_machine()

    def create_s3_bucket(self):
        return Bucket(
            self,
            'S3 Bucket',
            bucket_name=f'sfn-bucket-by-aws-cdk',
        )

    def create_managed_policy(self):
        statement = PolicyStatement(
            effect=Effect.ALLOW,
            actions=[
                "s3:PutObject",
            ],
            resources=[
                f'{self.bucket.bucket_arn}/*',
            ]
        )

        return ManagedPolicy(
            self,
            'Managed Policy',
            managed_policy_name='sfn_lambda_policy',
            statements=[statement],
        )

    def create_role(self):
        service_principal = ServicePrincipal('lambda.amazonaws.com')

        return Role(
            self,
            'Role',
            assumed_by=service_principal,
            role_name='sfn_lambda_role',
            managed_policies=[self.managed_policy],
        )

    def create_first_lambda(self):
        function_path = str(self.lambda_path_base.joinpath('first'))
        code = AssetCode(function_path)

        scipy_layer = LayerVersion.from_layer_version_arn(
            self, f'sfn_scipy_layer_for_first', AWS_SCIPY_ARN)

        return Function(
            self,
            f'id_first',
            # Lambda本体のソースコードがあるディレクトリを指定
            code=code,
            # Lambda本体のハンドラ名を指定
            handler='lambda_function.lambda_handler',
            # ランタイムの指定
            runtime=Runtime.PYTHON_3_7,
            # 環境変数の設定
            environment={'BUCKET_NAME': self.bucket.bucket_name},
            function_name='sfn_first_lambda',
            layers=[scipy_layer],
            memory_size=128,
            role=self.role,
            timeout=core.Duration.seconds(10),
        )

    def create_other_lambda(self, function_name):
        function_path = str(self.lambda_path_base.joinpath(function_name))

        return Function(
            self,
            f'id_{function_name}',
            code=AssetCode(function_path),
            handler='lambda_function.lambda_handler',
            runtime=Runtime.PYTHON_3_7,
            function_name=f'sfn_{function_name}_lambda',
            memory_size=128,
            timeout=core.Duration.seconds(10),
        )

    def create_sub_state_machine(self):
        error_task = Task(
            self,
            'Error Task',
            task=InvokeFunction(self.error_lambda),
        )

        # 2つめのTask
        second_task = Task(
            self,
            'Second Task',
            task=InvokeFunction(self.second_lambda),

            # 渡されてきた項目を絞ってLambdaに渡す
            input_path="$['first_result', 'parallel_no', 'message', 'context_name', 'const_value']",

            # 結果は second_result という項目に入れる
            result_path='$.second_result',

            # 次のタスクに渡す項目は絞る
            output_path="$['second_result', 'parallel_no']"
        )
        # エラーハンドリングを追加
        second_task.add_catch(error_task, errors=['States.ALL'])

        # 3つめのTask
        third_task = Task(
            self,
            'Third Task',
            task=InvokeFunction(self.third_lambda),

            # third_lambdaの結果だけに差し替え
            result_path='$',
        )
        # こちらもエラーハンドリングを追加
        third_task.add_catch(error_task, errors=['States.ALL'])

        # 2つ目のTaskの次に3つ目のTaskを起動するように定義
        definition = second_task.next(third_task)

        return StateMachine(
            self,
            'Sub StateMachine',
            definition=definition,
            state_machine_name='sfn_sub_state_machine',
        )

    def create_main_state_machine(self):
        first_task = Task(
            self,
            'S3 Lambda Task',
            task=InvokeFunction(self.first_lambda, payload={'message': 'Hello world'}),
            comment='Main StateMachine',
        )

        parallel_task = self.create_parallel_task()

        # 1番目のTaskの次に、パラレルなTask(StateMachine)をセット
        definition = first_task.next(parallel_task)

        return StateMachine(
            self,
            'Main StateMachine',
            definition=definition,
            state_machine_name='sfn_main_state_machine',
        )

    def create_parallel_task(self):
        parallel_task = Parallel(
            self,
            'Parallel Task',
        )

        for i in range(1, 4):
            sub_task = StartExecution(
                self.sub_state_machine,
                input={
                    'parallel_no': i,
                    'first_result.$': '$',

                    # first_taskのレスポンスにある、messageをセット
                    'message.$': '$.message',

                    # コンテキストオブジェクトの名前をセット
                    'context_name.$': '$$.State.Name',
                    # 固定値を2つ追加(ただ、Taskのinputでignore_valueは無視)
                    'const_value': 'ham',
                    'ignore_value': 'ignore',
                },
            )

            invoke_sub_task = Task(
                self,
                f'Sub Task {i}',
                task=sub_task,
            )
            parallel_task.branch(invoke_sub_task)
        return parallel_task
