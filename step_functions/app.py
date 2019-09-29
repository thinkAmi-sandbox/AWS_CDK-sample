#!/usr/bin/env python3

from aws_cdk import core

from step_functions.step_functions_stack import StepFunctionsStack


app = core.App()

# CFnのStack名を第2引数で渡す
StepFunctionsStack(app, 'step-functions')

app.synth()
