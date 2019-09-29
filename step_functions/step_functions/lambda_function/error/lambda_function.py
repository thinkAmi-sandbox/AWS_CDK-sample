import json


def lambda_handler(event, context):
    # {
    #   "resource": "arn:aws:lambda:region:id:function:sfn_error_lambda",
    #   "input": {
    #     "Error": "Exception",
    #     "Cause": "{\"errorMessage\": \"\\u5076\\u6570\\u3067\\u3059\",
    #                \"errorType\": \"Exception\",
    #                \"stackTrace\": [\"  File \\\"/var/task/lambda_function.py\\\", line 5,
    #                   in lambda_handler\\n    raise Exception('\\u5076\\u6570\\u3067\\u3059')
    #               \\n\"]}"
    #   },
    #   "timeoutInSeconds": null
    # }

    return {
        # JSONをPythonオブジェクト化することで、文字化けを直す
        'error_message': json.loads(event['Cause']),
    }
