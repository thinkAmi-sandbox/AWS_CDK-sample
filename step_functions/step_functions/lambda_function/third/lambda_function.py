def lambda_handler(event, context):
    if event['parallel_no'] == 1:
        raise Exception('強制的にエラーとします')

    return 'only 3rd message.'
