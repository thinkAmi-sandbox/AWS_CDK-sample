def lambda_handler(event, context):
    if event['parallel_no'] % 2 == 0:
        raise Exception('偶数です')

    return {
        'message': event['message'],
        'const_value': event['const_value']
    }
