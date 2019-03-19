import boto3

TOPIC_NAME = 'MySubscriptionTopic'
TOPIC_ARN = 'TOPIC_ARN'
QUEUE_ARN = 'SQS_QUEUE_ARN'


def sns_client():
    sns = boto3.client('sns', region_name='eu-west-1')
    """ :type : pyboto3.sns """
    return sns


def create_topic():
    sns_client().create_topic(
        Name=TOPIC_NAME
    )


def get_topics():
    return sns_client().list_topics()


def get_topic_attributes(topic_arn):
    return sns_client().get_topic_attributes(
        TopicArn=topic_arn
    )


def update_topic_attributes(topic_arn):
    return sns_client().set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName='DisplayName',
        AttributeValue=TOPIC_NAME + '-UPDATED'
    )


def delete_topic(topic_arn):
    return sns_client().delete_topic(
        TopicArn=topic_arn
    )


def create_email_subscription(topic_arn, email_address):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='email',
        Endpoint=email_address
    )


def create_sms_subscription(topic_arn, phone_number):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='sms',
        Endpoint=phone_number
    )


def create_sqs_queue_subscription(topic_arn, queue_arn):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue_arn
    )


def get_topic_subscriptions(topic_arn):
    return sns_client().list_subscriptions_by_topic(
        TopicArn=topic_arn
    )


def list_all_subscriptions():
    return sns_client().list_subscriptions()


def check_if_phone_number_opted_out(phone_number):
    return sns_client().check_if_phone_number_is_opted_out(
        phoneNumber=phone_number
    )


def list_opted_out_phone_numbers():
    return sns_client().list_phone_numbers_opted_out()


def opt_out_of_email_subscription(email_address):
    subscriptions = get_topic_subscriptions(TOPIC_ARN)
    for subscription in subscriptions['Subscriptions']:
        if subscription['Protocol'] == 'email' and subscription['endpoint'] == email_address:
            print("Unsubscribing " + subscription['Endpoint'])
            subscription_arn = subscription['SubscriptionArn']
            sns_client().unsubscribe(
                SubscriptionArn=subscription_arn
            )
            print("Unsubscribed " + subscription['Endpoint'])


def opt_out_of_sms_subscription(phone_number):
    subscriptions = get_topic_subscriptions(TOPIC_ARN)
    for subscription in subscriptions['Subscriptions']:
        if subscription['Protocol'] == 'sms' and subscription['Endpoint'] == phone_number:
            print("Unsubscribing " + subscription['Endpoint'])
            subscription_arn = subscription['SubscriptionArn']
            sns_client().unsubscribe(
                SubscriptionArn=subscription_arn
            )
            print("Unsubscribed " + subscription['Endpoint'])


def opt_in_phone_number(phone_number):
    return sns_client().opt_in_phone_number(
        phoneNumber=phone_number
    )


def publish_message(topic_arn):
    return sns_client().publish(
        TopicArn=topic_arn,
        Message="Hello, you're receiving this because you've subscribed!"
    )


if __name__ == '__main__':
    print(create_topic())
    # print(get_topics())
    # print(get_topic_attributes(TOPIC_ARN))
    # update_topic_attributes(TOPIC_ARN)
    # delete_topic(TOPIC_ARN)
    # print(create_topic())
    # create_email_subscription(TOPIC_ARN, 'YOUR_EMAIL_ADDRESS')
    # create_sms_subscription(TOPIC_ARN, 'YOUR_PHONE_NUMBER')
    # create_sqs_queue_subscription(TOPIC_ARN, QUEUE_ARN)
    # print(get_topic_subscriptions(TOPIC_ARN))
    # print(list_all_subscriptions())
    # print(check_if_phone_number_opted_out('905321613889'))
    # print(list_opted_out_phone_numbers())
    # opt_out_of_email_subscription('YOUR_EMAIL_ADDRESS')
    # opt_out_of_sms_subscription('YOUR_PHONE_NUMBER')
    # opt_in_phone_number('YOUR_PHONE_NUMBER')
    # publish_message(TOPIC_ARN)
