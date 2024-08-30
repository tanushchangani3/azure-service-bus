from azure.identity import ClientSecretCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient
from azure.servicebus.exceptions import MessagingEntityNotFoundError

import os
connstr = os.environ['SERVICE_BUS_CONNECTION_STR']
topic_name = os.environ['SERVICE_BUS_TOPIC_NAME']
namespace = os.environ['SERVICEBUS_FULLY_QUALIFIED_NAMESPACE']
subscription_name = os.environ['SUBSCRIPTION_NAME']
credential = ClientSecretCredential(os.getenv("TENANT_ID"), os.getenv("CLIENT_ID"), os.getenv("CLIENT_SECRET"))

servicebus_client = ServiceBusClient(namespace, credential)
servicebus_mgmt_client = ServiceBusAdministrationClient(namespace, credential)

def send(message):
    with ServiceBusClient.from_connection_string(connstr) as client:
        with client.get_topic_sender(topic_name) as sender:
            sender.send_messages(ServiceBusMessage(message))

def receive():
    should_retry = True
    while should_retry:
        try:
            receiver = servicebus_client.get_subscription_receiver(topic_name, subscription_name)
            for msg in receiver:
                print(str(msg))
                receiver.complete_message(msg)
        except MessagingEntityNotFoundError:
            print("Subscription {} not Found.".format(subscription_name))
            _create_subscription()

def _create_subscription():
    if not _check_subscription(False):
        print("Going to create subscription {}.".format(subscription_name))
        try:
            servicebus_mgmt_client.create_subscription(topic_name, subscription_name)
        except Exception:
            pass
        while True:
            if _check_subscription(True):
                break

def _check_subscription(log: bool):
    subscription_list = servicebus_mgmt_client.list_subscriptions(topic_name)
    for subscription_properties in subscription_list:
        if subscription_properties.name == subscription_name:
            if log:
                print("Subscription {} is created.".format(subscription_name))
            return True
    return False