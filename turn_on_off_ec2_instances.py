__Author__ = "Sai Prashanth Thalanayar Swaminathan"
__Email__ = "saiprashanthts@gmail.com"
__Version__ = "0.0.1"
__Doc__ = "Purpose of this module is to turn on and off using Boto3"

"""
    Importing the Necessary Packages
"""
try:
    import boto3
    import argparse
except Exception as e:
    print("Some packages are missing. Please install them and then continue. Error Message is {}".format(e))


class PythonHandler(object):

    def __init__(self, instance_id):
        self.instance_id = instance_id
        self.client = boto3.client('ec2')
        self.resource = boto3.resource('ec2')

    def list_instances(self):
        """
        Used to list the instaces present in the given account
        :return: list_of_instances
        """
        list_of_instances = []
        response = self.client.describe_instances()
        for i in response['Reservations']:
            for j in i['Instances']:
                list_of_instances.append(j['InstanceId'])
        return list_of_instances

    def turn_on_instance(self):
        """
        Used to Turn on the instance,. Make sure we pass list as an argument to InstanceIds argument . It doesn't take
        string as an argument. Based on the abive explanation it is clear that we can pass multiple instances as an
        argument, which would turn on all the instances in an single shot
        :return:
        """
        response = self.client.stop_instances(
            InstanceIds=[self.instance_id]
        )
        return response

    def turn_off_instance(self):
        """
        Used to Turn off the instance,. Make sure we pass list as an argument to InstanceIds argument . It doesn't take
        string as an argument. Based on the abive explanation it is clear that we can pass multiple instances as an
        argument, which would turn off all the instances in an single shot
        :return:
        """
        response = self.client.stop_instances(
            InstanceIds=[self.instance_id]
        )
        return response


def main(choice, instance_id= ''):
    handler = PythonHandler(instance_id)
    handler.list_instances()
    if choice == 'STOP':
        handler.turn_off_instance()
        print('Ec2 Instance stopped Initiated.It would take approximately around 10 seconds to completed stop')
    elif choice == 'LIST':
        list_of_instances = handler.list_instances()
        print("List of Instance ID are as follows: {}".format(list_of_instances))
    else:
        handler.turn_on_instance()
        print('EC2 Instance Started Initiated Successfully')
        print('It would take approximately 10 second to up running')
    print("Purpose of Job Accomplished")

if __name__ == "__main__":
    parser =argparse.ArgumentParser(description="To stop and Start EC2 instance",
                                    prog="turn_on_off_ec2_instances.py")
    parser.add_argument("--choice", "-choice", dest="choice", choices=[ "START","STOP","LIST"], required=True)
    parser.add_argument("--instance_id", "-instance_id", dest="instance_id", required=False, default='i-017aea5d5c9aaa48b')
    arg = parser.parse_args()
    choice = arg.choice
    instance_id = arg.instance_id
    main(choice,instance_id)

