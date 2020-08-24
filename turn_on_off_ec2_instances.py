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
        response = self.client.describe_instances()
        print(response)

    def turn_on_instance(self):
        pass

    def turn_off_instance(self):
        pass


def main(choice, instance_id= ''):
    handler = PythonHandler(instance_id)
    handler.list_instances()
    if choice == 'STOP':
        handler.turn_off_instance()
    else:
        handler.turn_on_instance()
    print("Purpose of Job Accomplished")

if __name__ == "__main__":
    parser =argparse.ArgumentParser(description="To stop and Start EC2 instance",
                                    prog="turn_on_off_ec2_instances.py")
    parser.add_argument("--choice", "-choice", dest="choice", choices=[ "START","STOP","LIST"])
    main(choice,)

