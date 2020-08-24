__Author__ = "Sai Prashanth Thalanayar Swaminathan"
__Email__ = "saiprashanthts@gmail.com"
__Version__ = "0.0.1"
__Doc__ = "Purpose of this module is to save cloud watch logs as a file based on the format user passes using Boto3"


"""
    Importing the Necessary Packages
"""
try:
    import boto3
    import argparse
    import pandas as pd
    import datetime
except Exception as e:
    print("Some packages are missing. Please install them and then continue. Error Message is {}".format(e))


def timeit(method1):
    """
    Decorator to find out how much time it took to run the complete program
    :param method1:
    :return:
    """
    def time_method(*args, **kwargs):
        start_time = datetime.datetime.now()
        result = method1(*args, **kwargs)
        end_time = datetime.datetime.now()
        print('Total Time taken is {}'.format(end_time - start_time))
        return result
    return time_method


class CloudWatchLog(object):

    def __init__(self, log_group_name, log_stream_name):
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name
        self.client = boto3.client('logs')

    def get_logs(self):
        """
        Get the log of clud watch based on log group name and log stream name
        :return:
        """
        message_list = []
        response = self.client.get_log_events(
            logGroupName=self.log_group_name,
            logStreamName=self.log_stream_name
        )
        for i in response['events']:
            message_list.append([i['message'],i['timestamp']])
        df = pd.DataFrame(data=message_list, columns=['Messages', 'TimeStamp'])
        return df

    def save_csv(self):
        """
        Write the Dataframe as csv based on USER choice
        :return:
        """
        df = self.get_logs()
        df.to_csv('Cloud_Watchlogs.csv',index=False, header=True)
        return True

    def save_json(self):
        """
        Write the Dataframe as json based on USER choice
        :return:
        """
        df = self.get_logs()
        df.to_json("Cloud_Watch_logs.json",index=False, orient='split')
        return True

    def save_excel(self):
        """
        Write the Dataframe as excel based on USER choice
        :return:
        """
        df = self.get_logs()
        df.to_excel("Cloud_Watch.xls",sheet_name="logs",index=False)
        return True


@timeit
def main(choice, log_group_name, log_stream_name):
    handler = CloudWatchLog(log_group_name, log_stream_name)
    if choice == 'JSON':
        handler.save_json()
    elif choice == 'CSV':
        handler.save_csv()
    else:
        handler.save_excel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="To save clod watch logs",
                                    prog="savecloudwatchlogs.py")
    parser.add_argument("--choice", "-choice", dest="choice", choices=[ "CSV", "JSON", "EXCEL"], required=True)
    parser.add_argument("--log_group_name", "-log_group_name", dest="log_group_name", required=False, default='/aws/lambda/trigger_ec2')
    parser.add_argument("--log_stream_name", "-log_stream_name", dest="log_stream_name", required=False, default='2020/08/23/[$LATEST]44c09c2b41db4575ae00698040b1506f')
    arg = parser.parse_args()
    choice = arg.choice
    log_group_name = arg.log_group_name
    log_stream_name = arg.log_stream_name
    main(choice, log_group_name, log_stream_name)

