__Author__ = "Sai Prashanth Thalanayar Swainathan"
__Email__ = "saiprashanthts@gmail.com"
__Version__ = "0.0.1"
__Doc__ = "Purpose of this module is to load and Read data into S3 using Boto3"

"""
    Importing the Necessary Packages
"""
try:
    import pandas as pd
    import boto3
    from io import StringIO
    import argparse
except Exception as e:
    print("SOme packages are missing. Please install them and then continue. Error Message is {}".format(e))


class S3Load:

    def __init__(self, bucket_name='saitestts'):
        self.bucket_name = bucket_name

    def read_Data(self, file_name = 'saitestts'):
        pass

    def write_data(self, src_file_name = '', target_file_name = ''):
        pass

    def list_files(self):
        pass

    def delete_file(self, file_name = 'OnlineRetail.csv'):
        pass


def main(bucket_name, choice):
    load_obj = S3Load(bucket_name=bucket_name)
    if choice == "LIST":
        load_obj.list_files()
    elif choice == "DELETE":
        load_obj.delete_file()
    elif choice == "READ":
        load_obj.read_Data()
    elif choice == "WRITE":
        load_obj.write_data()
    else:
        print("Invalid Choice")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="To Load and Read Data present in S3",
                                     prog="Reading_Writing_Data_into_S3.py")
    parser.add_argument("--choice","-choice",required=True, choices= ['DELETE', 'READ', 'WRITE', 'LIST'],
                        default='READ',dest="choice"
                        )
    parser.add_argument("--bucket_name", "-bucket_name", required= False, default= "saitestts",
                        dest="bucket")
    arg = parser.parse_args()
    choice = arg.choice
    choice = choice.upper()
    bucket_name = arg.bucket
    main(bucket_name, choice)