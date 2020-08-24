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

    def __init__(self):
        pass

    def read_Data(self):
        pass

    def write_data(self):
        pass

    def list_files(self):
        pass

    def delete_file(self):
        pass

def main():
    pass


if __name__ == "__main__":
    main()