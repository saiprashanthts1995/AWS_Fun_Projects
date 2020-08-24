__Author__ = "Sai Prashanth Thalanayar Swaminathan"
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
    import csv
except Exception as e:
    print("Some packages are missing. Please install them and then continue. Error Message is {}".format(e))


class S3Load:

    def __init__(self, bucket_name='saitestts'):
        self.bucket_name = bucket_name
        self.resources = boto3.resource('s3')
        self.client = boto3.client('s3')

    def read_data(self, file_name='Values.txt'):
        """
        Read file from S3, and then apply transformation to decode it as utf-2 and then use csv.reader module to convert
        data into list and and then subsequently convert it ito pandas dataframe using pandas DataFrame Utility
        :param file_name:
        :return:
        """
        data_list = []
        response = self.client.get_object(
            Bucket = self.bucket_name,
            Key = file_name
        )
        data = response['Body'].read().decode('utf-8')
        data = csv.reader(data.split('\r\n'))
        header = next(data)
        for row in data:
            data_list.append(row)
        df = pd.DataFrame(data=data_list, columns= header)
        return df

    def write_data(self, src_file_name='Iris.csv', target_file_name='iris.csv'):
        """
        Write the dataframe content into S3 as an file
        :param src_file_name:
        :param target_file_name:
        :return:
        """
        df = pd.read_csv(src_file_name)
        print(df.head())
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, header=True, index=False)
        response = self.client.put_object(Bucket=self.bucket_name, Body = csv_buffer.getvalue(), Key = target_file_name)
        return(response)

    def list_files(self):
        """
        UDF to list the files of the given Bucket
        :return: List of files
        """
        response = self.client.list_objects(
            Bucket = self.bucket_name
        )
        return [(i['Key']) for i in response['Contents']]


    def delete_file(self, file_name='OnlineRetail.csv'):
        """
        Delete the file based on Key
        :param file_name:
        :return:
        """
        response = self.client.delete_object(
            Bucket =self.bucket_name,
            Key = file_name
        )
        return (response)


def main(bucket_name, choice):
    load_obj = S3Load(bucket_name=bucket_name)
    if choice == "LIST":
        files = load_obj.list_files()
        print(files)
    elif choice == "DELETE":
        load_obj.delete_file()
        print("File Deleted Successfully")
    elif choice == "READ":
        df = load_obj.read_data()
        print(df.head(20))
        print("File read Successfully")
    elif choice == "WRITE":
        load_obj.write_data()
        print('Write Completed')
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