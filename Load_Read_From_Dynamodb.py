__Author__ = "Sai Prashanth Thalanayar Swainathan"
__Email__ = "saiprashanthts@gmail.com"
__Version__ = "0.0.1"
__Doc__ = "Purpose of this module is to load data into Dynamo DB Table using Boto3"



try:
    '''
    Importing the Necessary Packages
    '''
    import boto3
    import argparse
    from boto3.dynamodb.conditions import Key,Attr
    import pandas as pd

except Exception as e:
    print("Some of the packages are missing. Please Install anad continue with the process. The error is {}".format(e))


class Load_Data_DynamoDB(object):

    def __init__(self, table, filename = 'Values.txt'):
        """
        :param table : Table Name of Dynamo DB
        """
        self.table = table
        self.resource = boto3.resource('dynamodb')
        self.file_name = filename
        self.client = boto3.client('dynamodb')
        self.table = self.resource.Table(self.table)

    def read_file(self):
        '''
        Reads the file which we wish to load
        :return: df
        '''
        df = pd.read_csv(self.file_name)
        return df

    def load_data(self):
        """
        Purpose to load the data into Dynamo DB Table
        :return: Boolean
        """
        df= self.read_file()
        for row,col in df.iterrows():
            Employeeid = int(col['Empolyeeid'])
            Employee_Name = col['Employee_Name']
            Age = col['Age']
            Salary = col['Salary']
            self.table.put_item(
                Item={
                    "Employeeid":Employeeid,
                    "Employee_Name": Employee_Name,
                    "Age": Age,
                    "Salary": Salary
                }
            )
        return True

    def read_table(self, Primary_Key):
        """
        Read the table using the Primary Key
        :param Primary_Key:
        :return:
        """
        response = self.table.get_item(
            Key={
                "Employeeid": int(Primary_Key)
            }
        )
        print(response['Item'])
        df = pd.DataFrame(data=response['Item'], index = [0])
        print(df.head())
        return True

    def delete_table(self,Primary_Key):
        """
        Delete the table item using primary Key
        :param Primary_Key:
        :return:
        """
        response = self.table.delete_item(
            Key={
                "Employeeid": int(Primary_Key)
            }
        )

    def scan_table(self,expression=''):
        """
        UDF to Scan the table
        :param expression:
        :return:
        """
        response = self.table.query(KeyConditionExpression=Key("Employeeid").eq(int(expression)))
        print(response['Items'])
        df = pd.DataFrame(response['Items'], index=[0])
        print(df.head())
        return df


    def query_table(self, expression = ''):
        """
        Udf to Query the table using the expression
        :param expression:
        :return:
        """
        response = self.table.scan(FilterExpression = Attr("Employeeid").gt(int(expression)))
        df = pd.DataFrame(response['Items'])
        print(df.head(20))
        return df


def main(Option, Primary_key = ''):
    dbobj  = Load_Data_DynamoDB('Employee')
    if Option.upper() == 'READ':
        dbobj.read_table(Primary_Key=Primary_key)
    elif Option.upper() == 'DELETE':
        dbobj.delete_table(Primary_Key=Primary_key)
    elif Option.upper() == 'SCAN':
        dbobj.scan_table(Primary_Key)
    elif Option.upper() == 'QUERY':
        dbobj.query_table(Primary_Key)
    else:
        dbobj.load_data()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Please let me know the appropriate option to perform")
    parser.add_argument("--option","-option",required=True, choices=["READ", "DELETE", "SCAN", "QUERY"])
    parser.add_argument("--Primary_Key","-Primary_Key",required=False)
    arg = parser.parse_args()
    option = arg.option
    Primary_Key = int(arg.Primary_Key)
    main(option,Primary_Key)