# PYTHON
import os
import boto3
import pymssql
import botocore
import pandas as pd
from math import isnan
from time import sleep
from datetime import date, datetime

# AIRFLOW
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

with DAG(

        "DAG_ETL_S3_SQLServer",
        default_args = {"retries": 1},
        description = "Generate Unemployment Insurance / Life Insurance file and Send to SQL Server",
        schedule = "0 1 1 * *",
        start_date = pendulum.datetime(2023, 8, 10, tz="America/Sao_Paulo"),
        catchup = False,
        tags = [ "Python", "AWS", "SQL", "Insurance" ],

) as dag:
    
    DW_SQL_CONN = {'DBIP':'0.0.0.0',
    'DBLOGIN' : 'login',
    'DBPASSWORD' : 'password'}


    def unemployment_insurance_file():
        """Generate unemployment insurance file and send to Amazon S3."""

        try:
            # Get the current date
            today = date.today().strftime("%d%m%Y")

            bucket_name = ""  # S3 bucket name

            s3_file = f"insurance/ins_776_{today}.csv"  # Amazon S3 file directory and name
            local_file = "ins_776.csv"  # Local file name
 
            # Amazon S3 connection
            s3 = boto3.client(
                "s3",
                aws_access_key_id="access_key_id",  
                aws_secret_access_key="access_key"  
            )

            # SQL query to get unemployment insurance data
            get_unemployment_insurances_sql = """
                SELECT [Name], [Date of Birth], [document], [Released Amount], [Installments], [Installment Amount], [Insurance]
                FROM [vw_unemployment_insurance]
            """

            # Run SQL query
            results = query_sql_df(sql=get_unemployment_insurances_sql, db="insuranceDB")

            # Create a dataframe with the results
            dataframe = pd.DataFrame([{key: value for key, value in row.items()} for row in results])

            # Drop the first two columns
            dataframe.drop(dataframe.columns[[0, 1]], axis=1, inplace=False)

            # Save the dataframe as a CSV file
            dataframe.to_csv(local_file, sep=";", encoding="latin1", index=False)

            # Send the file to Amazon S3
            s3.upload_file(local_file, bucket_name, s3_file)

        except Exception as e:
            print(e)
    
    
    def life_insurance_file():
        """Generate life insurance file and send to Amazon S3."""
        try:
            # Get the current date
            today = date.today().strftime("%d%m%Y")

            bucket_name = "insurance_bucket"  # Amazon S3 bucket name

            s3_file = f"insurance/ins_775_{today}.csv"  # Amazon S3 file directory and name
            local_file = "ins_775.csv"  # Local file name

            # Amazon S3 connection
            s3 = boto3.client(
                 "s3",
                aws_access_key_id="access_key_id",  
                aws_secret_access_key="access_key" 
            )

            # SQL query to get life insurance data
            get_life_insurances_sql = """
                SELECT [Name], [Date of Birth], [document], [Released Amount], [Installments], [Installment Amount], [Insurance]
                FROM [vw_life_insurance]
            """

            # Run SQL query

            results = query_sql_df(sql=get_life_insurances_sql, db="insuranceDB")

            # Create a dataframe with the results
            dataframe = pd.DataFrame([{key: value for key, value in row.items()} for row in results])

            # Remove first two columns
            dataframe.drop(dataframe.columns[[0, 1]], axis=1, inplace=False)

            # Save dataframe to CSV
            dataframe.to_csv(local_file, sep=";", encoding="latin1", index=False)

            # Send file to Amazon S3
            s3.upload_file(local_file, bucket_name, s3_file)
        except Exception as e:
            print(e)


    def save_files_to_database():

        list_key=['insurance']  

        bucket_name='insurance_bucket'

        path='C:\\work\\company\\base\\s3_files\\myBucket\\'

        s3 = boto3.resource('s3',
                            aws_access_key_id="access_key_id",  
                            aws_secret_access_key="access_key" 
                                )

        # Access the S3 folders, returning files from the insurance folder that have not been processed yet and are .csv
        def download_directory_from_s3(client_resource, bucket_name, remote_directory_name):

            result=[]

            bucket = client_resource.Bucket(bucket_name) 
            for obj in (bucket.objects.filter(Prefix=remote_directory_name)):
                if 'csv' in obj.key and 'processed' not in obj.key:
                    result.append(obj.key)
            return result    

        
        for item_key in list_key:
            file_list = download_directory_from_s3(s3, bucket_name, item_key)
            
            for item in file_list:
                
                if item.endswith('.csv'):

                    temp_file = path + item_key + '\\' + item.split('/')[-1]

                    try:
                        s3.Bucket(bucket_name).download_file(item, temp_file)
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == "404":
                            print("The object does not exist.")
                        else:
                            raise e 

        for item_key in list_key:

            entries = os.listdir(path+item_key)
            
            for item_file in entries:

                print(f'Reading data from file: {item_file}\n')
                
                df = pd.read_csv(path+item_key+'\\'+item_file, encoding="ISO-8859-1", sep = ';', usecols=["Name", "Date of Birth", "document", "Released Amount", "Installments", "Installment Amount", "Insurance"]) # Create dataframe with the file

                df['document'] = df['document'].apply(lambda x: str(x).zfill(11)) # Add leading zeros to document until it has 11 digits

                # Change the date of birth format to insert into the database
                birth_date_format = "%d/%m/%Y"
                df['Date of Birth'] = df['Date of Birth'].apply(lambda x: datetime.strptime(str(x), birth_date_format).date().isoformat() if x and (isinstance(x, str) or not isnan(x)) else "1900-01-01")

                # Block to get the date from the file
                file_date = item_file[-12:-4] # item_file is the file name, extracting the date from the name
                date_format = "%d%m%Y"
                file_date_datetime = datetime.strptime(file_date, date_format).date() # Transforming
                file_date_iso = file_date_datetime.isoformat()
                df['file_date'] = file_date_iso

                # Iterate over the created dataframe to insert the rows into the database
                for i, value in df.iterrows():
                    name = value[0]
                    birth_date = value[1]
                    document = value[2]
                    released_amount = value[3]
                    installments = value[4]
                    installment_amount = value[5]

                    insurance = value[6]

                    if insurance == '776':
                        insurance = 'D'
                    elif insurance == '775':
                        insurance = 'V'                     
                    
                    file_date = value[7]
                                            
                    insert = """
                    INSERT INTO [insuranceDB].[dbo].[t_historico_segurados]
                    (Name, birth_date, document, released_amount, installments, installment_amount, insurance, file_date )
                        VALUES (
                                '{}',
                                '{}',
                                '{}',
                                {},
                                {},
                                {},
                                '{}',
                                '{}'
                                )
                    """.format(name, birth_date, document, released_amount, installments, installment_amount, insurance, file_date)
                    insert_data_sql_server_DW(insert, 'insuranceDB')
                    sleep(2)
                print(f'Data inserted from file: {item_file}\n')
                         

        # Iterate over these files to remove them from the insurance folder and insert them into the processed folder
        for item_key in list_key:
            file_list = download_directory_from_s3(s3, bucket_name, item_key)
            for item in file_list:
                try:
                    copy_source = {
                        'Bucket': bucket_name,
                        'Key': item
                    }

                    s3.Bucket(bucket_name).copy(copy_source, item_key + '/processed/' + item.split('/')[-1])
                    s3.Object(bucket_name, item).delete()
                    
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise

            print(f'File moved to processed: {item}\n')
            print('--'*30)

        # Delete the files that were downloaded
        for item_key in list_key:

            entries = os.listdir(path+item_key)
            for item_file in entries:
                os.remove(path + item_key + '\\' + item_file)


    #  abstracting a function to insert data into the DW SQL server
    def insert_data_sql_server_DW(sql, db, connx):

        try:

            # establish the connection
            conn = pymssql.connect(
                server=connx['DBIP'],
                user=connx['DBLOGIN'],
                password=connx['DBPASSWORD'],
                database=db,
                autocommit=True
            )   
            # build a cursor
            cursor = conn.cursor()  
            # execute a SQL instruction
            cursor.execute(sql)   
            # confirm the transaction
            conn.commit()
            
        except Exception as e:
            print(e)
            conn.rollback() # in case of error, apply the rollback clause
            
        finally:

            # close the connection in the end  
            conn.close()


    def query_sql_df(sql,db,connx):
        try:
            conn = pymssql.connxect(server=connx['DBIP'], user=connx['DBLOGIN'], password=connx['DBPASSWORD'] , database=db)
            df = pd.read_sql_query(sql, connx)
            return df
            conn.commit()
        except Exception as e: 
            conn.rollback()
            return  None
        finally:
            conn.close() 


    start = EmptyOperator(task_id="start")

    life_insurance_file_task = PythonOperator(task_id="life_insurance_file",
                                               python_callable=life_insurance_file
                                              )
    
    unemployment_insurance_file_task = PythonOperator(task_id="unemployment_insurance_file", 
                                            python_callable=unemployment_insurance_file
                                            )
    
    save_files_to_database_task = PythonOperator(task_id="save_files_to_database",
                                            python_callable=save_files_to_database
                                            )
    
    end = EmptyOperator(task_id="end")


    start >> life_insurance_file_task >> unemployment_insurance_file_task >> save_files_to_database_task >> end
