"""TestS3BucketConnectorMethods"""
import os
import unittest

import boto3
import pandas as pd

from io import StringIO, BytesIO

from moto import mock_s3


from xetra.common.s3 import S3BucketConnector
from xetra.common.custom_exceptions import WrongFormatException


class TestS3BucketConnectorMethod(unittest.TestCase):
    """
    Testing the S3BucketConnector class
    """

    def setUp(self):
        """ 
        Setting up the enironment
        """
        # mockung s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key= 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key= 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test_bucket'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key]= 'KEY1'
        os.environ[self.s3_secret_key]= 'KEY2'
        # Creating a bucket on the mocked s3
        self.s3= boto3.resource(service_name='s3',endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-central-1'
                              })

        self.s3_bucket=self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)                                  


    def tearDown(self):
        """
        Executing after unittests
        """
        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_list_in_prefix_ok(self):
        """
        Tests the list files in prefix method for getting 2 files keys as a list on the mocket s3 bucket

        """
        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'        
        # Test init
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertEqual(len(list_result),2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key' : key1_exp                                  
                    },
                    {   
                        'Key' : key2_exp
                    }
                    ]
            }
        )
        
    def test_list_in_prefix_wrong_prefix(self):
        """
        Tests the list files in prefix method in case of a wrong or not existing prefix
        """
        # Expected results
        prefix_exp = 'no-prefix/'
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertTrue(not list_result)
               
    def test_read_csv_to_df_ok(self):
        """
        Test read_csv_to_df method for reading one csv file from the mocked s3 bucket
        """
        # Expected results
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'      
        # Test init
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)
        # Method execution
        with self.assertLogs() as logm:
             df = self.s3_bucket_conn.read_csv_to_df(key_exp)
             # Log test after method execution
             self.assertIn(log_exp,logm.output[0])
        # Tests after method execution
        self.assertEqual(df.shape[0],1)
        self.assertEqual(df.shape[1],2)
        self.assertEqual(val1_exp, df[col1_exp][0])
        self.assertEqual(val2_exp, df[col2_exp][0])
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key' : key_exp                                  
                    }
                    ]
            }
        ) 

    def test_write_df_to_s3_empty(self):
        """
        Test write_df_to_s3 method when Data Frame is empty as an imput
        """
        # Expected results
        return_exp = None
        log_exp = f'The DataFrame is empty. No file will be written!'   
        # Test init
        key_exp = 'test.csv'
        df_empty =pd.DataFrame()
        file_format ='csv'
        # Method execution
        with self.assertLogs() as logm:
             result=self.s3_bucket_conn.write_df_to_s3(df_empty, key_exp, file_format)
             # Log test after method execution
             self.assertIn(log_exp,logm.output[0])
        # Tests after method execution
        self.assertEqual(return_exp, result)
        
        

    def test_write_df_to_s3_csv(self):
        """
        Test write_df_to_s3 method if wtitten csv file is successfull
        """
        # Expected results
        return_exp= True
        key_exp = 'test.csv'
        df_exp =pd.DataFrame([['A','B'],['D','C']],columns=['col1','col2'])
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'   
        # Test init
        file_format ='csv'
        # Method execution
        with self.assertLogs() as logm:
             result=self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format)
             # Log test after method execution
             self.assertIn(log_exp,logm.output[0])
        # Tests after method execution
        data=self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO (data)
        df_result =pd.read_csv(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key' : key_exp                                  
                    }
                    ]
            }
        )     
    def test_write_df_to_s3_parquet(self):
        """
        Test write_df_to_s3 method if wtitten parquet file is successfull
        """
        # Expected results
        return_exp= True
        key_exp = 'test.parquet'
        df_exp =pd.DataFrame([['A','B'],['D','C']],columns=['col1','col2'])
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'   
        # Test init
        file_format ='parquet'
        # Method execution
        with self.assertLogs() as logm:
             result=self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format)
             # Log test after method execution
             self.assertIn(log_exp,logm.output[0])
        # Tests after method execution
        data=self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result =pd.read_parquet(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key' : key_exp                                  
                    }
                    ]
            }
        )    

    def test_write_df_to_s3_wrong_format(self):
        """
        Test write_df_to_s3 method if a not supported format is given as an argument
        """
        # Expected results
        key_exp = 'test.parquet'
        df_exp =pd.DataFrame([['A','B'],['D','C']],columns=['col1','col2'])
        format_exp = "wrong_format"  
        log_exp = f'The file {format_exp} format is not supported to be written to s3!'           
        exception_exp= WrongFormatException
        # Method execution
        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
             self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, format_exp)
             # Log test after method execution
             self.assertIn(log_exp,logm.output[0])
                        

if __name__=="__main__":
    unittest.main()
   # testIns = TestS3BucketConnectorMethod()
   # testIns.setUp()
   # testIns.test_list_in_prefix_ok()
   # testIns.test_list_in_prefix_wrong_prefix()
   # testIns.tearDown() 
