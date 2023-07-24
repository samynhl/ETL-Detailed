import extract
import pandas as pd 

# Set of Data Quality Checks Needed to Perform Before Loading
def Data_Quality(load_df):
    #Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No Songs Extracted')
        return False
    
    #Enforcing Primary keys since we don't need duplicates
    if pd.Series(load_df['name']).is_unique:
       pass
    else:
        #The Reason for using exception is to immediately terminate the program and avoid further processing
        raise Exception("Primary Key Exception,Data Might Contain duplicates")
    
    #Checking for Nulls in our data frame 
    if load_df.isnull().values.any():
        raise Exception("Null values found")

# Writing some Transformation Queries to get the count of artist
def transform_df(load_df):

    # Drop unused columns
    # Removing unwanted columns
    
    return load_df[['name','album','artist','duration','popularity']]

if __name__ == "__main__":

    #Importing the songs_df from the Extract.py
    load_df=extract.extract()
    Data_Quality(load_df)
    Transformed_df = Transform_df(load_df)
