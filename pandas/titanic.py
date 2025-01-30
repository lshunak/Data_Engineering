import numpy as np
import pandas as pd

# Load the dataset
df = pd.read_csv('Titanic-Dataset.csv')

#Get dataset details
#get columns
print("Columns in the dataset")
print(df.columns)
#Get shape of the dataset
print("Shape of the dataset")
print(df.shape)

#Get dataset info
print("Dataset info")
print(df.info())

# Check for missing values
print("Missing values in the dataset:")
print(df.isnull().sum())

# Calculate missing values percentage
missing_percentage = (df.isnull().sum() / len(df)) * 100
print("\nMissing Values Percentage:")
print(missing_percentage)

print("\nNumerical Variables Statistics:")
print(df.describe())

# Display the first 5 rows of the dataset
print(df.head(8))