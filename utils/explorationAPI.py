# EXPLORATION: Perform exploratory data analysis (EDA) to understand the dataset’s structure, identify potential issues, and summarize key statistics.

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def getDescriptionStatistics(df):

	return df.describe()


def getDataDistribution(df):

	if 'Country' not in df.columns or 'CO2 emissions ' not in df.columns:
		raise ValueError("The required columns are not present in the DataFrame")

	df['CO2 emissions '] = pd.to_numeric(df['CO2 emissions '], errors='coerce')

	# Histogram
	plt.figure(figsize=(10, 6))
	sns.histplot(df["CO2 emissions "], kde=True)
	plt.title("Distribution of CO2 Emissions")
	plt.xlabel("CO2 Emissions")
	plt.ylabel("Frequency")
	plt.show()

    # Box plot for 'CO2 emissions' by 'Country'
	plt.figure(figsize=(15, 8))
	sns.boxplot(data=df, x='Country', y='CO2 emissions ')
	plt.xlabel('Country')
	plt.ylabel('CO2 Emissions')
	plt.title('Boxplot of CO2 Emissions by Country')
	plt.xticks(rotation=90)
	plt.show()
    
	# # Line plot for 'CO2 emissions' by 'Country'
	# plt.figure(figsize=(10, 6))
	# sns.lineplot(data=df, x='Country', y='CO2 emissions ')
	# plt.title("CO2 Emissions Over Index")
	# plt.xlabel("Country")
	# plt.ylabel("CO2 Emissions")
	# plt.show()
    
	# # Bar plot for 'CO2 emissions' by 'Country'
	# plt.figure(figsize=(10, 6))
	# sns.barplot(data=df, x='Country', y='CO2 emissions ')
	# plt.title("Bar Plot of CO2 Emissions")
	# plt.xlabel("Country")
	# plt.ylabel("CO2 Emissions")
	# plt.show()


def getDataCorrelation(df):

	# Correlation matrix
	# correlation_matrix = df.corr()
	# sns.heatmap(correlation_matrix, annot=True)
	# plt.show()

	# Scatter plot
	# sns.scatterplot(x='Country', y='CO2 emissions ', data=df)
	# plt.show()

	return