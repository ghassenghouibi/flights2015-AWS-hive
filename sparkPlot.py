from pyspark.sql import SparkSession
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import numpy as np
import pandas
import csv

schema = StructType({
 	StructField('IATA_CODE',StringType(),True),
 	StructField('NUMBER_OF_FLIGHTS',IntegerType(),True),
 	StructField('MOST_LANDED_AIRPORT',StringType(),True),
 	StructField('NB_OF_MLA',IntegerType(),True),
 	StructField('LESS_LANDED_AIRPORT',StringType(),True),
 	StructField('NB_OF_LLA',IntegerType(),True)
})


spark = SparkSession.builder.appName("DFwithPython").getOrCreate()
sc=spark.sparkContext

df= spark.read.csv('classement_airports.txt',header=True,sep=',',schema=schema)

df.show()

flights={}
airports_best={}
airports_worst={}

with open('file.csv', newline='') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
		print(row['IATA_CODE'], row['NUMBER_OF_FLIGHTS'])
		flights.update( {row['IATA_CODE'] :int(row['NUMBER_OF_FLIGHTS'])} )
		airports_best.update( {row['MOST_LANDED_AIRPORT'] :int(row['NB_OF_MLA'])} )
		airports_worst.update( {row['LESS_LANDED_AIRPORT'] :int(row['NB_OF_LLA'])} )




plt.figure(1)
plt.bar(range(len(flights)), list(flights.values()), align='center')
plt.xticks(range(len(flights)), list(flights.keys()))
plt.xlabel("Nom de compagnies aériennes")
plt.ylabel("Nombre de vol par an")
plt.title("Classement des compagnies aériennes par nombre de vol")

plt.figure(2)
plt.bar(range(len(airports_best)), list(airports_best.values()), align='center')
plt.xticks(range(len(airports_best)), list(airports_best.keys()))
plt.xlabel("Nom de l'aéroport")
plt.ylabel("Nombre de vol par an")
plt.title("Classement des aéroports avec le plus d'influence par nombre de vol")

plt.figure(3)
plt.bar(range(len(airports_worst)), list(airports_worst.values()), align='center')
plt.xticks(range(len(airports_worst)), list(airports_worst.keys()))
plt.xlabel("Nom de l'aéroport")
plt.ylabel("Nombre de vol par an")
plt.title("Classement des aéroports avec le moins d'influence par nombre de vol")

plt.show()