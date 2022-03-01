#!/usr/bin/env python
# coding: utf-8

#Defining Required Libraries and List of 50 movies
import pyspark
from pyspark.sql import SparkSession

myList = ['Kigdom', 'Squid Game', 'See', 'Game of Thrones', 'Vikings', 'Peaky Blinders', 'The Queens Gambit', 'Delhi Crime',
          'Breathe(hindi)', 'Breaking Bad', 'Chernobyl', 'The Night Of', 'Patrick Melrose', 'Walking Dead', 'Rick & Morty',
          'Mortal Wound', 'Moana', 'Solar Opposites', 'Soul', 'Monsters University', 'Money Heist', 'Goblin', 'Wish Dragon',
          'Minions', 'The Mitchells vs. the Machines', 'Mr. Queen', 'Dexter', 'Shameless', 'Mr. Robot','Monsters, Inc.',
          'Hannibal', 'The Witcher', 'Lucifer', 'Dark', 'Better call Saul', 'The 100', 'The Maze Runner', 'Shrek', 'Tangled',
          'Pirates of the Caribbean', 'Coco', 'Zootropolis', 'Sing', 'Hotel Transylvania', 'Harry Potter', 'Pasta', 'Heirs',
          'Twenty', 'The Twilight Saga', 'Chicken Little']



#Defining Spark Session and Converting list to RDD
spark = SparkSession.builder.appName('SparkPractice').getOrCreate()
sc = spark.sparkContext
rdd = sc.parallelize(myList)


#Finding list's 20th item by filter function
twentiethItem = rdd.filter(lambda x: myList[19] in x).collect()
print("Result is:" , twentiethItem)


#Converting all letter of items to capital letters
capitalLetterItems = rdd.map(lambda x: x.upper()).collect()
print("Result is:\n", capitalLetterItems)


#Grouping items based on item's first letter by groupBy and map function
groupedItems = rdd.groupBy(lambda x: x[0]).map(lambda x: (x[0],list(x[1]))).collect()
print("Result is:\n", groupedItems)


#Converting Text to RDD and doing map reduce function
textRdd = sc.textFile("Data.txt")
textRddM = textRdd.flatMap(lambda x: x.split(' '))
textRddM = textRddM.map(lambda x: (x,1))
textRddM = textRddM.reduceByKey(lambda x,y: x+y)
textRddM = textRddM.collect()
print("Result is:\n", textRddM)
