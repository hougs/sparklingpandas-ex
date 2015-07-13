"""Illustrates loading a simple dataframe, distributing it, and then mapping over it."""

from pyspark import SparkContext
import pandas as pd

sc = SparkContext(appName="PandaMapExample")
input = [[1, 2], [1, 2], [3, 4]]
rdd = sc.parallelize(zip(range(len(input)), input))
frames = rdd.map(lambda x: pd.DataFrame(data = [x[1]], index = [x[0]]))
sqF = frames.map(lambda f: f.applymap(lambda x: x * x))
sqDFs = sqF.collect()
sqDF = reduce(lambda x,y: x.append(y), sqDFs)
print sqDF
