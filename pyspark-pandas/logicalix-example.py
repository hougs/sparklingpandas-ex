"""Illustrates loading a simple dataframe, distributing it, and then applying logical indexing"""

from pyspark import SparkContext
import pandas as pd

sc = SparkContext(appName="PandaMapExample")
input = [[1, 2], [1, 2], [3, 4]]
rdd = sc.parallelize(zip(range(len(input)), input))
frames = rdd.map(lambda x: pd.DataFrame(data = [x[1]], index = [x[0]]))
framesWithFour = frames.map(lambda x: x[x[1] == 4])
dfs = framesWithFour.collect()
df = reduce(lambda x,y: x.append(y), dfs)
print df
