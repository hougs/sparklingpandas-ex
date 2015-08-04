from pyspark import SparkContext
from sparklingpandas.pcontext import PSparkContext

# Create spark context.
sc = SparkContext("local", "example iris data")

# Create sparklingpandas context.
psc = PSparkContext(sc)

# read csv file with sparkling pandas context.
COL_NAMES = ['sepal_length',
             'sepal_width',
             'petal_length',
             'petal_width',
             'class']
iris_df = psc.read_csv("ex-data/iris.csv", names = COL_NAMES)

# Groupby Category
iris_classes = iris_df.groupby('class')