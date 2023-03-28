'''
Read a melville-moby_dick text file and print the first 15 lines.
'''
from pathlib import Path
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

folder = Path(__file__).parent
mobydick_filename = str(folder / "melville-moby_dick.txt")
# Read a text file
text = sc\
    .textFile(mobydick_filename)

# Take first 15 lines
print(text.take(15))