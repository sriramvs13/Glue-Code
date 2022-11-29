import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import *
spark = SparkSession.builder.appName(
	'Read CSV File into DataFrame')\
	.config("spark.sql.legacy.timeParserPolicy","LEGACY")\
     .getOrCreate()
path1=r'C:\Users\vssri\Downloads\emp.txt'
path2=r'C:\Users\vssri\Downloads\finance.txt'
emp_df=spark.read.csv(path1,header=True)
empdf=spark.read.csv(path1).toDF('emp_id','name','superior_emp_id','year_joined','emp_dept_id','gender','salary')
findf=spark.read.csv(path2)
findf=spark.read.csv(path2).toDF('dept_name','dept_id')
empdf.show()
findf.show()

#JOIN - Inner Join/Default Join
# empdf.join(findf,empdf.emp_dept_id == findf.dept_id,'inner').show(truncate=False)
# print("Inner join executed successfully")

#Full Join/Full Outer Join/Outer Join
### Full join /Full outer /outer###
###join returns all rows from both datasets,
# where join expression doesn’t match it returns null on respective record columns.#
# empdf.join(findf,empdf.emp_dept_id==findf.dept_id,'outer').show(truncate=False)
# print("Outer join executed successfully")

#Left Outer Join,Left
# Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match,
# it assigns null for that record and drops records from right where match not found.
# empdf.join(findf,empdf.emp_dept_id==findf.dept_id,'left').show(truncate=False)
# print('Left join executed Successfully')

# right outer /right jon
#Right a.k.a Rightouter join is opposite of left join, here it returns all rows from the right dataset regardless of math found on the left dataset, when join expression doesn’t match,
#it assigns null for that record and drops records from left where match not found.
# print('####Right join executed Successfully#####')
# empdf.join(findf,empdf.emp_dept_id==findf.dept_id,'right').show(truncate=False)

# Left semi join
#leftsemi join is similar to inner join difference being leftsemi join returns all columns from the left dataset and ignores all columns from the right dataset. In other words, this join returns columns from the only left dataset for the records match in the right dataset on join expression,
#records not matched on join expression are ignored from both left and right datasets.
# empdf.join(findf,empdf.emp_dept_id==findf.dept_id,'leftsemi').show(truncate=False)
# print("Left semi join")

# Left Anti join ######
#returns only columns from the left dataset for non-matched records.
# empdf.join(findf,empdf.emp_dept_id==findf.dept_id,'leftanti').show(truncate=False)
# print('#######Left Anti join executed Successfully#######')

#Leftside table data should be always heavy compared to right

empdf.alias("emp1").join(empdf.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)
print('######Self join executed successfully######')
