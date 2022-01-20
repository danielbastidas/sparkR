Sys.setenv(JAVA_HOME="/usr/lib/jvm/jdk1.8.0_241")
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/spark-3.1.1-bin-hadoop3.2")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
ss <- sparkR.session (
    master = "spark://dbastidas-workstation:7077", 
    enableHiveSupport = FALSE,
    sparkConfig = list(spark.driver.memory = "2g"), 
    #sparkJars = c("/home/danielbastidas/Downloads/spark-avro_2.11-4.0.0.jar",
    #              "/home/danielbastidas/Downloads/spark-avro_2.11-2.4.3.jar",
    #              "/home/danielbastidas/Downloads/spark-sql_2.11-2.1.2.jar"),
    sparkPackages = c(#"com.databricks:spark-avro_2.11:3.2.0",
                      "org.apache.spark:spark-avro_2.12:3.1.1"
                      #"org.apache.spark:spark-sql_2.11:2.1.2"
      )
)

require(foreign)
require(nnet)
require(ggplot2)
require(reshape2)
library(RColorBrewer)
library(reshape2)
# load the ggally library adds functionality to the ggplot package
library(GGally)

# read all avro files in the folder
data <- SparkR::read.df(
  path="hdfs://localhost:9000/media/danielbastidas/HardDisk/BigData/hadoop/datanode/sink/parquet-factored/",
  source = "avro")

#data <- SparkR::read.df(
#  path="hdfs://localhost:9000/media/danielbastidas/HardDisk/BigData/hadoop/datanode/sink/parquet-factored/part-00000-60933d60-faaa-4275-850a-a08299171ac8-c000.avro",
#  source = "avro")

SparkR::head(data)
 
#data <- read.dta("https://stats.idre.ucla.edu/stat/data/hsbdemo.dta", convert.factors = TRUE)

# dataframe in parquet format
#pData <- read.parquet('/home/danielbastidas/git-repo/sparkR/students.parquet')

# counting null values in dataframe column 
#sum(is.na(pData$id))

#sparkDf <- createDataFrame(data)
#SparkR::write.parquet(sparkDf, "/home/danielbastidas/git-repo/sparkR/students.parquet")

#typeof(data$ses)
#typeof(data$prog)
#head(data)
SparkR::nrow(data)

# defining/creating schema
#schemaWithFactors <- structType(structField("id","double"), structField("female","integer"),structField("ses","integer"),
#                     structField("schtyp","integer"), structField("prog","integer"), structField("read","double"),
#                     structField("write","double"), structField("math","double"), structField("science","double"),
#                     structField("socst","double"), structField("honors","integer"), structField("awards","double"),
#                     structField("cid","int"))
#data2 <- collect(createDataFrame(data), stringsAsFactors = TRUE)
#rDf <- SparkR::createDataFrame(data, schema = schemaWithFactors)
#rDf <- SparkR::withColumn(rDf, "ses", SparkR::cast(rDf$ses, "integer"))
#SparkR::head(rDf)

#SparkR column types data types
#SparkR::dtypes(rDf)

library(sparklyr)
library(tidyverse)
#sc <- spark_connect(master="local")
#dataRemote <- copy_to(sc, data)

#dataFact <- dataRemote %>%
#  sparklyr::mutate(prog_idx = factor("prog")) %>%
#  collect

# remove unnecessary columns
data$id <- NULL
data$honors <- NULL
data$cid <- NULL
data$honorsFactored <- NULL

### Descriptive Statistics Section ####

# find some descriptive statistics about gender
SparkR::showDF(SparkR::describe(data, "gender"))
# descriptive statistics (count, mean, max, min, and mean standard deviation) for the whole data set
SparkR::showDF(SparkR::describe(data), vertical=TRUE)
# descriptive statistic range
SparkR::showDF(SparkR::agg(data, minimum = min(data$math), maximum = max(data$math), 
    range_width = abs(max(data$math) - min(data$math))))

# descriptive statistic average value of a column. Note that by using showDF we are only interested in printing
# the value. If we want to collect the result we should use collect
SparkR::showDF(agg(data, mean = mean(data$awards)))

# descriptive statistics variance
SparkR::showDF(SparkR::agg(data, variance = var(data$read)))
SparkR::showDF(SparkR::agg(data, variance = var(data$write)))
SparkR::showDF(SparkR::agg(data, variance = var(data$math)))
SparkR::showDF(SparkR::agg(data, variance = var(data$science)))
SparkR::showDF(SparkR::agg(data, variance = var(data$socst)))

# descriptive statistic standard deviation
SparkR::showDF(SparkR::agg(data, std_dev = sd(data$math)))

# drop/remove not available/null/missing values from a column
SparkR::dropna(df, cols = "mths_remng")

# descriptive statistics quantile
quantiles <- SparkR::approxQuantile(x = data, cols = c("read", "write", "math", "science", "socst"), 
                                    probabilities = c(0.25,0.50,0.75), relativeError = 0.001)

# descriptive statistics skewness
readSkewness <- SparkR::agg(data, skewness(data$read))
writeSkewness <- SparkR::agg(data, skewness(data$write))
mathSkewness <- SparkR::agg(data, skewness(data$math))
scienceSkewness <- SparkR::agg(data, skewness(data$science))
socstSkewness <- SparkR::agg(data, skewness(data$socst))

# descriptive statistics kurtosis
readKurtosis <- SparkR::agg(data, kurtosis(data$read))
writeKurtosis <- SparkR::agg(data, kurtosis(data$write))
mathKurtosis <- SparkR::agg(data, kurtosis(data$math))
scienceKurtosis <- SparkR::agg(data, kurtosis(data$science))
socstKurtosis <- SparkR::agg(data, kurtosis(data$socst))

# descriptive statistics correlation
corr_read_write <- SparkR::corr(data, "read", "write")
corr_math_science <- SparkR::corr(data, "math", "science")
corr_read_socst <- SparkR::corr(data, "read", "socst")

# descriptive statistics covariance (how strong are the correlations)
cov_read_write <- SparkR::cov(data, "read", "write")
cov_math_science <- SparkR::cov(data, "math", "science")
cov_read_socst <- SparkR::cov(data, "read", "socst")

# descriptive statistic frequency table/count elements
countByGender <- SparkR::count(groupBy(data, data$female))

# descriptive statistics contingency table. The categorization count between two categorical variables
contGenderProg <- SparkR::crosstab(data, "female", "prog")
contGenderSchTyp <- SparkR::crosstab(data, "female", "schtyp")
contSesSchtyp <- SparkR::crosstab(data, "ses", "schtyp")
contSesProg <- SparkR::crosstab(data, "ses", "prog")
contSchtypProg <- SparkR::crosstab(data, "schtyp", "prog")

### SparkR Basics Section ####

# execute and print the operation at the same time
(ncol <- ncol(data))

# for loops in R
initialIndex <- 0
endIndex <- 3
text <- "hello"
for (i in initialIndex:endIndex) {
  # concatenate strings
  text <- paste0(text, i)
}

# add rows to a dataset copy rows to dataset. Both data frames must have the same columns
rbind(df1, df2)

# create a subset assigning names columns in the subset
dataSubSet <- SparkR::select(data, col = c("female", "ses"))

# rename column
data <- SparkR::withColumnRenamed(data, "female","gender")
data <- SparkR::withColumnRenamed(data, "ses","soc_eco_sta")
data <- SparkR::withColumnRenamed(data, "schtyp","scho_typ")
data <- SparkR::withColumnRenamed(data, "femaleFactored","genderFactored")

# display/show column names
SparkR::columns(data)

# display/show a compact visualization of the data frame, column names, types and the firsts several rows
SparkR::str(data)

# display/show column types
SparkR::dtypes(data)
SparkR::schema(data)
SparkR::printSchema(data)

# casting columns types
data$awards <- SparkR::cast(data$awards, dataType = "integer")
data$sesFactored <- SparkR::cast(data$sesFactored, dataType = "integer")
data$schtypFactored <- SparkR::cast(data$schtypFactored, dataType = "integer")
data$progFactored <- SparkR::cast(data$progFactored, dataType = "integer")
data$genderFactored <- SparkR::cast(data$genderFactored, dataType = "integer")

# compute the number of distinct values in a column
showDF(SparkR::agg(data, distMath = countDistinct(data$math)))

# arrange/sort/order by column
showDF(SparkR::arrange(data, desc(data$math)))

# copy data frame/dataset and add a new column at the same time. Sometimes is useful if you want
# to retain your original dataset/data frame
data2 <- SparkR::withColumn(data, "test", data$math*0.5)

# cache. All nodes in the cluster loads into memory the data so future operations doesn't have to read
# the data from the source/files to perform dataframes/dataset operations
SparkR::cache(data)

# unpersist clears out a dataframe/dataset from the cache in all nodes in the cluster
SparkR::unpersist(data)

# measuring cache times
system.time(ncol(data))
system.time(nrow(data))
system.time(SparkR::head(SparkR::agg(SparkR::groupBy(data, data$prog), math_avg = avg(data$math))))
# remove objects from R memory session
rm(data)
# load the dataframe again
data <- SparkR::read.df(
  path="hdfs://localhost:9000/media/danielbastidas/HardDisk/BigData/hadoop/datanode/sink/parquet-factored/",
  source = "avro")

SparkR::cache(data)
system.time(ncol(data))
system.time(nrow(data))
system.time(SparkR::head(SparkR::agg(SparkR::groupBy(data, data$prog), math_avg = avg(data$math))))
SparkR::unpersist(data)

# convert SparkR dataframe/dataset into a R local dataframe. The collect operation has the same result
SparkR::take(data, num = 100)
SparkR::collect(data)

# subset dataframe/dataset by rows subset rows
showDF(SparkR::filter(data, data$prog == 'general' | data$prog == 'vocation'))
# we can also use sql syntax to filter
showDF(SparkR::filter(data, "prog = 'general' or prog = 'vocation'"))
# or using base R syntax
showDF(data[data$prog == 'general' | data$prog == 'vocation'],)
# filter empty values in columns
showDF(SparkR::filter(data, data$prog != ""))

# subset rows using where or group by with condition
showDF(agg(groupBy(where(data, data$math >= 65), where(data, data$math >= 65)$schtyp),
           math_avg = avg(where(data, data$math >= 65)$math),
           count = n(where(data, data$math >= 65)$math)))

# subset dataframe/dataset by columns or subset columns. You can use the column field or the column name.
# whatever approach you follow to reference the column must be the same for all the columns. For instance you
# can't mixed "prog" and data$schtyp
showDF(select(data, data$prog, data$schtyp))

# you can subset columns and add a new column at the same time
showDF(select(data, data$female, data$prog, abs(data$read - data$write) ))

# subset rows and columns at the same time. You can also use filter
showDF(select(where(data, data$female == 'female'), c("female","prog","math")))

# drop a column using null or drop funtion
showDF(drop(data, data$prog))

# subset or sample a dataframe/dataset. Getting only 1% of the data. You can use a seed if you always
# want to work with the same data in the sample. You can convert the sample to a local R dataset using collect
showDF(sample(data, withReplacement = FALSE, fraction = 0.01))
showDF(sample(data, withReplacement = FALSE, fraction = 0.01, seed = 0))

# collect the sample dataframe/dataset in one node of the cluster so you can export it or save it. Of course
# you need to make sure the data can fit in one node of the cluster
repartionedDF <- repartition(data, numPartitions = 1)
# then you can write the dataframe as usually

# specify null values when loading data in. In this case the columns with empty strings will be considered
# as null in the dataframe/dataset. This only applies for numeric columns. String will keep their empty string
# value
data <- SparkR::read.df(
  path="hdfs://localhost:9000/media/danielbastidas/HardDisk/BigData/hadoop/datanode/sink/parquet-factored/",
  source = "avro",
  na.strings = "")

# filter null values
showDF(where(data, isNotNull(data$math)))

# filter NAN values
showDF(where(data, !isNaN(data$math)))

# filter empty strings
showDF(where(data, data$gender != "" ))

# group by null data
data_math_null <- where(data, isNull(data$math))
showDF( agg( groupBy(data_math_null, data_math_null$gender), Nulls = n(data_math_null$gender) ) )

# drop rows with missing data. In this example rows with female null values will be removed from the dataframe
# a new dataframe will be returned
showDF(SparkR::dropna(data, cols = list("female")))

# drop rows where all the columns are null
showDF(SparkR::dropna(data, how = "all"))

# drop rows where some of the columns are null without specifying which columns
showDF(SparkR::dropna(data, how = "any"))

# drop rows where specifying a minimum number of non null columns
showDF(SparkR::dropna(data, minNonNulls = 3))

# fill all null value entries with a specified value
showDF(SparkR::fillna(data, value = 12345))

# fill only specific columns which has null values with some speficified value
showDF(SparkR::fillna(data, list("female" = "neutral")))

# fill column values depending on condition
data$female <- SparkR::ifelse(data$female == "", "Unknown", data$female)

# the merge function is similar to join but instead of adding the join column twice with the
# same name, it adds the join expression column with two different names joinExpr_column_x and joinExpr_column_y
# in the following example a female_x and female_y column will be added to the resulting dataframe
showDF(SparkR::merge(df1, df2, by = "female" ))

# splitting dataframe in two in order to show how to append rows to a dataframe
A <- SparkR::sample(data, withReplacement = FALSE, fraction = 0.5, seed = 1)
B <- SparkR::except(data, A)
# intersect two dataframes
showDF(SparkR::intersect(A, B))

# append rows from one dataframe to another. Notice both dataframes must have the same column names and order
# rbind allows you to append more than two dataframes
showDF(SparkR::rbind(A, B))

# union all does the same as rbind but only works with two dataframes
showDF(SparkR::unionAll(A, B))

# append rows when dataframes column names are not equal. This approach intersects returns the intersection
# of both dataframes
rbind.intersect <- function(x, y) {
  cols <- base::intersect(colnames(x), colnames(y))
  return(SparkR::rbind(x[, sort(cols)], y[, sort(cols)]))
}
showDF(rbind.intersect(df1, df2))

# a second approach to append rows when dataframes column names are not equal is: Use withColumn to add 
# columns to DF where they are missing and set each entry in the appended rows of these columns equal to NA.
rbind.fill <- function(x, y) {
  
  m1 <- ncol(x)
  m2 <- ncol(y)
  col_x <- colnames(x)
  col_y <- colnames(y)
  outersect <- function(x, y) {setdiff(union(x, y), intersect(x, y))}
  col_outer <- outersect(col_x, col_y)
  len <- length(col_outer)
  
  if (m2 < m1) {
    for (j in 1:len){
      y <- withColumn(y, col_outer[j], cast(lit(""), "string"))
    }
  } else { 
    if (m2 > m1) {
      for (j in 1:len){
        x <- withColumn(x, col_outer[j], cast(lit(""), "string"))
      }
    }
    if (m2 == m1 & col_x != col_y) {
      for (j in 1:len){
        x <- withColumn(x, col_outer[j], cast(lit(""), "string"))
        y <- withColumn(y, col_outer[j], cast(lit(""), "string"))
      }
    } else { }         
  }
  x_sort <- x[,sort(colnames(x))]
  y_sort <- y[,sort(colnames(y))]
  return(SparkR::rbind(x_sort, y_sort))
}
showDF(rbind.fill(df1, df2))

# dataframe dimensions rows and columns
dim(data)

# SparkR histogram
histogram <- SparkR::histogram(data, col = "math", nbins=10)
# visualize the histogram
ggplot(histogram, aes(x=centroids, y=counts)) + geom_path() + xlab("math") + ylab("Frequency") + 
  ggtitle("Freq. Polygon: math")

# visually examine how numerical variables correlate
geom_bivar_histogram.SparkR <- function(df, x, y, nbins){
  
  library(ggplot2)
  
  x_min <- collect(agg(df, x_min = min(df[[x]])))
  x_max <- collect(agg(df, max(df[[x]])))
  x.bin <- seq(floor(x_min[[1]]), ceiling(x_max[[1]]), length = nbins)
  
  y_min <- collect(agg(df, min(df[[y]])))
  y_max <- collect(agg(df, max(df[[y]])))
  y.bin <- seq(floor(y_min[[1]]), ceiling(y_max[[1]]), length = nbins)
  
  x.bin.w <- x.bin[[2]]-x.bin[[1]]
  y.bin.w <- y.bin[[2]]-y.bin[[1]]
  
  df_ <- withColumn(df, "x_bin_", ceiling((df[[x]] - x_min[[1]]) / x.bin.w))
  df_ <- withColumn(df_, "y_bin_", ceiling((df[[y]] - y_min[[1]]) / y.bin.w))
  
  df_ <- mutate(df_, x_bin = ifelse(df_$x_bin_ == 0, 1, df_$x_bin_))
  df_ <- mutate(df_, y_bin = ifelse(df_$y_bin_ == 0, 1, df_$y_bin_))
  
  dat <- collect(agg(groupBy(df_, "x_bin", "y_bin"), count = n(df_$x_bin)))
  
  p <- ggplot(dat, aes(x = x_bin, y = y_bin, fill = count)) + geom_tile()
  
  return(p)
}

p3 <- geom_bivar_histogram.SparkR(df = data, x = "read", y = "write", nbins = 100)
p3 + scale_colour_brewer(palette = "Blues", type = "seq") + xlab("Read") + ylab("Write") + 
  ggtitle("Read vs Write")

# transform or mutate a dataframe. Adds new columns or transformed the existed ones 
showDF(SparkR::transform(data, rw=(data$read + data$write), sq_math=data$math^2))

# convert char/string date to date type if the string date format is not using the default one: 'yyyy-mm-dd'
# get the unix timestamp
date_uts <- SparkR::unix_timestamp(df$myStringDate, 'MM/dd/yyyy')
# get the timestamp
date_ts <- SparkR::cast(date_uts, 'timestamp')
# get the date data type
date <- SparkR::cast(date_ts, 'date')
df <- SparkR::withColumn(df, 'date_column', date)

# convert char/string date to date type if the string date format is the default: 'yyyy-mm-dd'
# option 1: where dateColumn is the original column with the date as a string
df$dateColumn <- SparkR::to_date(df$dateColumn)
# option 2: 
df <- withColumn(df, 'convertedDateColumn', cast(df$dateColumn, 'date'))

# date operations
# returns the last day of the month for the specified date
showDF(SparkR::withColumn(df, 'last_day_of_month', SparkR::last_day(df$convertedDateColum)))
# Returns the first date which is later than the value of the date column that is on the specified day of the week
showDF(SparkR::withColumn(df, 'next_day', SparkR::next_day(df$convertedDateColum, "Sunday")))
# Returns the date that is 'numMonths' after 'startDate'
showDF(SparkR::withColumn(df, 'next_month', SparkR::add_months(df$convertedDateColum, 1)))
# Returns the date that is 'days' days after 'start'
showDF(SparkR::withColumn(df, 'add_date', SparkR::date_add(df$convertedDateColum, 1)))
# Returns the date that is 'days' days before 'start'
showDF(SparkR::withColumn(df, 'sub_date', SparkR::date_sub(df$convertedDateColum, 1)))
# Returns the week of year as an integer
showDF(SparkR::withColumn(df, 'week_of_year', SparkR::weekofyear(df$convertedDateColum)))
# Returns the day of year as an integer
showDF(SparkR::withColumn(df, 'day_of_year', SparkR::dayofyear(df$convertedDateColum)))
# Returns the day of month as an integer
showDF(SparkR::withColumn(df, 'day_of_month', SparkR::dayofmonth(df$convertedDateColum)))
# Returns the number of months between two dates
showDF(SparkR::withColumn(df, 'num_of_months', SparkR::months_between(df$date1, df$date2)))
# Returns the number of days between two dates
showDF(SparkR::withColumn(df, 'num_of_days', SparkR::datediff(df$date1, df$date2)))
# Returns the year as an integer
showDF(SparkR::withColumn(df, 'year', SparkR::year(df$date1)))
# Returns the month as an integer
showDF(SparkR::withColumn(df, 'month', SparkR::month(df$date1)))
# aggregations and grouping by date. Grouping by year. Re-sample by another unit of time
showDF(agg(groupBy(df, df$date1), month_mean = mean(df$month)))
# aggregations and grouping by date. Grouping by year and month. Re-sample by another unit of time
showDF(agg(groupBy(df, df$date1, df$month), days_mean = mean(df$num_of_days)))

#

### Analysis Section ####
# schema <- structType(structField("gender", "string"), 
#                      structField("count", "int"))
# countByGender <- SparkR::gapply(
#   data,
#   data$female, # key or grouping field
#   function(key, x) {
#     y <- data.frame(key, nrow(x))
#   },
#   schema)
countByGender$percentage <- countByGender$count/nrow(data)
# retrieve the dataset in memory
SparkR::collect(SparkR::arrange(countByGender, "count", decreasing = TRUE))
# only print the dataset
SparkR::showDF(countByGender)

# finding average for write, read, math, science and socst by gender
# schema represents the resulting rows
schema <- structType(structField("gender", "string"), 
                     structField("readAvg", "double"),
                     structField("writeAvg", "double"),
                     structField("mathAvg", "double"),
                     structField("scienceAvg", "double"),
                     structField("socstAvg", "double"))
avgByGender <- SparkR::gapply(
  data,
  data$female, # key or grouping field
  function(key, x) {
    y <- data.frame(key, mean(x$read), mean(x$write), mean(x$math), mean(x$science), mean(x$socst))
  },
  schema)
# there are two different ways to collect the data, sorting, and without sorting
SparkR::collect(SparkR::arrange(avgByGender, "readAvg", decreasing = TRUE))
SparkR::collect(avgByGender)

# drop the repeated column in the join operation
genderSummary <- SparkR::drop(SparkR::join(countByGender, avgByGender, countByGender$gender == avgByGender$gender), 
                              avgByGender$gender)
head(genderSummary)

# get descriptive statistics about the data (quartiles) to see if there are outliers
quantiles <- SparkR::approxQuantile(x = data, cols = c("read", "write", "math", "science", "socst"), 
                                    probabilities = c(0.25,0.50,0.75), relativeError = 0.001)

correlations <- c(SparkR::corr(rDf, "prog","prog"), 
                  SparkR::corr(rDf, "prog","female"), 
                  SparkR::corr(rDf, "prog","ses"), 
                  SparkR::corr(rDf, "prog","schtyp"), 
                  SparkR::corr(rDf, "prog","read"), 
                  SparkR::corr(rDf, "prog","write"),
                  SparkR::corr(rDf, "prog","math"),
                  SparkR::corr(rDf, "prog","science"),
                  SparkR::corr(rDf, "prog","socst"),
                  SparkR::corr(rDf, "prog","honors"),
                  SparkR::corr(rDf, "prog","awards"))

# TODO: create a correlation matrix to see how the variables relate to each other
# Create a correlation matrix indicating the indexes of the columns we want to use
# Create a correlation matrix between the columns or variables you might think influence a variable
# use the sparkr method corr SparkR::corr(rDf, "prog","prog") for the variables I think might influence another 
# variable
correlations <- cor(movies[,c(2,4,5,6)])

### Visualization analysis section ####
library(lattice)
library(ggplot2)

data$progFactored <- cast(data$progFactored, "integer")
data$femaleFactored <- cast(data$femaleFactored, "integer")
data$schtypFactored <- cast(data$schtypFactored, "integer")
data$sesFactored <- cast(data$sesFactored, "integer")
dataR <- SparkR::collect(data)

# univariate visualization for gender
# create a frequency bar chart for gender
ggplot(
  data = dataR,
  aes(x = female)) +
  geom_bar() + 
  ggtitle("count of students by gender")

# create a frequency bar chart for social economic status
ggplot(
  data = dataR,
  aes(x = ses)) +
  geom_bar() + 
  ggtitle("count of students by social economic status")

# create a frequency bar chart for school type
ggplot(
  data = dataR,
  aes(x = schtyp)) +
  geom_bar() + 
  ggtitle("count of students by school type")

# create a frequency bar chart for program
ggplot(
  data = dataR,
  aes(x = prog)) +
  geom_bar() + 
  ggtitle("count of students by program")

# create a cleveland dot plot
ggplot(
  data = dataR,
  aes(x = female)) +
  geom_point(stat = "count") + 
  coord_flip() +
  ggtitle("count of students by gender")

# quantitative univariate variable analysis using ggplot
# create a dot plot for read score
ggplot(
  data = dataR,
  # using statistical transformation to show the count of students read score
  aes(x = read, stat = "count")
) +
  geom_dotplot(binwidth = 1) +
  ggtitle("Distribution of students read score") +
  xlab("read score")

# create a box plot of runtime. Shows the quantiles
ggplot(
  data = dataR,
  aes(x = read, y = read)) +
  geom_boxplot() +
  coord_flip() +
  ggtitle("Distribution of students read score") +
  xlab("") +
  ylab("read score") +
  theme(
    axis.text.y = element_blank(),
    axis.ticks.y = element_blank()
  )

# create a density plot for read score
ggplot(
  data = dataR,
  aes(x = read)) +
  geom_density() +
  ggtitle("ditribution of students read score") +
  xlab("read score")

# create a density plot for write score
ggplot(
  data = dataR,
  aes(x = write)) +
  geom_density() +
  ggtitle("ditribution of students write score") +
  xlab("write score")

# create a density plot for math score
ggplot(
  data = dataR,
  aes(x = math)) +
  geom_density() +
  ggtitle("ditribution of students math score") +
  xlab("math score")

# create a density plot for science score
ggplot(
  data = dataR,
  aes(x = science)) +
  geom_density() +
  ggtitle("ditribution of students science score") +
  xlab("science score")

# create a density plot for social studies score
ggplot(
  data = dataR,
  aes(x = socst)) +
  geom_density() +
  ggtitle("ditribution of students social studies score") +
  xlab("social studies score")

# bivariate visualizations
# create a contingency table
progByGenderFreq <- table(
  dataR$female,
  dataR$prog
)

print(progByGenderFreq)

# create a grouped frequency barchart
ggplot(
  data = dataR,
  aes(x = prog, fill = female)) +
  geom_bar(position = "dodge") + 
  ggtitle("count of students by gender and program") +
  scale_fill_discrete(labels = c("female","male"))

# create a stacked frequency bar chart
ggplot(
  data = dataR,
  aes(x = prog, fill = female)) +
  geom_bar() + 
  ggtitle("count of students by gender and program") +
  scale_fill_discrete(labels = c("female","male"))

# create a 100% stacked frequency bar chart
ggplot(
  data = dataR,
  aes(x = prog, fill = female)) +
  geom_bar(position = "fill") + 
  ggtitle("proportion of students by gender and program") +
  ylab("Proportion of students") +
  scale_fill_discrete(labels = c("female","male"))

# create a spine plot
spineplot(
  x = progByGenderFreq,
  main = "Proportion of students by gender and program",
  xlab = "program",
  ylab = "gender"
)

spineplot(
  x = table(dataR$female, dataR$schtyp),
  main = "Proportion of students by gender and school type",
  xlab = "school type",
  ylab = "gender"
)

spineplot(
  x = table(dataR$female, dataR$ses),
  main = "Proportion of students by gender and social economic status",
  xlab = "social economic status",
  ylab = "gender"
)

# create a mosaic plot
mosaicplot(
  x = progByGenderFreq,
  main = "Proportion of students by gender and program",
  xlab = "program",
  ylab = "gender"
)

# create a scatter plot: useful when we want to find the correlation between two variables (weak, strong or
# no correlation at all) and whether the correlation is positive or negative. Is also useful to find the 
# spread, shape and location of the data
ggplot(
  data = dataR,
  aes(x = read, y = write)) +
  geom_point() + 
  ggtitle("Reading vs Writing score") + 
  xlab("Read score") +
  ylab("Write Score")

# add a linear regression line
ggplot(
  data = dataR,
  aes(x = read, y = write)) +
  geom_point() + 
  geom_smooth(method = "lm") +
  ggtitle("Reading vs Writing score") + 
  xlab("Read score") +
  ylab("Write score")

# add a linear regression line
ggplot(
  data = dataR,
  aes(x = math, y = read)) +
  geom_point() + 
  geom_smooth(method = "lm") +
  ggtitle("math vs read score") + 
  xlab("math score") +
  ylab("read score")

# add a linear regression line
ggplot(
  data = dataR,
  aes(x = read, y = math)) +
  geom_point() + 
  geom_smooth(method = "lm") +
  ggtitle("read vs math score") + 
  xlab("read score") +
  ylab("math score")

# add a linear regression line
ggplot(
  data = dataR,
  aes(x = write, y = math)) +
  geom_point() + 
  geom_smooth(method = "lm") +
  ggtitle("write vs math score") + 
  xlab("write score") +
  ylab("math score")

# add a linear regression line
ggplot(
  data = dataR,
  aes(x = math, y = write)) +
  geom_point() + 
  geom_smooth(method = "lm") +
  ggtitle("math vs write score") + 
  xlab("math score") +
  ylab("write score")

# add a linear regression line
ggplot(
  data = dataR,
  aes(x = math, y = science)) +
  geom_point() + 
  geom_smooth(method = "lm") +
  ggtitle("math vs science score") + 
  xlab("math score") +
  ylab("science score")

# add a linear regression line
ggplot(
  data = dataR,
  aes(x = read, y = socst)) +
  geom_point() + 
  geom_smooth(method = "lm") +
  ggtitle("read vs socst score") + 
  xlab("read score") +
  ylab("socst score")

# add a linear regression line
ggplot(
  data = dataR,
  aes(x = math, y = socst)) +
  geom_point() + 
  geom_smooth(method = "lm") +
  ggtitle("math vs socst score") + 
  xlab("math score") +
  ylab("socst score")

# create a frequency heat map: useful to get the frequency joint of two variables
ggplot(
  data = dataR,
  aes(x = read, y = write)) +
  stat_bin2d() +
  ggtitle("Reading vs Writing score") + 
  xlab("Read score") +
  ylab("Write score")

# create a hexagonal binned frequency heat map
ggplot(
  data = dataR,
  aes(x = read, y = write)) +
  stat_binhex() +
  ggtitle("Reading vs Writing score") + 
  xlab("Read score") +
  ylab("Write score")

# create a hexagonal binned frequency heat map
ggplot(
  data = dataR,
  aes(x = math, y = science)) +
  stat_binhex() +
  ggtitle("math vs science score") + 
  xlab("math score") +
  ylab("science score")

# create a hexagonal binned frequency heat map for women
ggplot(
  data = collect(subset(data, data$female == 'female', select = c(data$female, data$math, data$science))),
  aes(x = math, y = science)) +
  stat_binhex() +
  ggtitle("math vs science score for women") + 
  xlab("math score") +
  ylab("science score")

# create a hexagonal binned frequency heat map for women
ggplot(
  data = collect(subset(data, data$female == 'male', select = c(data$female, data$math, data$science))),
  aes(x = math, y = science)) +
  stat_binhex() +
  ggtitle("math vs science score for men") + 
  xlab("math score") +
  ylab("science score")

# create a hexagonal binned frequency heat map for women
ggplot(
  data = dataR,
  aes(x = math, y = awards)) +
  stat_binhex() +
  ggtitle("math vs awards") + 
  xlab("math score") +
  ylab("awards")

# create a contour plot of density to find where most of the data is located
ggplot(
  data = dataR,
  aes(x = read, y = write)) +
  geom_density2d() +
  ggtitle("Reading vs Writing score") + 
  xlab("Read score") +
  ylab("Write score")

# create a level plot of density
ggplot(
  data = dataR,
  aes(x = read, y = write)) +
  # the .. dot before and after the variable indicates that the value is not present in the 
  # data frame movies but it is calculated by the stat_density2d function
  stat_density2d(aes(fill = ..level..), geom = "polygon") +
  ggtitle("Reading vs Writing score") + 
  xlab("Read score") +
  ylab("Write score")

# create a level plot of density
ggplot(
  data = dataR,
  aes(x = math, y = science)) +
  # the .. dot before and after the variable indicates that the value is not present in the 
  # data frame movies but it is calculated by the stat_density2d function
  stat_density2d(aes(fill = ..level..), geom = "polygon") +
  ggtitle("math vs science score") + 
  xlab("math score") +
  ylab("science score")

library(lattice)
library(MASS)
# create a 2D kernel density estimation
density2d <- kde2d(
  x = dataR$read,
  y = dataR$write,
  n = 50
)

# create a grid from our 2D kernel density estimate. 
grid <- expand.grid(
  x = density2d$x,
  y = density2d$y
)

grid$z <- as.vector(density2d$z)

# create a mesh plot of density. Cannot be created with ggplot2, that's why we're using lattice
wireframe(
  x = z ~ x * y,
  data = grid,
  main = "Reading vs Writing score",
  xlab = "read score",
  ylab = "write score",
  zlab = "Density"
)

# create a surface plot of density
wireframe(
  x = z ~ x * y,
  data = grid,
  drape = TRUE,
  main = "Reading vs Writing score",
  xlab = "read score",
  ylab = "write score",
  zlab = "Density"
)

math_avg_by_gender <- collect(SparkR::agg(group_by(data, data$female), math_avg=mean(data$math)))
read_avg_by_gender <- collect(SparkR::agg(group_by(data, data$female), read_avg=avg(data$read)))
write_avg_by_gender <- collect(SparkR::agg(group_by(data, data$female), write_avg=avg(data$write)))
science_avg_by_gender <- collect(SparkR::agg(group_by(data, data$female), science_avg=avg(data$science)))
socst_avg_by_gender <- collect(SparkR::agg(group_by(data, data$female), socst_avg=avg(data$socst)))
awards_avg_by_gender <- collect(SparkR::agg(group_by(data, data$female), awards_avg=avg(data$awards)))
read_avg_by_ses <- SparkR::agg(group_by(data, data$ses), read_avg=avg(data$read))
read_avg_by_ses <- collect(SparkR::arrange(read_avg_by_ses, asc(read_avg_by_ses$read_avg)))
write_avg_by_ses <- collect(SparkR::agg(group_by(data, data$ses), write_avg=avg(data$write)))
math_avg_by_ses <- collect(SparkR::agg(group_by(data, data$ses), math_avg=avg(data$math)))
science_avg_by_ses <- collect(SparkR::agg(group_by(data, data$ses), science_avg=avg(data$science)))
socst_avg_by_ses <- collect(SparkR::agg(group_by(data, data$ses), socst_avg=avg(data$socst)))
awards_avg_by_ses <- collect(SparkR::agg(group_by(data, data$ses), awards_avg=avg(data$awards)))

math_avg_by_schtyp <- collect(SparkR::agg(group_by(data, data$schtyp), math_avg=mean(data$math)))
read_avg_by_schtyp <- collect(SparkR::agg(group_by(data, data$schtyp), read_avg=avg(data$read)))
write_avg_by_schtyp <- collect(SparkR::agg(group_by(data, data$schtyp), write_avg=avg(data$write)))
science_avg_by_schtyp <- collect(SparkR::agg(group_by(data, data$schtyp), science_avg=avg(data$science)))
socst_avg_by_schtyp <- collect(SparkR::agg(group_by(data, data$schtyp), socst_avg=avg(data$socst)))
awards_avg_by_schtyp <- collect(SparkR::agg(group_by(data, data$schtyp), awards_avg=avg(data$awards)))


# math maximum score by gender
math_max_by_gender <- collect(SparkR::agg(group_by(data, data$female), max=max(data$math)))

# create a bivariate bar chart
ggplot(
  data = math_avg_by_gender,
  aes(x = female, y = math_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("The title") +
  xlab("Gender") +
  ylab("math score")

# create a bivariate bar chart
ggplot(
  data = read_avg_by_gender,
  aes(x = female, y = read_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("Read score by gender") +
  xlab("Gender") +
  ylab("read score")

# create a bivariate bar chart
ggplot(
  data = write_avg_by_gender,
  aes(x = female, y = write_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("write score by gender") +
  xlab("Gender") +
  ylab("write score")

# create a bivariate bar chart
ggplot(
  data = science_avg_by_gender,
  aes(x = female, y = science_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("science score by gender") +
  xlab("Gender") +
  ylab("science score")

# create a bivariate bar chart
ggplot(
  data = socst_avg_by_gender,
  aes(x = female, y = socst_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("socst score by gender") +
  xlab("Gender") +
  ylab("socst score")

# create a bivariate bar chart
ggplot(
  data = awards_avg_by_gender,
  aes(x = female, y = awards_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("awards score by gender") +
  xlab("Gender") +
  ylab("awards")

# create a bivariate bar chart
ggplot(
  data = read_avg_by_ses,
  aes(x = reorder(ses, read_avg), y = read_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("read score by social economic status") +
  xlab("Social economic status") +
  ylab("read score")

# create a bivariate bar chart
ggplot(
  data = write_avg_by_ses,
  aes(x = reorder(ses, write_avg), y = write_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("write score by social economic status") +
  xlab("Social economic status") +
  ylab("write score")

# create a bivariate bar chart
ggplot(
  data = math_avg_by_ses,
  aes(x = reorder(ses, math_avg), y = math_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("math score by social economic status") +
  xlab("Social economic status") +
  ylab("math score")

# create a bivariate bar chart
ggplot(
  data = science_avg_by_ses,
  aes(x = reorder(ses, science_avg), y = science_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("science score by social economic status") +
  xlab("Social economic status") +
  ylab("science score")

# create a bivariate bar chart
ggplot(
  data = socst_avg_by_ses,
  aes(x = reorder(ses, socst_avg), y = socst_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("social studies score by social economic status") +
  xlab("Social economic status") +
  ylab("social studies score")

# create a bivariate bar chart
ggplot(
  data = awards_avg_by_ses,
  aes(x = reorder(ses, awards_avg), y = awards_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("awards by social economic status") +
  xlab("Social economic status") +
  ylab("awards")

# create a bivariate bar chart
ggplot(
  data = math_avg_by_schtyp,
  aes(x = reorder(schtyp, math_avg), y = math_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("math avg score by school type") +
  xlab("school type") +
  ylab("math avg score")

ggplot(
  data = read_avg_by_schtyp,
  aes(x = reorder(schtyp, read_avg), y = read_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("read avg score by school type") +
  xlab("school type") +
  ylab("read avg score")

ggplot(
  data = write_avg_by_schtyp,
  aes(x = reorder(schtyp, write_avg), y = write_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("write avg score by school type") +
  xlab("school type") +
  ylab("write score")

ggplot(
  data = science_avg_by_schtyp,
  aes(x = reorder(schtyp, science_avg), y = science_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("science avg score by school type") +
  xlab("school type") +
  ylab("science score")

ggplot(
  data = socst_avg_by_schtyp,
  aes(x = reorder(schtyp, socst_avg), y = socst_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("social studies avg score by school type") +
  xlab("school type") +
  ylab("social studies score")

# sort chart sort visualization sort graphic arrange chart arrange visualization arrange graphic
ggplot(
  data = awards_avg_by_schtyp,
  aes(x = reorder(schtyp, awards_avg), y = awards_avg)) +
  # indicates the value of our y transformation is the result of the statistical transformation
  geom_bar(stat = "identity") +
  ggtitle("average number of awards by school type") +
  xlab("school type") +
  ylab("awards number")

# create a bivariate box plot
ggplot(
  data = dataR,
  aes(x = female, y = math)) +
  geom_boxplot() +
  ggtitle("Average math score by gender") +
  xlab("gender") +
  ylab("math score")

# create a bivariate box plot
ggplot(
  data = dataR,
  aes(x = female, y = read)) +
  geom_boxplot() +
  ggtitle("Read score by gender") +
  xlab("gender") +
  ylab("read score")

# create a bivariate box plot
ggplot(
  data = dataR,
  aes(x = female, y = write)) +
  geom_boxplot() +
  ggtitle("write score by gender") +
  xlab("gender") +
  ylab("write score")

# create a bivariate box plot
ggplot(
  data = dataR,
  aes(x = female, y = science)) +
  geom_boxplot() +
  ggtitle("science score by gender") +
  xlab("gender") +
  ylab("science score")

# create a bivariate box plot
ggplot(
  data = dataR,
  aes(x = female, y = socst)) +
  geom_boxplot() +
  ggtitle("social studies score by gender") +
  xlab("gender") +
  ylab("social studies score")

# create a bivariate box plot
ggplot(
  data = dataR,
  aes(x = female, y = awards)) +
  geom_boxplot() +
  ggtitle("awards by gender") +
  xlab("gender") +
  ylab("awards score")

# create a notched box plot
ggplot(
  data = dataR,
  aes(x = female, y = read)) +
  geom_boxplot(notch = TRUE) +
  ggtitle("Average read score by gender") +
  xlab("gender") +
  ylab("read score")

# create a notched box plot. Gives you the confidence of the median value. The greater the notch the fewer
# the confidence in the median value
ggplot(
  data = dataR,
  aes(x = female, y = awards)) +
  geom_boxplot(notch = TRUE) +
  ggtitle("Average awards by gender") +
  xlab("gender") +
  ylab("awards")

# create a notched box plot
ggplot(
  data = dataR,
  aes(x = female, y = write)) +
  geom_boxplot(notch = TRUE) +
  ggtitle("Average write score by gender") +
  xlab("gender") +
  ylab("write score")

# create a notched box plot
ggplot(
  data = dataR,
  aes(x = female, y = math)) +
  geom_boxplot(notch = TRUE) +
  ggtitle("Average math score by gender") +
  xlab("gender") +
  ylab("math score")

# create a notched box plot
ggplot(
  data = dataR,
  aes(x = female, y = socst)) +
  geom_boxplot(notch = TRUE) +
  ggtitle("Average socst score by gender") +
  xlab("gender") +
  ylab("socst score")

# create a notched box plot
ggplot(
  data = dataR,
  aes(x = female, y = awards)) +
  geom_boxplot(notch = TRUE) +
  ggtitle("Average awards by gender") +
  xlab("gender") +
  ylab("awards")

# create a violin plot
ggplot(
  data = dataR,
  aes(x = female, y = read)) +
  geom_violin() +
  ggtitle("Average read score by gender") +
  xlab("gernder") +
  ylab("read score")

# create a violin plot
ggplot(
  data = dataR,
  aes(x = female, y = write)) +
  geom_violin() +
  ggtitle("Average write score by gender") +
  xlab("gernder") +
  ylab("write score")

# create a violin plot
ggplot(
  data = dataR,
  aes(x = female, y = math)) +
  geom_violin() +
  ggtitle("Average math score by gender") +
  xlab("gernder") +
  ylab("math score")

# create a violin plot
ggplot(
  data = dataR,
  aes(x = female, y = science)) +
  geom_violin() +
  ggtitle("Average science score by gender") +
  xlab("gernder") +
  ylab("science score")

# create a violin plot
ggplot(
  data = dataR,
  aes(x = female, y = socst)) +
  geom_violin() +
  ggtitle("Average social studies score by gender") +
  xlab("gernder") +
  ylab("social studies score")

# create a violin plot
ggplot(
  data = dataR,
  aes(x = female, y = awards)) +
  geom_violin() +
  ggtitle("Average awards by gender") +
  xlab("gernder") +
  ylab("awards")

# mutlti-variable data analysis using the base plot system
# create parameterized function to plot line chart
plotFacet <- function(index, name) {
  values <- dataR[, c(17, index)] # 17 is the progFactored column
  yMax <- 100#max(dataR$read)
  
  plot(
    x = values,
    type = "p",
    ylim = c(0, yMax),
    main = name,
    xlab = "Program",
    ylab = "Subject score"
  )
}

# create horizontal facets indicating that we want 1 row and 4 columns
par(mar=c(1,1,1,1))
# horizontal facets. we want to give importance to variable in the Y axis
par(mfrow = c(1,5))
# vertical facets. we want to give importance to variable in the X axis
par(mfrow = c(5, 1))
# create wrapped facets. The X and Y variables are equally important
par(mfrow = c(2, 2))

plotFacet(6, "read")
plotFacet(7, "write")
plotFacet(8, "math")
plotFacet(9, "science")
plotFacet(10, "socst")

# Reset to indicate that any subsequent graph will be displayed using only 1 row and 1 column
par(mfrow = c(1,1))

# mutlti-variable data analysis using ggplot
# horizontal facet
ggplot(
  data = dataR,
  aes(
    x = read,
    y = awards
  )) + geom_line() + 
  facet_grid(
    # indicates rows (.) and columns (Rating)
    facets = .~prog) + 
  ggtitle("awards number by read score and school program") + 
  xlab("read score") +
  ylab("awards")

# horizontal facet
ggplot(
  data = dataR,
  aes(
    x = femaleFactored,
    y = science
  )) + geom_line() + 
  facet_grid(
    # indicates rows (.) and columns (Rating)
    facets = .~schtypFactored) + 
  ggtitle("science score by gender and school type") + 
  xlab("gender") +
  ylab("science score")

# vertical facet
ggplot(
  data = dataR,
  aes(
    x = femaleFactored,
    y = science
  )) + geom_line() + 
  facet_grid(
    # indicates rows (.) and columns (Rating)
    facets = schtypFactored ~.) + 
  ggtitle("science score by gender and school type") + 
  xlab("gender") +
  ylab("science score")

# wrapped facets
ggplot(
  data = dataR,
  aes(
    x = femaleFactored,
    y = science
  )) + geom_line() + 
  facet_wrap(
    facets = ~schtypFactored) + 
  ggtitle("science score by gender and school type") + 
  xlab("Gender") +
  ylab("science score")

# create 2-dimensional facets (vertical). 4 variables displayed at the same time
ggplot(
  data = dataR,
  aes(
    x = female,
    y = science
  )) + geom_line() + 
  facet_grid(
    facets = schtyp~ses) + 
  ggtitle("science score by gender, school type, and social economic status") + 
  xlab("gender") +
  ylab("science score")

# create 2-dimensional facets (vertical). 4 variables displayed at the same time, using three categorical variables
ggplot(
  data = dataR,
  aes(
    x = female,
    y = awards
  )) + geom_line() + 
  facet_grid(
    facets = schtyp~ses) + 
  ggtitle("number of awards by gender, school type, and social economic status") + 
  xlab("gender") +
  ylab("number of awards")

# create 2-dimensional facets (vertical). 4 variables displayed at the same time, using two categorical variables
ggplot(
  data = dataR,
  aes(
    x = awards,
    y = read
  )) + geom_line() + 
  facet_grid(
    facets = schtyp~ses) + 
  ggtitle("read score by number of awards, school type, and social economic status") + 
  xlab("awards") +
  ylab("read score")

# three categorical variables visualization using ggplot2
ggplot(
  data = dataR,
  aes(
    x = schtyp,
    fill = female)) +
  geom_bar(
    position = "dodge") +
  facet_wrap(
    facets = ~ses) +
  ggtitle("Count of students by gender, school type, and social economic status") + 
  xlab("school type") +
  ylab("Count of students")

# three categorical variables visualization using ggplot2
ggplot(
  data = dataR,
  aes(
    x = prog,
    fill = female)) +
  geom_bar(
    position = "dodge") +
  facet_wrap(
    facets = ~ses) +
  ggtitle("Count of students by gender, school program, and social economic status") + 
  xlab("school program") +
  ylab("Count of students")

# create a faceted stacked frequency bar chart
ggplot(
  data = dataR,
  aes(
    x = prog,
    fill = female)) +
  geom_bar() +
  facet_wrap(
    facets = ~ses) +
  ggtitle("Count of students by school program, gender and social economic status") + 
  xlab("school program") +
  ylab("Count of students")

# create a 2D faceted frequency bar chart
ggplot(
  data = dataR,
  aes(
    x = prog)) +
  geom_bar() +
  facet_grid(
    facets = female ~ schtyp) +
  ggtitle("Count of students by school program, gender and school type") + 
  xlab("school program") +
  ylab("Count of students")

# visualization of two categorical and one numeric variables
# create a grouped bar chart
ggplot(
  data = dataR,
  aes(
    x = female,
    y = socst,
    fill = prog)) + 
  geom_bar(
    stat = "summary",
    fun = "mean",
    position = "dodge") + 
  ggtitle("Average social studies score by gender and school program") + 
  xlab("gender") +
  ylab("social studies score")

# create a grouped bar chart
ggplot(
  data = dataR,
  aes(
    x = schtyp,
    y = socst,
    fill = ses)) + 
  geom_bar(
    stat = "summary",
    fun = "mean",
    position = "dodge") + 
  ggtitle("social studies score by school type and social economic status") + 
  xlab("school type") +
  ylab("social economic status")

# create a stacked bar chart
ggplot(
  data = dataR,
  aes(
    x = ses,
    y = science,
    fill = schtyp)) + 
  geom_bar(
    stat = "summary",
    fun = "mean",
    position = "stack") + 
  ggtitle("Average science score by school type and social economic status") + 
  xlab("school type") +
  ylab("science score")

# create a faceted bar char
ggplot(
  data = dataR,
  aes(
    x = female,
    y = science)) + 
  geom_bar(
    stat = "summary",
    fun = "mean") + 
  facet_wrap(
    facets = ~schtyp) +
  ggtitle("Average science score by gender and school type") + 
  xlab("gender") +
  ylab("science score")

# create a heat map
ggplot(
  data = collect(agg(groupBy(select(data, data$female, data$schtyp, data$math), 
          "female", "schtyp"), math_mean=mean(data$math))),
  aes(
    x = female,
    y = schtyp,
    fill = math_mean)) + 
  geom_tile(
    stat = "identity") +
  ggtitle("Average math score by gender and school type") + 
  xlab("gender") +
  ylab("school type") +
  labs(fill = "math score")

# one categorical and two numeric variables using ggplot2
# create a color-coded scatter plot
# create a color palette
# load color brewer
library(RColorBrewer)
colors <- brewer.pal(4, "Set1")

ggplot(
  data = dataR,
  aes(
    x = write,
    y = math,
    color = female
  )
) + scale_color_manual(
  name = "female",
  labels = levels(as.factor(dataR$female)),
  values = colors
) + geom_point() + 
  ggtitle("write score vs math score by gender") + 
  xlab("Write Score") +
  ylab("Math Score")

ggplot(
  data = dataR,
  aes(
    x = read,
    y = write,
    color = female
  )
) + scale_color_manual(
  name = "female",
  labels = levels(as.factor(dataR$female)),
  values = colors
) + geom_point() + 
  ggtitle("read score vs write score by gender") + 
  xlab("read Score") +
  ylab("write Score")

ggplot(
  data = dataR,
  aes(
    x = read,
    y = awards,
    color = female
  )
) + scale_color_manual(
  name = "female",
  labels = levels(as.factor(dataR$female)),
  values = colors
) + geom_point() + 
  ggtitle("read score vs number of awards by gender") + 
  xlab("read Score") +
  ylab("number of awards")

# create a shape palette for the pch parameter print character
shapes <- c(1, 2)
# create a shape-coded scatter plot
ggplot(
  data = dataR,
  aes(
    x = read,
    y = write,
    shape = female
  )
) + scale_shape_manual(
  name = "Gender",
  labels = levels(as.factor(dataR$female)),
  values = shapes
) + geom_point() + 
  ggtitle("read score vs write score by gender") + 
  xlab("Read Score (%)") +
  ylab("Write Score")

# create a faceted scatter plot
ggplot(
  data = dataR,
  aes(
    x = math,
    y = science
  )
) + geom_point() + 
  facet_wrap(
    facets = ~schtyp
  ) +
  ggtitle("math score vs science score by gemder") + 
  xlab("math score") +
  ylab("science score")

# create a multi-series line chart
ggplot(
  data = dataR,
  aes(
    x = read,
    y = write,
    color = female
  )
) + geom_line() + 
  ggtitle("read score vs write score by gender") + 
  xlab("Read Score") +
  ylab("Write Score")

# create a multi-series line chart
ggplot(
  data = dataR,
  aes(
    x = read,
    y = write,
    color = prog
  )
) + geom_line() + 
  ggtitle("read score vs write score by program") + 
  xlab("Read Score") +
  ylab("Write Score")

# create a stacked area chart
ggplot(
  data = dataR,
  aes(
    x = read,
    y = write,
    fill = female
  )
) + geom_area() + 
  ggtitle("read score vs write score by gender") + 
  xlab("read score") +
  ylab("write score")

# split data by range, subset by range
readByRange <- withColumn(data, "readRange", 
          coalesce(
            when(data$read >= 30 & data$read <= 40, lit("30-40")),
            when(data$read >= 41 & data$read <= 50, lit("41-50")),
            when(data$read >= 51 & data$read <= 60, lit("51-60")),
            when(data$read >= 61 & data$read <= 70, lit("61-70")),
            when(data$read >= 71 & data$read <= 80, lit(">70")),
            lit("<30")
          ))

readByRange <- withColumn(readByRange, "readRangeOrder", 
          coalesce(
            when(readByRange$readRange == "<30", lit(as.integer(0))),
            when(readByRange$readRange == "30-40", lit(as.integer(1))),
            when(readByRange$readRange == "41-50", lit(as.integer(2))),
            when(readByRange$readRange == "51-60", lit(as.integer(3))),
            when(readByRange$readRange == "61-70", lit(as.integer(4))),
            when(readByRange$readRange == ">70", lit(as.integer(5)))
          ))

# create a multi-series line chart
ggplot(
  data = collect(agg(groupBy(readByRange, readByRange$readRange, readByRange$female, readByRange$readRangeOrder), 
                     writeAvg = mean(readByRange$write))),
  aes(
    x = reorder(readRange, readRangeOrder),
    y = writeAvg,
    color = female
  )
) + geom_point() + 
  ggtitle("read score vs write score by gender") + 
  xlab("read score") +
  ylab("write score (avg)")

# create a stacked area chart
ggplot(
  data = SparkR::collect(agg(groupBy(readByRange, readByRange$readRangeOrder, readByRange$female), 
                     writeAvg = mean(readByRange$write))),
  aes(
    x = readRangeOrder,
    y = writeAvg,
    fill = female
  )
) + geom_area() + 
  ggtitle("read score vs write score by gender") + 
  xlab("read score") +
  ylab("write score")

# create a faceted line chart
ggplot(
  data = SparkR::collect(agg(groupBy(readByRange, readByRange$readRangeOrder, readByRange$female), 
                             writeAvg = mean(readByRange$write))),
  aes(
    x = readRangeOrder,
    y = writeAvg
  )
) + geom_line() + 
  facet_wrap(
    facets = ~female
  ) +
  ggtitle("read score vs write score by gender") + 
  xlab("read score") +
  ylab("write score")

gradient <- brewer.pal(5, "YlOrRd")
# three numerical variables using ggplot2
# create a gradient color-scale scatter plot
ggplot(
  data = dataR,
  aes(
    x = read,
    y = write, 
    color = awards)) + 
  geom_point() + 
  scale_color_gradient(
    low = gradient[1],
    high = gradient[5]) + 
  ggtitle("read score, write score, and number of awards") +
  xlab("read score") + 
  ylab("write score") +
  labs(color = "awards")

# create a gradient color-scale scatter plot. Useful if you want a heat map with three quantitative variables
ggplot(
  data = dataR,
  aes(
    x = science,
    y = math, 
    color = awards)) + 
  geom_point() + 
  scale_color_gradient(
    low = gradient[1],
    high = gradient[5]) + 
  ggtitle("science score, math score, and number of awards") +
  xlab("science score") + 
  ylab("math score") +
  labs(color = "awards")

# create a gradient color-scale scatter plot
ggplot(
  data = dataR,
  aes(
    x = science,
    y = math, 
    color = socst)) + 
  geom_point() + 
  scale_color_gradient(
    low = gradient[1],
    high = gradient[5]) + 
  ggtitle("science score, math score, and social economic studies score") +
  xlab("science score") + 
  ylab("math score") +
  labs(color = "social economic studies")

# create divergent color palette
divergent <- rev(brewer.pal(5, "RdBu"))

# create divergent color-scale scatter plot
palette(divergent)

# create a divergent color-scale scatter plot. Useful if you want a clear contrast in your heat map with three
# quantitative variables
ggplot(
  data = dataR,
  aes(
    x = read,
    y = write, 
    color = socst)) + 
  geom_point() + 
  scale_color_gradientn(colors = divergent) + 
  ggtitle("read score, write score, and social economic studies score") +
  xlab("read score") + 
  ylab("write score") +
  labs(color = "socst")

# create a bubble chart. Instead of using a color label we could also use a size label
ggplot(
  data = dataR,
  aes(
    x = science,
    y = awards, 
    size = math, 10)) + 
  geom_point() + 
  scale_size_area() + 
  ggtitle("science score, awards number, and math score") +
  xlab("science score") + 
  ylab("awards") +
  # size label
  labs(color = "math score")

# create a bubble chart. Instead of using a color label we could also use a size label. If you want to replace a 
# heat map with a bubble map
ggplot(
  data = dataR,
  aes(
    x = science,
    y = math, 
    size = awards, 10)) + 
  geom_point() + 
  scale_size_area() + 
  ggtitle("science score, math score, and awards number") +
  xlab("science score") + 
  ylab("math score") +
  # size label
  labs(color = "awards")

# visualize many quantitative variables using ggplot2
# Create a correlation matrix indicating the indexes of the columns we want to use
# the following line pick read, write, math, science, socst, and awards
correlations <- cor(dataR[,c(6,7,8,9,10,11)])
melted <- melt(correlations)

round(correlations, 2)

ggplot(
  data = melted,
  aes(
    x = Var1,
    y = Var2,
    fill = value)) + 
  geom_tile() + 
  scale_fill_gradient2(
    low = "red",
    mid = "white",
    high = "blue",
    limit = c(-1, 1),
    midpoint = 0
  )

# create a scatter plot matrix. The graphs shown depends on the variable type (qualitative or quantitative)
# quick way to see correlations, curves and data spread, correlations shortcut
ggpairs(
  data = dataR,
  columns = c(6:11)
)

ggpairs(
  data = dataR,
  columns = c(2:6)
)

### findings ####
# 1. you will get a little higher score in writing than the score you get in reading. The scatter plot
# using two quantitative variables and one qualitative variable contradicts the finding, I think so
# 2. you will most probably get the same score in science as in math
# 3. there's almost no difference between men and women in social studies
# 4. women are considerably better at writing than men
# 5. women get considerably more awards than men
# 6. people with higher economic status get considerably more awards than other economic status
# 7. public school type doesn't do better than private schools in any category
# 8. There is a remarkable difference (10 points approx) between the lower score for students
# in private and public schools (students at private school doing much better). Although
# students at public school score much higher than students at private school (5 points approx)
# people with a score less than 50 points don't have awards
# the stronger correlation (0.85) is between write and awards