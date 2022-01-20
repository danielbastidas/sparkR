# start slave: start-slave.sh spark://ubuntu:7077
# this is how you install the SparkR package: 
#install.packages("https://cran.r-project.org/src/contrib/Archive/SparkR/SparkR_2.4.6.tar.gz",
#repos = NULL,
#type = "source")

Sys.setenv(JAVA_HOME="/usr/lib/jvm/jdk1.8.0_241")
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/spark-3.1.1-bin-hadoop3.2")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
ss <- sparkR.session(master = "spark://dbastidas-workstation:7077", enableHiveSupport = TRUE,
  sparkConfig = list(spark.driver.memory = "2g"))

#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))

cars <- SparkR::createDataFrame(mtcars)
SparkR::head(cars, 10)

# select only one column. Notice that you can also get the same information using the following command: 
#SparkR::head(cars$hp) but using the select command provides a easier to read output
SparkR::head(select(cars, cars$hp))

# select only one column refering the column name as a string
SparkR::head(select(cars, "mpg"))

# filter data frames by conditions
SparkR::head(filter(cars, cars$mpg > 20))

# grouping and aggregation count
cylinderCount <- summarize(group_by(cars, cars$cyl), count = n(cars$cyl))
SparkR::head(cylinderCount)
SparkR::head(SparkR::arrange(cylinderCount, desc(cylinderCount$count)))

# grouping and aggregation sum
cylinderSum <- summarize(group_by(cars, cars$cyl), sum = sum(cars$mpg))
SparkR::head(cylinderSum)

# grouping and aggregation sum
cylinderAvg <- summarize(group_by(cars, cars$cyl), average = avg(cars$mpg))
SparkR::head(cylinderAvg)

# cube operator
SparkR::head(SparkR::agg(SparkR::cube(cars, cars$cyl, cars$disp, cars$gear), avg(cars$mpg)))

# rollup operator
SparkR::head(SparkR::agg(SparkR::rollup(cars, "cyl", "disp", "gear"), avg(cars$mpg)))

# applying function to spark data frame
schema <- structType(structField("mpg","double"), 
  structField("cyl","double"), structField("hp","double"),
  structField("disp","double"), structField("drat", "double"), 
  structField("wt", "double"), structField("qsec","double"),
  structField("vs", "double"), structField("am", "double"),
  structField("gear","double"), structField("carb", "double"),
  structField("kpg","double"))

df1 <- dapply(cars, function(car) { car <- cbind(car, "kpg" = car$mpg * 1.60 )}, schema)
SparkR::head(SparkR::collect(df1))

# applying function but returning a R data frame, so if it doesn't fit into memory it will
# fail
ldf <- dapplyCollect(
  cars,
  function(car) {
    car <- cbind(car, "kpg" = car$mpg * 1.60)
  })
head(ldf, 3)

# apply a function to each group of a spark data frame
schema <- structType(structField("cyl","double"), structField("maxKpg","double"))
result <- SparkR::gapply(
  df1,
  df1$cyl,
  function(key, car) {
    y <- data.frame(key, max(car$kpg))
  },
  schema
)

SparkR::head(collect(arrange(result, desc(result$maxKpg))))

# gapplyCollect collects a group but without the need for a schema
result <- gapplyCollect(
  df1,
  "cyl",
  function(key, x) {
    y <- data.frame(key, max(x$kpg))
    colnames(y) <- c("cylinders", "kilometers_per_galon")
    y
  })

SparkR::head(result[order(result$kilometers_per_galon, decreasing = TRUE), ])

# spark.lapply runs a function over a list of elements and distributes the 
# computation with spark. The results of all computations should fit in a single
# machine. If that is not the case convert the data frame into a spark data frame
# using SparkR::createDataFrame

families <- c("gaussian", "poisson")
# local R function
train <- function(family) {
  model <- glm(Sepal.Length ~ Sepal.Width + Species, iris, family = family)
  summary(model)
}

# Return a list of model's summaries
model.summaries <- spark.lapply(families, train)
# Print the summary of each model
print(model.summaries)

#################### Loading spark data frames from files ###################
# Load data from data source. Load json file
peopleDF <- read.json(file.path(Sys.getenv("SPARK_HOME"), "examples/src/main/resources/people.json"))
printSchema(peopleDF)
SparkR::head(peopleDF)

# Running SQL queries from SparkR
# Register the spark data frame as a temporary view 
SparkR::createOrReplaceTempView(peopleDF, "people")

# SQL statements can be run by using the sql method
namesContainingA <- SparkR::sql("select name from people where name like '%a%' or 
                                name like '%A%'" )
head(namesContainingA)

# load more than one file at the same time
people <- SparkR::read.json(file.path(Sys.getenv("SPARK_HOME"), c("examples/src/main/resources/people.json", "examples/src/main/resources/employees.json")))
SparkR::head(people)

# loading csv file
csv <- SparkR::read.df("/opt/spark-3.1.1-bin-hadoop3.2/examples/src/main/resources/people.csv", 
               "csv", header = "true", inferSchema = "true", na.strings = "NA")
SparkR::head(csv)

# convert one file into parquet file format
SparkR::write.df(people, path = "/home/danielbastidas/git-repo/sparkR/people.parquet", 
         source = "parquet", mode = "overwrite")

##### Machine Learning #####
# Logistic regression: predicts a binomial/binary categorical value. 

# Loading data
df <- read.df(path="/opt/spark-3.1.1-bin-hadoop3.2/data/mllib/sample_libsvm_data.txt", source = "libsvm")
training <- df
testing <- df

# model
binomialModel <- spark.logit(training, label ~ features, maxIter = 10, regParam = 0.3, elasticNetParam = 0.8)
SparkR::summary(binomialModel)

#multinomialModel <- spark.logit(training, label ~ features, maxIter = 10, regParam = 0.3, elasticNetParam = 0.8, family="multinomial")
#SparkR::summary(multinomialModel)

# Prediction
binomialPrediction <- predict(binomialModel, testing)
head(binomialPrediction)

#multinomialPrediction <- predict(multinomialModel, testing)
#head(multinomialPrediction)

# Logistic regression: predicts a multinomial value (more than two categories). 
# Load training data
multinomialDf <- read.df("/opt/spark-3.1.1-bin-hadoop3.2/data/mllib/sample_multiclass_classification_data.txt", source = "libsvm")
trainingMultinomial <- multinomialDf
testingMultinomial <- multinomialDf

# Fit a multinomial logistic regression model with spark.logit. Notice you don't have to specify the family,
# R infers that from the data frame
multinomialModel <- spark.logit(trainingMultinomial, label ~ features, maxIter = 10, regParam = 0.3, elasticNetParam = 0.8)

# Model summary
summary(multinomialModel)

# Prediction
multinomialPredictions <- SparkR::predict(multinomialModel, testingMultinomial)
SparkR::head(multinomialPredictions)

# decision tree classifier
df <- read.df("/opt/spark-3.1.1-bin-hadoop3.2/data/mllib/sample_libsvm_data.txt", source = "libsvm")
training <- df
test <- df

# Fit a DecisionTree classification model with spark.decisionTree
model <- spark.decisionTree(training, label ~ features, "classification")

# Model summary
summary(model)

# Prediction
predictions <- predict(model, test)
head(predictions)

# Fit a random forest classification model with spark.randomForest
model <- spark.randomForest(training, label ~ features, "classification", numTrees = 10)

# Model summary
summary(model)

# Prediction
predictions <- predict(model, test)
head(predictions)

# Fit a GBT classification model with spark.gbt
model <- spark.gbt(training, label ~ features, "classification", maxIter = 10)

# Model summary
summary(model)

# Prediction
predictions <- predict(model, test)
head(predictions)

### Multilayer perceptron classifier ###
# Load training data
df <- read.df("/opt/spark-3.1.1-bin-hadoop3.2/data/mllib/sample_multiclass_classification_data.txt", source = "libsvm")
training <- df
test <- df

SparkR::showDF(df$features, truncate = FALSE)