# Logistic regression example to predict a categorical value if a car has a V engine
# is very important that java version in operating system is the same as indicated here
Sys.setenv(JAVA_HOME="/usr/lib/jvm/jdk1.8.0_241")
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/spark-3.1.1-bin-hadoop3.2")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
ss <- sparkR.session(master = "spark://dbastidas-workstation:7077", enableHiveSupport = FALSE,
                     sparkConfig = list(spark.driver.memory = "2g"))

mtcars2 <- mtcars
mtcars2$vs <- as.factor(mtcars2$vs)
df <- SparkR::createDataFrame(mtcars2)
df <- SparkR::withColumn(df, "vs", SparkR::cast(df$vs, "integer"))
SparkR::head(df)
# data frame count
SparkR::nrow(df)

# training data
train <- SparkR::sample(df, withReplacement = FALSE, fraction = 0.7, seed = 42)

# test data
test <- SparkR::except(df, train)

# This way to split the training and test data is better because it returns values according to the percentages
splitData <- SparkR::randomSplit(df, c(0.7,0.3), 42) # the last argument is the seed
train <- splitData[[1]]
test <- splitData[[2]]

# before creating the model I should create some graphics to see if there is a correlation between the variables
# involved in the prediction -> read the file multivariate-data-visualization
# look for correlations in the candidate variables
#corrVsMpg <-  # 0.6640389
#corrVsDisp <-  # -0.7104159
#corrVsHp <-  # -0.7230967
#corrVsWt <-  # -0.5549157
correlations <- c(SparkR::corr(df, "vs","vs"), 
                  SparkR::corr(df, "vs","mpg"), 
                  SparkR::corr(df, "vs","disp"), 
                  SparkR::corr(df, "vs","hp"), 
                  SparkR::corr(df, "vs","wt"), 
                  SparkR::corr(df, "mpg","vs"),
                  SparkR::corr(df, "mpg","mpg"),
                  SparkR::corr(df, "mpg","disp"),
                  SparkR::corr(df, "mpg","hp"),
                  SparkR::corr(df, "mpg","wt"),
                  SparkR::corr(df, "disp","vs"),
                  SparkR::corr(df, "disp","mpg"),
                  SparkR::corr(df, "disp","disp"),
                  SparkR::corr(df, "disp","hp"),
                  SparkR::corr(df, "disp","wt"),
                  SparkR::corr(df, "hp","vs"),
                  SparkR::corr(df, "hp","mpg"),
                  SparkR::corr(df, "hp","disp"),
                  SparkR::corr(df, "hp","hp"),
                  SparkR::corr(df, "hp","wt"),
                  SparkR::corr(df, "wt","vs"),
                  SparkR::corr(df, "wt","mpg"),
                  SparkR::corr(df, "wt","disp"),
                  SparkR::corr(df, "wt","hp"),
                  SparkR::corr(df, "wt","wt"))
correlations <- round(correlations, 2)

correlogram <- matrix(correlations, nrow=5, ncol=5,
          dimnames=list(c("vs","mpg","disp","hp","wt"), c("vs","mpg","disp","hp","wt")),
          byrow=FALSE)
correlogram

library(MASS)
library(lattice)
levelplot(x = correlogram, at = seq(-1, 1, 0.1))

# It seems is not possible to create a correlation matrix using SparkR, so in case you have to do it, you can use
# a native R data frame with a subset of the whole data and apply the following commands
#correlations <- cor(mtcars[,c(1,3,4,6,8)])
#round(correlations, 2)
#library(corrgram)
#corrgram(mtcars)

# create the model (Binomial logistic regression for binary classification [vs]) 
model <- spark.logit(train, vs ~ mpg + disp + hp + wt, maxIter = 10, regParam = 0.3, elasticNetParam = 0.8)
summary(model)

# interpret model results: for every unit increase in the mpg value the log odds of the vs increase by 0.013. 
# For every unit increase in disp the log odds of vs decrease by 0.0006
# $coefficients
# Estimate
# (Intercept) -0.4759123305
# mpg          0.0138605245
# disp        -0.0006947179
# hp          -0.0020940328
# wt           0.0000000000

# visualize the multivariate graphic
# predict values with the model
predictions <- predict(model, test)
head(predictions)