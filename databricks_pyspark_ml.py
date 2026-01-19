# 1. Import Spark XGBoost Regressor
from xgboost.spark import SparkXGBRegressor
# 2. Load dataset
df = spark.read.csv(
    "/databricks-datasets/bikeSharing/data-001/hour.csv",
    header="true",
    inferSchema="true"
)
df.display()
# 3. Drop unused columns
df = (
    df.drop("instant")
      .drop("dteday")
      .drop("casual")
      .drop("registered")
)

display(df)
# 4. Train-test split
train, test = df.randomSplit([0.7, 0.3], seed=0)
# 5. Feature engineering
from pyspark.ml.feature import VectorAssembler, VectorIndexer

featuresCols = df.columns
featuresCols.remove("cnt")  # label column

vectorAssembler = VectorAssembler(
    inputCols=featuresCols,
    outputCol="rawFeatures"
)
# 6. Vector Indexer
vectorIndexer = VectorIndexer(
    inputCol="rawFeatures",
    outputCol="features",
    maxCategories=4
)
# 7. XGBoost Regressor
from xgboost.spark import SparkXGBRegressor

xgb_regressor = SparkXGBRegressor(
    num_workers=sc.defaultParallelism,
    label_col="cnt",
    missing=0.0
)

# 8. Hyperparameter tuning with CrossValidator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

paramGrid = (
    ParamGridBuilder()
        .addGrid(xgb_regressor.max_depth, [2, 5])
        .addGrid(xgb_regressor.n_estimators, [10, 15])
        .build()
)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol=xgb_regressor.getLabelCol(),
    predictionCol=xgb_regressor.getPredictionCol()
)

cv = CrossValidator(
    estimator=xgb_regressor,
    evaluator=evaluator,
    estimatorParamMaps=paramGrid
)
# 9. Build ML Pipeline
from pyspark.ml import Pipeline

pipeline = Pipeline(
    stages=[vectorAssembler, vectorIndexer, cv]
)
# 10. Train pipeline
pipelineModel = pipeline.fit(train)

# 11. Generate predictions
predictions = pipelineModel.transform(test)
# 12. Display predictions
display(
    predictions.select("cnt", "prediction", *featuresCols)
)
# 13. Evaluate RMSE
rmse = evaluator.evaluate(predictions)
print("RMSE on our test set: %g" % rmse)







