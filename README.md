# Spark_FIFA_Dataset_Analysis

**Video Presentation** [Link](https://cmu.box.com/s/7h0r3m6r9b4amh9pumfbej4mgnrnnabt)

**Tung-Yu (Dalen) Hsiao**

**Peer: Kaiyu Guan**

---
## Instructions

**Version**
- Python 3.8
- PySpark 3.5.0
- PyTorch 2.1.0


**Usage**

- **Initialize Spark Session**:

  **Local Machine**
    ```python
  # Spark context access the hardware-level and software-leve configuration 
  # For Spark 2.X
  # Spark Session provides a unified interface for interacting with 
  # different Spark APIs and allows applications to run on a Spark cluster. 
  
  import pyspark
  from pyspark import SparkContext, SQLContext 
  from pyspark.sql import SparkSession
  
  appName = "FIFA_project"
  master = "local"
  
  ### Create Configuration object for Spark.
  # setAppName: set the name of the application 
  # setMaster: set Spark cluster to use, here "local" indicating local machine
  # set("setting configuration", "attribute"): The configuration could be "spark.driver.host" or "spark.executor.memory"
  # and the second entry indicating the corresponding configuration 
  
  conf = pyspark.SparkConf()\
      .set('spark.driver.host','127.0.0.1')\
      .setAppName(appName)\
      .setMaster(master) 
  
  # Create Spark Context with the new configurations rather than relying on the default one
  # SparkContext 
  sc = SparkContext.getOrCreate(conf=conf) # getOrCreate get the current configuration or create a new one
  
  # Linking to SQL API
  # You need to create SQL Context to conduct some database operations like what we will see later.
  # SQLContext
  sqlContext = SQLContext(sc) # Connected to SQL API
  
  # Spark Session 
  spark = SparkSession.builder.master("local[*]").appName(appName).getOrCreate()
  
    ```

  **Cloud**
    ```python
    import pyspark
    from pyspark import SparkContext, SQLContext 
    from pyspark.sql import SparkSession
    
    appName = "FIFA_project"
    
    # Spark Session 
    spark = SparkSession.builder.master("yarn").appName(appName).getOrCreate()
    ```



- **FIFA datasets**:

  The folder `fifa_dataset` contains fifa players datasets spanning years from 2015-2022. To read the data all csv data, try
  ```python
  path = "YourLocalFolderPath"
  file_paths = [path + "/fifa_dataset/players_%d.csv" % n for n in range(15,23)]
  
  # Read each CSV file and add a new column
  dataframes = [spark.read.csv(file_path, header=True, inferSchema=True) for file_path in file_paths]
  ```

- **Postgres Server**:

  ```python
  db_properties={}
  #update your db username
  db_properties['username']="YourUserName"
  #update your db password
  db_properties['password']="YourPassWord"
  #make sure you got the right port number here
  db_properties['url']= "jdbc:postgresql://localhost:5432/postgres"
  #make sure you had the Postgres JAR file in the right location
  db_properties['driver']="org.postgresql.Driver"
  db_properties['table']= "YourTableName"
  db_properties["primary_key_cloumns"] = "id" # id is the primary key in our FIFA table
  
  # Write the DataFrame to PostgreSQL with the specified primary key
  fifa.write.format("jdbc") \
      .mode("overwrite").option("url", db_properties["url"]) \
      .option("dbtable", db_properties["table"]) \
      .option("user", db_properties["username"]) \
      .option("password", db_properties["password"]) \
      .option("driver", db_properties["driver"])\
      .save()
  ```

- **Load Trained Neural Network Models**:

  The best models trained in this project are stored in the folder `trained_models`, to access these models, we have to make sure to create an instance that has the exact configurations as the pretrained model. We have trained two MLP in the later section, with one shallow MLP model `current_best_Shallow Model.pth` named and one deep MLP model named `current_best_DeepModel.pth`.

  The model conifurations are as follow:
  ```
  # Dimensions
  inputDim = XTrain.shape[1] # feauture space
  outpuDim = 1

  # Configurations
  shallow MLP model:
  inputDim=inputDim, outputDim=outputdim, hiddenLayers=[10]

  deep MLP model:
  inputDim=inputDim, outputDim=outputDim, hiddenLayers=[512, 256, 128, 64]
  ```
  
  ```python
  # Model dimensions
  inputDim = "Feature Space"
  outpuDim = "Label Dim"
  hiddenLayers = "Structure of the pre-trained model"
  
  # Load model 
  model_load = Model(inputDim, outputDim, hiddenLayers) # create a NN model instance with predefined configurations
  model_load.load_state_dict(torch.load("YOURMODEL.pth"))
  model_load.eval()
  ```
 


- **Runnning PyTorch on the Cloud**:

  If running Pytorch on the cloud, run the follow command

  ```python
    # To train our nn model, we have to install torch
    !pip install torch
  ```


## Phase I- Data Ingestion & Analysis

In phase I, data ingestion and analysis is performed. The data analyzed in this project is the [FIFA dataset](https://www.kaggle.com/datasets/stefanoleone992/fifa-22-complete-player-dataset) spanning the years 2015 to 2022. Each year's data is stored in an individual CSV table. Before advancing to data analysis, we first ingest all tables into Python using Spark and merge all table altogether into one big table that contains data spanning all years. Then we will store our data into Postgres database for future access. 

### Data Ingestion
**Tasks**
- Build Spark Session
- Build DF Schema
- Write Spark DF to Postgres


The original FIFA dataset contains 109 columns with both numeric and nominal features. Note that at this stage, we created a column with unique id called `id` as the primary key to this new table, this ensures every record can be uniquely identified in the database table. A column `year` is also added to the table as an indicator of the source year of a specific record. Some details descriptions and constraints of every column can be found in the [phaseI_Spark_Data_Analysis](https://github.com/dalenhsiao/Spark_FIFA_Dataset_Analysis/tree/main/phaseI_Spark_Data_Analysis).


### Data Analysis
**Tasks**
- Find the $X$ clubs with the highest number of players with contracts ending in 2023
- List the $Y$ clubs with highest average number of players that are older than 27 years across all years
- Find the most frequent `nation_position` in the dataset for each year

As our data is now available in our Postgres server, we can easily access the data we have organized in the previous step. 

First, we define a function `getXClubWithMostPlayerContractEnd` that takes in the variable $X$(int) as input, the first $X$ number of clubs for which we aim to retrieve information of teams with most players whose contracts are ending in 2023. The function returns a dataframe containing the top $X$ teams. In case of teams having the same record, they will be ranked equally and presented together in the output. 

***Output:***
|         club_name         | count |
|--------------------------|-------|
| En Avant de Guingamp     |   19  |
| Club Atlético Lanús      |   17  |
| Lechia Gdańsk            |   17  |
| Barnsley                 |   16  |
| Kasimpaşa SK             |   16  |
| Bengaluru FC             |   16  |



Then, we define another function `highestAVGPlayerAge` takes in the variable $Y$(int) as input, the first $Y$ number of clubs for which we aim to retrieve information on teams that have most players older than 27.The function returns a dataframe containing the top $Y$ teams. In case of teams having the same record, they will be ranked equally and presented together in the output. 

***Output:***

|            club_name          | avg(count) |
|---------------------------|------------|
| Dorados de Sinaloa        |    19.0    |
| Matsumoto Yamaga FC   |    19.0    |
| Shanghai Shenhua FC   |    18.5    |
| Qingdao FC                      |    18.0    |
| Club Deportivo Jorge Wilstermann |  17.5 |
| Altay SK                          |    17.0    |
| Guaireña FC                    |    17.0    |


Lastly, to determine the most frequent `nation_position` in the dataset for each year, we need to exclude players who are not part of national teams. After removing these records, the remaining entries represent players participating in their respective national teams. We group these records by year and count the players in each position, allowing us to identify the position with the highest number of players for each year.

***Output:***

| Count | Year | Nation Position |
|-------|------|-----------------|
|  564  | 2015 |       SUB       |
|  511  | 2016 |       SUB       |
|  564  | 2017 |       SUB       |
|  600  | 2018 |       SUB       |
|  576  | 2019 |       SUB       |
|  588  | 2020 |       SUB       |
|  588  | 2021 |       SUB       |
|  396  | 2022 |       SUB       |



---

## Phase II(1)- Data Cleaning & Modeling 


In Phase II, our focus is on constructing machine learning models and neural networks based on the FIFA dataset. Before delving into the modeling phase, it is imperative to preprocess the raw data. The processing contains several stages of data cleaning and feature selection to ensures that the data is appropriately formatted and refined, laying a solid foundation for the subsequent stages of our analysis and model development. 

Our analysis aims to predict the overall rating of players based on their attributes, framing the task as a regression problem. Consequently, the entire data preprocessing phase is tailored to suit the requirements of a regression problem.


**Task**
- Feature Selection 
- Data Cleaning
- Data Pipeline

### Feature Selection 

In feature selection, we filtered out features that are uninformative to our analysis such as URL, nouns (players name, clubs name), text descriptions, tags, jerseys number, id, players positions, nationality, clubs loans, preferred foot, weak foot, and work rate. 

```python
drop_cols = ["player_url", "short_name", "long_name", "league_name", "club_loaned_from", "player_face_url", "player_traits", "dob",
             "club_position", "player_tags", "club_logo_url", "club_flag_url", "nation_logo_url", "nation_flag_url", "player_positions", 
             "sofifa_id",  "club_jersey_number", "club_joined", "club_contract_valid_until", "nationality_id", "weak_foot", "id",
             "nation_jersey_number",  "nation_team_id", "real_face", "club_loaned_from", "nation_position", "preferred_foot", "club_team_id",
            "club_name","body_type","nationality_name", "work_rate"
]
```

We also noticed that there are columns contains numbers and mathematical operations, for example `89+1`, `75+3` . These columns are named **sub_numeric** in this project, which requires special operations in later processing to convert them to numeric values. 

```python
sub_numeric = ["ls", "st", "rs", "lw", "lf", "cf", "rf", "rw", "lam", "cam", "ram", "lm", "lcm", "cm", "rcm",
               "rm", "lwb", "ldm", "cdm", "rdm", "rwb", "lb", "lcb", "cb", "rcb", "rb", "gk"
]
```

The remaining columns consist of information deemed relevant for our analysis, encompassing both numeric and nominal columns. The nominal columns are:

```python
nominal_cols = [
      "league_level", "year"
]
```
These columns will undergo one-hot encoding later in the stages of the data pipeline.

The numeric columns can simply be extracted by: 

```python
numeric_col = np.setdiff1d(np.array(df_imputed.columns), np.array(nominal_cols))
```

### Data Cleaning

In this stage, we handled the None values, outliers, find correlations, feature type casting, and handling  `sub_numeric` columns. 

- None Value Handle

In handling none values we noticed that there are specific columns having many missing values
                       
|        Attributes         | Count  |
|--------------------------|--------|
|           pace           | 15791  |
|         shooting         | 15791  |
|         passing          | 15791  |
|        dribbling         | 15791  |
|        defending         | 15791  |
|          physic          | 15791  |
| release_clause_eur      | 55582  |
| mentality_composure     | 31778  |
| goalkeeping_speed       | 126288 |
|value_eur                   | 1897 |  
| wage_eur                    | 1622 |
|league_level                | 2015  | 

By further investigation, we found that goalkeepers lack attributes in pace, shooting, passsing, dribbling, defending, and physics. On the contrary, players other than goalkeeper lack attributes in goalkeeping _speed. Here, we are dropping goalkeeping _speed, since this attribute is possess by goalkeepers and there are other important features that also contribute to the overall rate for goalkeepers. 

For the columns `pace, shooting, passing, dribbling, defending, physic, release_clause_eur, mentality_composure, value_eur, and wage_eur`, we imputed missing values using medians. We made the assumption that goalkeepers perform median on attributes such as pace, shooting, passing, dribbling, defending, and physic. Columns like release_clause_eur, mentality_composure, value_eur, and wage_eur are features that only started to appear in the last few years. For the missing values in these columns, we also opted for median imputation.

We treated the 'league_level' column as a nominal feature and opted to drop records with NaN values in this column instead of imputing values.

- Data Type Casting
  
Upon observation, we noted that all numeric data in the FIFA dataset are integers. To optimize database storage, we have casted the numeric columns to LongType(). During data computations and modeling in the later phase, we then convert the data type to float.

- Finding Outliers
  
The basic philosophy in finding ourliers is based on the following:

  upper limit: $Q3 + 1.5 IQR$
  
  lower limit: $Q1 - 1.5 IQR$ (where $IQR = Q3 - Q1$)

  Data points outside of this range will be considered outliers, and records with more than 25 atrributes that are outliers will be removed. 


- Finding Correlation
  
The threshold for high correlation in this project is set to correlation over 0.9, and within the carrelation pair, the feature that is less correlated with the target variable (i.e. `overall`) is priortized in column dropping.

Dropped columns:
```python 
col_corr_drop = ['lam', 'cam', 'ram', 'cf', 'lf', 'rf', 'lm', 'rm', 'lw', 'rw', 'ls', 'rs', 'st', 'lcm', 'cm', 'rcm', 
                 'skill_dribbling', 'lb', 'rb', 'lwb', 'rwb', 'ldm', 'cdm', 'rdm', 'rcb', 'lcb', 'defending_sliding_tackle', 
                 'defending_standing_tackle', 'defending_marking_awareness', 'mentality_interceptions', 'goalkeeping_reflexes', 
                 'goalkeeping_diving', 'goalkeeping_positioning', 'goalkeeping_handling', 'goalkeeping_kicking', 'movement_acceleration']
```


### Data Pipeline

**Stages**
- `FeatureTypeCaster`: this stage will cast the feature columns as appropriate types
- `StringIndexer`: this stage converts nominal cloumns into numeric vaules
- `OneHotEncoder`: this stage performs one hot encoding on the "StringIndexed" columns
- `VectorAssembler`: this stage vectorizes the onehot encoded features and numeric features altogether to a large vector
- `StandardScalar`: this stage scales the data by standard scaling
- `OutcomeCreater`: this stage casts the output column (ground truth label) as appropriate types
- `ColumnDropper`: this stage drops redundant columns generated during the pipeline

Final Output DF Schema

```
 |-- overall: long (nullable = true)
 |-- features: vector (nullable = true)
```
---

## Phase II(2)- Machine Learning & Neural Network Modeling 

In this phase, we are modeling our processed data using 2 machine learning models and 2 neural newtwork (1 shallow MLP and 1 deep MLP).

- **SparkML Machine Learning**
  
    The  machine learning models of our choice are ***Linear Regression Model(LR)*** and ***Decision Tree Regressor(DT)***.  LR is a straightforward model that serves as a baseline for benchmarking regression results and validating our data cleaning process. The other machine learning model, originally, we attempted to apply a more sophisticated Random Forest Regressor (RT), since the nature DT structure can easily filter the significance of the selected features. Yet, RT is too computationally expensive to run on local machines (even on the cloud) that we eventually resort to a more simple DT regressor. 


- **Pytorch Neural Network**
  
  The neural network (NN) employed in this project is implemented as a simple Multi-Layer Perceptron (MLP) using PyTorch. The two models applied here are a shallow MLP with only 1 hidden layer containing 10 neurons, and a deep MLP with 4 hidden layers having widths of [512, 256, 128, 64]. The rationale behind this NN modeling approach is similar to machine leanring model analysis, where the shallow MLP serves as a baseline model for benchmarking regression results, and the deep MLP illustrates the performance of MLP on the regression problem, particularly after appropriate hyperparameter tuning.

- **Metrics**
  
  The metric we used to train and evaluate all models is ***Mean Squared Error(MSE)***, since our problem is an regression problem, MSE is a proper metric to evaluate the performance of our model. The lower the MSE, the closer the forecast is to actual. 
  
  In addition to MSE, we also evaluate our model performance with ***R Square(R2)*** and ***Parity Plot***. R2 is the proportion of the variance in the ground truth output that is predictable from input features using an regression model. This can be computed by $R^2= 1- \frac{S_{res}}{S_{tot}}$, where $S_{res} = \sum_{} (y_i-y_m)^2$ is the ground truth ($y_i$) total sum of squares relative to ground truth mean ($y_m$); $S_{tot} = \sum_{} (y_i-\hat{y})^2$ is the ground truth ($y_i$) total sum of squares relative to model prediction ($\hat{y}$). A parity plot is a scatterplot that compares a set of results from a computational model (Model Predictions) against benchmark data (Label), which is a good way to visualize R2 score. 



### SparkML


**Task**
- Modeling (Linear Regression and Decision Tree)
- Parameter Grids
- Evaluation

**Utilities**

```python
from pyspark.ml.regression import LinearRegression # Linear regressor
from pyspark.ml.regression import DecisionTreeRegressor # Decision Tree regressor
from pyspark.ml.evaluation import RegressionEvaluator # regression models evaluation 
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator # parameter grids and crossvalidation 
```

- **Linear Regression**

  In the linear regressor, the identified hyperparameters are `maxIter` (maximum number of iterations), `regParam` (L2 regularization coefficient), and `elasticNet` (the L1 and L2 ratio). We obtained optimal hyperparameters by implementing parametric grids and cross-validating the model with MSE as the metric. The optimal hyperparameters obtained are:

 |        Hyperparameter         | Value |
|--------------------------|--------|
| Best Param (regParam)                                                    | 0.01               |
| Best Param (MaxIter)                                                     | 1                  |
| Best Param (elasticNetParam)                                             | 0.0                |
| Mean Squared Error (MSE) of the best Linear Regression Model on test data | 3.291979091287333 |

- **Decision Tree**

  In decision tree regressor, the identified hyperparameters are `maxBins` (the discretization of each numeric feature) and `max_depth` (the depth of decision tree model). Here, parametric grids and cross-validation is performed again with MSE as the metric. The optimal hyperparameters obtained are:

|        Hyperparameter         | Value |
|--------------------------|--------|
| Best Param (maxDepth) | 20                               |
| Best Param (maxBins)   | 30                               |
| Mean Squared Error (MSE) of the best Decision Tree model on test data | 0.5538861113811901 |





### Neural Network

**Task**
- Modeling (Shallow and Deep MLP)
- Hyperparameter Tuning 
- Evaluation

**Utilities**
- `Train_log`: save training log
  
  ```python
  class Train_Log:
      def __init__(self):
  
          self.ovrLoss = [] 
          self.batchLoss = []
          self.ovrValLoss = [] 
  
      def log_update_train(self, loss):
  
          self.batchLoss.append(loss)
  
      def log_update(self, trainLoss, validLoss):
          self.ovrLoss.append(trainLoss)
          self.ovrValLoss.append(validLoss)
  
  ```

- `plot_loss`: plot the training process from `Train_log`
  
  ```python
  def plotLoss(logger, epoch, bestEpoch, title=None):

    fig, ax = plt.subplots(2, 1, figsize=(12, 8))

    # plot loss for all batches
    ax[0].plot(logger.batchLoss, label= "Training Loss")
    ax[0].set_title("Batch Loss")
    ax[0].set_xlabel("Iterations")
    ax[0].set_ylabel("Loss")
    ax[0].legend()

    # plot loss of all epochs
    ax[1].plot(logger.ovrLoss, label = "Training Loss")
    ax[1].plot(logger.ovrValLoss, label = "Validation Loss")
    # if earlyStop:
    ax[1].axvline(x = bestEpoch , color="#F75D59", linewidth=0.8, linestyle="dashed", label="Early stop")
    ax[1].set_title("Training & Validation Loss")
    ax[1].set_xlabel("Epochs")
    ax[1].set_ylabel("Loss")
    ax[1].legend()
    
    if title:
        plt.suptitle(title)
    plt.tight_layout()  # Adjust spacing between subplots
    plt.show()
  ```

- `plot_r2`: visualize R square
  
  ```python
  def plot_r2(prediction, label, title = None):
    r2 = r2_score(label, prediction)
    combined_array = np.concatenate([prediction, label])
    lb = np.min(combined_array)
    ub = np.max(combined_array)

    plt.scatter(label, prediction)
    plt.plot(np.arange(lb-10, ub+10), np.arange(lb-10, ub+10), "r--")
    plt.xlabel("Label")
    plt.ylabel("Prediction")

    plt.xlim((lb-5, ub+5))
    plt.ylim((lb-5, ub+5))
    plt.text((lb+ub)/2,(lb+ub)/2,"R2= %.2f" % r2, weight= "bold")
    if title:
        plt.title(title)
    plt.show()
  ```

- **Modeling (Shallow and Deep MLP)**




- **Hyperparameters**

  The selected hyperparameters are all closely related to the convergence of NN model, we selected the most important few:
  
  - **Early Stopping Steps**:
  
    Rather than tuning the maximum number of epochs, we opted to tune the early stopping step. Early stopping is defined as the number of epochs the training process will continue if there is no improvement within a given tolerance. Tuning early stopping allows us to better track model convergence. The early stopping improvement criterion is based on the validation loss. We tested three values for early stopping steps [10, 20, 50]. Our observation indicated that the model showed improvement within a tolerance of 10 to 20 steps, while a tolerance set above 20 was more likely to lead to overfitting. 

  - **Batch Size**:
  
    Batch size typically is not a tunable hyperparameter due to the limit of memory, yet, in this project, the size of the data and neural network models are not that large. Therefore, we take batch size as one of the tuning hyperparameters which could contributes to the randomness of the training process. Higher batch sizes tend to reduce the influence of randomness during training. In our hyperparameter tuning, we experimented with batch sizes of 64 and 1024. We observed no distinct difference in terms of validation loss between the two. However, a batch size of 64 proved to be more computationally expensive within each epoch due to a larger number of batches trained. On the other hand, a batch size of 1024 trained faster within an epoch but required more epochs for the training to converge.
    
  - **Learning Rate**:
  
    The learning rate plays a crucial role in model convergence. A larger learning rate allows for more substantial steps in training but poses the risk of overshooting the optimal solution. Conversely, a smaller learning rate reduces the risk of overshooting but makes it harder to determine the timing of convergence. After considering factors such as batch size, early stopping steps, and time consumption, we determined that a learning rate of 0.01 is a relatively ideal value. This choice strikes a balance, derived a converged result and making accurate predictions without excessive oscillations.


## Phase II(3)- Cloud Computation 
The folder [Cloud_computed](https://github.com/Systems-and-Toolchains-Fall-2023/course-project-option-1-dalenhsiao/tree/main/Cloud_computed) contains the all notebooks for Google Cloud computation. Since we found that Google Cloud cannot handle too heavey compuations, therefore, we segmented the whole procedures to four notebooks

- `Cloud_Phase1.ipynb`: Phase I tasks
- `Cloud_Phase2_ml_dt_model.ipynb`: Phase II decisition tree
- `Cloud_Phase2_ml_lr_model.ipynb`: Phase II linear regression
- `Cloud_Phase2_nn_deep_model.ipynb`: Phase II deep MLP model
- `Cloud_Phase2_nn_shallow_model.ipynb`: Phase II shallow MLP model

In addition, we only utilized 20% of cleaned FIFA dataset for clound computation. And within this 20% of the data, we then split the data for training and testing. 


