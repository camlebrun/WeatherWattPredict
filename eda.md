# EDA

Original dataset for 1950 to 2021 is ≈12Gb and is available in csv format. Due to this large size, I decide to use use PySpark to process the data. 
Due to sevral reasaons (detailed below) of the project I will use data from 2000 to 2021 and only somes features was selected (detailed below).

## Data Selection
* Time range: 2000 to 2021 : 
    * The data from 2000 to 2021 is more relevant because the energy production in France was mainly based on nuclear and renewable energy. Moreover energy demand is more relevant for this period due to the increase of the population and the increase of the number of electrical devices.

* Features selection:
    * Temperature:
    * Humidity:
    * Wind speed:
  
