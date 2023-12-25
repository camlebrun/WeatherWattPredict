import pandas as pd

df1 = pd.read_csv('data/data_2010_2021.csv')
df2 = pd.read_csv('data/data_2023.csv')

df = pd.concat([df1, df2], ignore_index=True)
df.to_csv('data/data_2010_2023.csv', index=False)
