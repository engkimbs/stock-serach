import pandas as pd

df = pd.DataFrame({ 'x':range(30) })
df = df.rolling(10).mean()           # version 0.18.0 syntax
print(df)