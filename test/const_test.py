from numpy.random import randn
import numpy as np
import pandas as pd
s_mi = pd.Series(np.arange(6),
                 index=pd.MultiIndex.from_product([[0, 1], ['a', 'b', 'c']]))
print(s_mi)
print(s_mi.iloc[s_mi.index.isin([(1, 'a'), (2, 'b'), (0, 'c')])])
print(s_mi.iloc[s_mi.index.isin(['a', 'c', 'e'], level=1)])




