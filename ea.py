
from sf import getSFToken, sf_api_call
import pandas as pd

orgdata = getSFToken()
print("after org info");

testfile = "./data.csv"
data = pd.read_csv(testfile)
print(data.head());

