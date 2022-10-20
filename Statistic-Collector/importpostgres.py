import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv('cohesity.csv')
print('Write to postgres')
engine = create_engine('postgresql://postgres:postgres@app2.pve:5432/postgres')
df.to_sql('cohesity', engine)