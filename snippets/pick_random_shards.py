import numpy as np


options = list(range(100))

options = options[13:]

np.random.seed(1918)

choices = np.random.choice(options, size=13, replace=False)

print(choices)
