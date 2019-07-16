# -*- coding: utf-8 -*-
"""
Created on Wed May 22 12:26:04 2019

@author: steve.musiol
"""
import numpy as np

num = 600851475143

i = 1
factors = []

while i < num:
    if num%i == 0:
        factors.append(i)
        
print(max(factors))