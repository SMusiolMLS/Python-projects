# -*- coding: utf-8 -*-
"""
Created on Fri Sep 21 15:45:47 2018

@author: steve.musiol
"""

import json
from difflib import get_close_matches

dict_data = json.load(open('.spyder-py3\data.json'))

def translate(w):
   w = w.lower()
   if w in dict_data:
       return dict_data[w]
   elif w.title() in dict_data:
       return dict_data[w.title()]
   elif w.upper() in dict_data:
       return dict_data[w.upper()]
   elif len(get_close_matches(w, dict_data.keys())) > 0:
       yn = input("Did you mean %s instead? Enter Y if yes, or N if no: " % get_close_matches(w, dict_data.keys())[0])
       if yn.upper() == 'Y':
           return dict_data[get_close_matches(w, dict_data.keys())[0]]
       elif yn.upper() == 'N':
           return "The word doesn't exist. Please double check it."
       else:
           return "We didn't understand your entry."
   else:
       return "The word doesn't exist. Please double check it."

word = input('Enter word: ')

output = translate(word)

if type(output) == list:
    for item in output:
        print(item)
else:
    print(output)

