# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 12:59:13 2018

@author: steve.musiol
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from bokeh.models.widgets import Select
from bokeh.models import ColumnDataSource, CustomJS
from bokeh.plotting import figure, reset_output
from bokeh.layouts import layout, widgetbox, row
from bokeh.io import output_file, show, curdoc

# clear off any historical plots from the document/visualization
curdoc().clear()


# specify where we want the file to go once we're done
output_file(r"output file name")

pt_claims = pd.read_csv(r"insert file name", sep="\t")

pt_claims.rename(columns={"CLM_DGNS_1_CD":"PRINCIPAL DIAGNOSIS", "desc":"CODE DESCRIPTION", "TOTAL_PAID":"TOTAL PAID", "UNIQUE_PATIENTS":"UNIQUE PATIENTS"}, inplace=True)

categories = list(pt_claims)

#fig, ax = plt.subplots(1, 3)
#fig=pt_claims.boxplot(column='CLAIMS PER PATIENT')
#fig.set_title('CLAIMS / PATIENT')

# bring the DataFrame to the bokeh data structure
source = ColumnDataSource(pt_claims)

# set up the info to display when hovering over dots on the scatter plot
# {0.2f} gets us the 2 decimal format
TOOLTIPS = [("ICD-10", "@{PRINCIPAL DIAGNOSIS}")
        , ("Description", "@{CODE DESCRIPTION}")
        , ("# Claims", "@CLAIMS")
        , ("Total Paid", "$@{TOTAL PAID}{0.2f}")]

# create the first scatter plot and give it a title and label the axes
scatter1 = figure(title='Avg Claims Dollars vs. Unique Patients', toolbar_location=None, tools='hover', tooltips=TOOLTIPS)
scatter1.circle('AMT PER CLAIM', 'UNIQUE PATIENTS', color='blue', source=source)
scatter1.xaxis.axis_label = '$ per Claim'
scatter1.yaxis.axis_label = 'Unique Patients'
scatter1.title.align = 'center'
#show(scatter1)

# create the second scatter plot and give it a title and label the axes
scatter2 = figure(title='Unique Patients vs. Claims Per Patient', toolbar_location=None, tools='hover', tooltips=TOOLTIPS)
scatter2.circle('UNIQUE PATIENTS', 'CLAIMS PER PATIENT', color='red', source=source)
scatter2.xaxis.axis_label = 'Unique Patients'
scatter2.yaxis.axis_label = 'Claims / Patient'
scatter2.title.align = 'center'
#show(scatter2)

# create the third scatter plot and give it a title and label the axes
scatter3 = figure(title='Avg Claims Dollars vs. Claims Per Patient', toolbar_location=None, tools='hover', tooltips=TOOLTIPS)
scatter3.scatter('AMT PER CLAIM', 'CLAIMS PER PATIENT', color='green', source=source)
scatter3.xaxis.axis_label = '$ per Claim'
scatter3.yaxis.axis_label = 'Claims / Patient'
scatter3.title.align = 'center'
#show(scatter3)


# display all of the plots in a single row
show(row(scatter1, scatter2, scatter3))