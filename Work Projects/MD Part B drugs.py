# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 14:34:36 2018

@author: steve.musiol
"""

import pandas as pd
import numpy as np
from bokeh.models import ColumnDataSource, CustomJS, NumeralTickFormatter, Spacer
from bokeh.plotting import figure, reset_output
from bokeh.layouts import layout, widgetbox, row, column
from bokeh.io import output_file, show, curdoc
from bokeh.transform import jitter

# clear off any historical plots from the document/visualization
curdoc().clear()

# specify where we want the file to go once we're done
output_file(r"insert output file destination")

# load the macular degeneration drug data
md_data = pd.read_csv(r"insert file name", sep='|')

md_data.rename(columns={"CLM_LINE_SRVC_UNIT_QTY":"Billed Units", "CLAIM_PAID_AMT":"CMS $ PAID", "DGNS_CD":"ICD-10", "PROVIDER_NAME":"PROVIDER NAME", "Descr":"ICD Description", "CLM_LINE_FROM_DT":"DOS"}, inplace=True)

# format the ICD-10 code correctly
new_icd = []
for code in md_data['ICD-10'].tolist():
    new_code = code[:3] + '.' + code[3:]
    new_icd.append(new_code)
md_data['ICD-10'] = new_icd

# format the date of service correctly
md_data['DOS'] = pd.to_datetime(md_data['DOS'])
md_data['DOS'] = md_data['DOS'].dt.strftime('%m-%d-%Y')
    
#create color map for the different drugs
colormap = {'Injection, aflibercept, 1 mg':'red', 'Injection, bevacizumab, 10 mg':'green', 'Injection, ranibizumab, 0.1 mg':'blue', 'Injection, verteporfin, 0.1 mg':'orange'}
colors = [colormap[x] for x in md_data['CODE_DESCRIPTION']]
md_data['colors'] = colors


#source = ColumnDataSource(md_data)

TOOLTIPS = [('ICD-10', '@{ICD-10}')
        , ('Description', '@{ICD Description}')
        , ('DOS', '@DOS')
        , ('Provider Name', '@{PROVIDER NAME}')]



#create the first scatter plot and give it a title and label the axes
scatter1 = figure(title='Billed MD Drug Units vs CMS $ Paid', toolbar_location='right', tools=['hover', 'wheel_zoom', 'box_zoom', 'pan', 'reset'], tooltips=TOOLTIPS, plot_width=600, plot_height=600)
scatter1.xaxis.axis_label = 'Billed MD Drug Units'
scatter1.xaxis.axis_label_text_font_size = '12pt'
scatter1.yaxis.axis_label = 'CMS $ Paid'
scatter1.yaxis.axis_label_text_font_size = '12pt'
scatter1.yaxis[0].formatter = NumeralTickFormatter(format='$0')
scatter1.title.align = 'center'
scatter1.title.text_font_size = '20pt'
scatter1.circle(jitter('Billed Units', 0.4), jitter('CMS $ PAID', 0.4), fill_color='colors', line_color=None, size=8, alpha = 0.4, muted_color='colors', muted_alpha=0.2, legend='CODE_DESCRIPTION', source=md_data)

#show(scatter1)



# create right side vertical histogram
cms_paid = md_data['CMS $ PAID'].values # convert DataFrame column to numpy array
vhist, vedges = np.histogram(cms_paid, bins=15)
vzeros = np.zeros(len(vedges)-1)
vmax = max(vhist)*1.1

pv = figure(toolbar_location=None, plot_width=200, plot_height=scatter1.plot_height, x_range=(0, vmax),
y_range=scatter1.y_range, min_border=10, y_axis_location="right")
pv.yaxis[0].formatter = NumeralTickFormatter(format='$0')
pv.xaxis.major_label_orientation = 0.7854


pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vhist, color="blue", line_color=None)

#show(row(scatter1, pv))

# create bottom horizontal histogram
billed = md_data['Billed Units'].values # convert DataFrame column to numpy array
hhist, hedges = np.histogram(billed, bins=15)
hzeros = np.zeros(len(hedges)-1)
hmax = max(hhist)*1.1

ph = figure(toolbar_location=None, plot_width=scatter1.plot_width, plot_height=200, x_range=scatter1.x_range,
y_range=(0, hmax), min_border=10, min_border_left=50, y_axis_location="right")
ph.yaxis[0].formatter = NumeralTickFormatter(format='$0')

ph.quad(bottom=0, left=hedges[:-1], right=hedges[1:], top=hhist, color="blue", line_color=None)

# lay out the plots as we would like
layout = column(row(scatter1, pv), row(ph, Spacer(width=200, height=200)))
#curdoc().add_root(layout)
show(layout)