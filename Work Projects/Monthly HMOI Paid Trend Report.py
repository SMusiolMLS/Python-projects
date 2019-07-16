#!/usr/bin/env python
# coding: utf-8

'''
ROC-02058
Author: Steve Musiol

Purpose: Create an automated monthly claims trend report for sites 197 and 488
that breaks things down into specific categories based on logic previously
supplied by Amber Kariolich, which is heavily based off of the Weekly Claims
Prepayment audit report. Original query was designed in ROC-1716 and then
modified for this report.
'''


import pandas as pd
import pyodbc
import datetime
import glob
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from bokeh.models import NumeralTickFormatter, Range1d
from bokeh.plotting import figure
from bokeh.layouts import layout, widgetbox, row, column, gridplot
from bokeh.io import output_file, curdoc, save
from bokeh.models.widgets import Panel, Tabs
from bokeh.models.annotations import Legend

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


# open the connection to the database
cnn_str = "Driver={ODBC Driver 13 for SQL Server};Server=insert server name;Database=insert db name;Uid=insert user name;Pwd=insert password"
conn = pyodbc.connect(cnn_str)

# pull the query for results retrieval into a text file
f = open(r"insert sql query file path", 'r')
query = f.read()
f.close()

# run the query and put the results into a pandas DataFrame
monthly_data = pd.read_sql(query, conn)

# close the database connection
conn.close()

# set the 'WEEK' to datetime and make it the index
pd.to_datetime(monthly_data['WEEK'])
monthly_data.set_index('WEEK')

# split the data by underwriter
data_197 = monthly_data[monthly_data['UNDWTR'] == '197']
data_488 = monthly_data[monthly_data['UNDWTR'] == '488']

# set the index to be time indexed for both subsets
data_197.set_index('WEEK')
pd.to_datetime(data_197['WEEK'])

data_488.set_index('WEEK')
pd.to_datetime(data_488['WEEK'])

categories = ['TOTAL_FFS_SPEC', 'TOTAL_CAP_SPEC',  'TOTAL_OON', 'TOTAL_PCP', 'TOTAL_OB', 'TOTAL_TERTIARY']

# calculate 90 day moving average for each column in both underwriter data sets and add to dataframe
for item in categories:
    # window of 13 gives us (7*13)= 91 days for moving average
    r90_197 = data_197[item].rolling(13).mean()
    col_name = item[6:] + ' 90 Day MA'
    # add the new moving average column to the DataFrame
    data_197 = data_197.assign(MA_col=r90_197)
    data_197 = data_197.rename(columns={'MA_col':col_name})
   
    # do the same for 488 site data
    r90_488 = data_488[item].rolling(13).mean()
    col_name = item[6:] + ' 90 Day MA'
    data_488 = data_488.assign(MA_col=r90_488)
    data_488 = data_488.rename(columns={'MA_col':col_name})
    
moving_avg_197 = ['FFS_SPEC 90 Day MA', 'TERTIARY 90 Day MA', 'OON 90 Day MA', 
                  'PCP 90 Day MA', 'OB 90 Day MA', 'CAP_SPEC 90 Day MA']

labels_197 = ['FFS Specialties', 'Tertiary', 'OON', 'PCP', 'OB', 'CAP Specialties']

categories_197 = ['TOTAL_FFS_SPEC', 'TOTAL_TERTIARY', 'TOTAL_OON', 'TOTAL_PCP', 'TOTAL_OB', 'TOTAL_CAP_SPEC']

moving_avg_488 = ['FFS_SPEC 90 Day MA', 'CAP_SPEC 90 Day MA', 'OON 90 Day MA',   
                  'TERTIARY 90 Day MA', 'PCP 90 Day MA', 'OB 90 Day MA']

categories_488 = ['TOTAL_FFS_SPEC', 'TOTAL_CAP_SPEC',  'TOTAL_OON', 
                  'TOTAL_TERTIARY', 'TOTAL_PCP', 'TOTAL_OB']

labels_488 = ['FFS Specialties', 'CAP Specialties', 'OON', 'Tertiary', 'PCP', 'OB']

# dictionary of colors for each column to keep consistency across the summary MA graphs
color_map = {'FFS_SPEC 90 Day MA':'blue', 'TERTIARY 90 Day MA':'yellowgreen', 'OON 90 Day MA':'seagreen', 
                  'PCP 90 Day MA':'red', 'OB 90 Day MA':'orange', 'CAP_SPEC 90 Day MA':'gold'}

# clear off any historical plots from the document/visualization
curdoc().clear()

# get the date the report is run to use in the naming convention
report_date = datetime.datetime.now().strftime('%Y%m%d')

# specify where we want the file to go once we're done
output_file(r'\\mc-file01\shared\AB ACO\Sharepoint Reports\Monthly HMO Claims Trend\%s Claims Trend Report.html' % report_date)

# need to create 197 moving average plot then create grid of subplots next to it by category
p_197 = figure(title='197 Weekly Claims 90 Day Moving Average', x_axis_type='datetime')
for i in range(0, len(moving_avg_197)):
    p_197.line(data_197['WEEK'], data_197[moving_avg_197[i]], legend=labels_197[i], line_color=color_map[moving_avg_197[i]], line_width=2.5)

p_197.outline_line_color = None
p_197.title.align = 'center'
p_197.title.text_font_size = '14pt'
p_197.legend.location = 'top_left'
p_197.legend.border_line_color = None
p_197.xaxis.major_tick_line_width = 2.5
p_197.yaxis.major_tick_line_width = 2.5
p_197.xaxis.axis_line_color = None
p_197.yaxis.axis_line_color = None
p_197.yaxis.formatter = NumeralTickFormatter(format="$0,000")
p_197.yaxis.minor_tick_line_color = None
p_197.xgrid.grid_line_color = None
p_197.ygrid.grid_line_color = None
# show(p_197)

# need to get max value from sets of 3 columns to set y-axis scale
row_1_197 = categories_197[:3]
row_2_197 = categories_197[3:]

row_1_488 = categories_488[:3]
row_2_488 = categories_488[3:]


def get_max(col_list, data_frame):
    max_vals = []
    for col in col_list:
        max_vals.append(data_frame[col].max())
    return max(max_vals)


# get the max values for our columns to deploy to our y-axes. Add 10% so axes have buffer over max values
max_1_197 = get_max(row_1_197, data_197) * 1.1
max_2_197 = get_max(row_2_197, data_197) * 1.1

max_1_488 = get_max(row_1_488, data_488) * 1.1
max_2_488 = get_max(row_2_488, data_488) * 1.2

# create the plot for each category and append to list to be used in gridplots()
ma_197_plots = []
row1 = []
row2 = []

for i in range(0, len(categories_197)):
    plot_197 = figure(title=labels_197[i], x_axis_type='datetime')
    p_cat = plot_197.line(data_197['WEEK'], data_197[categories_197[i]], line_color='navy', line_width=2.5, alpha=0.3)
    p_ma = plot_197.line(data_197['WEEK'], data_197[moving_avg_197[i]], line_color='red', line_width=2.5)
    legend = Legend(items=[('Weekly Total', [p_cat]), ('90 Day MA', [p_ma])], location='top_center', background_fill_color=None,
                       border_line_color=None)
    plot_197.xaxis.axis_line_color = None
    plot_197.yaxis.axis_line_color = None
    plot_197.yaxis.minor_tick_line_color = None
    plot_197.title.align = 'center'
    plot_197.title.text_font_size = '12pt'
    plot_197.outline_line_color = None
    plot_197.xgrid.grid_line_color = None
    plot_197.ygrid.grid_line_color = None
    # set up axes for the top left graph
    if i == 0:
        plot_197.yaxis.major_tick_line_width = 2.5
        plot_197.yaxis.major_label_text_font_size = '10pt'
        plot_197.yaxis.formatter = NumeralTickFormatter(format='$0,000')
        plot_197.y_range = Range1d(0, max_1_197)
        plot_197.xaxis.visible = False
        row1.append(plot_197)
    elif i == 1:
        plot_197.y_range = Range1d(0, max_1_197)
        plot_197.xaxis.visible = False
        plot_197.yaxis.visible = False
        legend = Legend(items=[('Weekly Total', [p_cat]), ('90 Day MA', [p_ma])], location='top_center', background_fill_color=None,
                       border_line_color=None)
        plot_197.add_layout(legend)
        row1.append(plot_197)
    elif i == 2:
        plot_197.y_range = Range1d(0, max_1_197)
        plot_197.xaxis.visible = False
        plot_197.yaxis.visible = False
        row1.append(plot_197)
    elif i == 3:
        plot_197.xaxis.major_tick_line_width = 2.5
        plot_197.yaxis.major_tick_line_width = 2.5
        plot_197.yaxis.major_label_text_font_size = '10pt'
        plot_197.xaxis.major_label_text_font_size = '10pt'
        plot_197.xaxis.major_label_orientation = 1.57
        plot_197.yaxis.formatter = NumeralTickFormatter(format='$0,000')
        plot_197.y_range = Range1d(0, max_2_197)
        row2.append(plot_197)
    else:
        plot_197.xaxis.major_label_text_font_size = '10pt'
        plot_197.xaxis.major_tick_line_width = 2.5
        plot_197.xaxis.major_label_orientation = 1.57
        plot_197.yaxis.visible = False
        plot_197.y_range = Range1d(0, max_2_197)
        row2.append(plot_197)

ma_197_plots.append(row1)
ma_197_plots.append(row2)

grid_197 = gridplot(ma_197_plots, plot_width=300, plot_height=300)

# need to create the same for 488
p_488 = figure(title='488 Weekly Claims 90 Day Moving Average', x_axis_type='datetime')
for i in range(0, len(moving_avg_488)):
    p_488.line(data_488['WEEK'], data_488[moving_avg_488[i]], legend=labels_488[i], line_color=color_map[moving_avg_488[i]], line_width=2.5)

p_488.outline_line_color = None
p_488.title.align = 'center'
p_488.title.text_font_size = '14pt'
p_488.legend.location = 'top_right'
p_488.legend.border_line_color = None
p_488.xaxis.major_tick_line_width = 2.5
p_488.yaxis.major_tick_line_width = 2.5
p_488.xaxis.axis_line_color = None
p_488.yaxis.axis_line_color = None
p_488.yaxis.formatter = NumeralTickFormatter(format="$0,000")
p_488.yaxis.minor_tick_line_color = None
p_488.xgrid.grid_line_color = None
p_488.ygrid.grid_line_color = None

# create the plot for each category and append to list to be used in gridplots()
ma_488_plots = []
row3 = []
row4 = []

for i in range(0, len(categories_197)):
    plot_488 = figure(title=labels_488[i], x_axis_type='datetime')
    p_cat = plot_488.line(data_488['WEEK'], data_488[categories_197[i]], line_color='navy', line_width=2.5, alpha=0.3)
    p_ma = plot_488.line(data_488['WEEK'], data_488[moving_avg_197[i]], line_color='red', line_width=2.5)
    plot_488.xaxis.axis_line_color = None
    plot_488.yaxis.axis_line_color = None
    plot_488.yaxis.minor_tick_line_color = None
    plot_488.title.align = 'center'
    plot_488.title.text_font_size = '12pt'
    plot_488.outline_line_color = None
    plot_488.xgrid.grid_line_color = None
    plot_488.ygrid.grid_line_color = None
    # set up axes for the top left graph
    if i == 0:
        plot_488.yaxis.major_tick_line_width = 2.5
        plot_488.yaxis.major_label_text_font_size = '10pt'
        plot_488.yaxis.formatter = NumeralTickFormatter(format='$0,000')
        plot_488.y_range = Range1d(0, max_1_488)
        plot_488.xaxis.visible = False
        row3.append(plot_488)
    elif i == 1:
        plot_488.y_range = Range1d(0, max_1_488)
        plot_488.xaxis.visible = False
        plot_488.yaxis.visible = False
        legend = Legend(items=[('Weekly Total', [p_cat]), ('90 Day MA', [p_ma])], location='top_center', background_fill_color=None,
                       border_line_color=None)
        plot_488.add_layout(legend)
        row3.append(plot_488)
    elif i == 2:
        plot_488.y_range = Range1d(0, max_1_488)
        plot_488.xaxis.visible = False
        plot_488.yaxis.visible = False
        row3.append(plot_488)
    elif i == 3:
        plot_488.xaxis.major_tick_line_width = 2.5
        plot_488.yaxis.major_tick_line_width = 2.5
        plot_488.xaxis.major_label_text_font_size = '10pt'
        plot_488.yaxis.major_label_text_font_size = '10pt'
        plot_488.xaxis.major_label_orientation = 1.57
        plot_488.yaxis.formatter = NumeralTickFormatter(format='$0,000')
        plot_488.y_range = Range1d(0, max_2_488)
        row4.append(plot_488)
    else:
        plot_488.xaxis.major_label_text_font_size = '10pt'
        plot_488.xaxis.major_tick_line_width = 2.5
        plot_488.xaxis.major_label_orientation = 1.57
        plot_488.yaxis.visible = False
        plot_488.y_range = Range1d(0, max_2_488)
        row4.append(plot_488)

ma_488_plots.append(row3)
ma_488_plots.append(row4)

grid_488 = gridplot(ma_488_plots, plot_width=300, plot_height=300)

# try to make these two sets of plots (197 and 488) linked and give it a tabbed format to switch between the two

# create each tab
tab_197_cat = Panel(child=grid_197, title='197 Categorical Trend')
tab_488_cat = Panel(child=grid_488, title='488 Categorical Trend')
tab_197_agg = Panel(child=p_197, title='197 Moving Avg')
tab_488_agg = Panel(child=p_488, title='488 Moving Avg')
tabs = Tabs(tabs=[tab_197_agg, tab_197_cat, tab_488_agg, tab_488_cat])
save(tabs)

# get the newest report to attach to email
file_list = glob.glob(r'insert file directory\*')
newest_report = max(file_list, key=os.path.getctime)
file_name = os.path.basename(newest_report)

# set up the email to send
from_addr = 'insert sending address'

# CHANGE THIS LINE IF YOU NEED TO EDIT THE RECIPIENT LIST FOR THE REPORT EMAIL
to_addr = 'insert recipient email list'

# create the header for the email
msg = MIMEMultipart()
msg['From'] = from_addr
msg['To'] = to_addr
msg['Subject'] = 'Monthly HMO Claims Trend Report'

date = datetime.datetime.now().strftime('%m-%d-%Y')

body = 'Attached is the monthly claims report run on %s. The file can be found at %s' % (date, newest_report)

msg.attach(MIMEText(body, 'plain'))

# get and attach the file to the email
attachment = open(newest_report, 'rb')
part = MIMEBase('application', 'octet-stream')
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', 'attachment; filename= %s' % file_name)

msg.attach(part)

# log on to server, compile and send the email
server = smtplib.SMTP('smtp.office365.com', 587)
server.starttls()
server.login(from_addr, 'INSERT PASSWORD') # COPY AND PASTE EMAIL PASSWORD AS STRING HERE AFTER from_addr
text = msg.as_string()
server.sendmail(from_addr, to_addr, text)
server.quit()