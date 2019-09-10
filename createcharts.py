import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as offline
from selenium import webdriver
import os, xlrd

ES_1 = 1
ES_3 = 2
ES_6 = 3
MONGO_1 = 4
MONGO_3 = 5
MONGO_6 = 6
MSSQL = 7
MARIA = 8

def save_png(imagepath, imagename, figure, width, height):
    offline.plot(figure, image='svg', auto_open=False,
                 image_width=width, image_height=height)
    driver = webdriver.PhantomJS(executable_path="./tools/phantomjs")
    driver.set_window_size(width, height)
    driver.get('temp-plot.html')
    os.system("mkdir -p " + imagepath)
    driver.save_screenshot(imagepath + imagename)
    os.system("rm -rf temp-plot.html")

def dbscomparefigure(databases, values_names, values, x_title, y_title, chart_title):
    dt = []
    for i in range(0, len(values)):
        dt += [go.Bar(name=values_names[i], x=databases,
                      y=values[i], text=values[i], textposition='auto')]
    figure = go.Figure(data=dt)
    figure.update_xaxes(title_text="<b>%s</b>" % x_title)
    figure.update_yaxes(title_text="<b>%s</b>" % y_title)
    figure.update_layout(title_text="<b>%s</b>" % chart_title)
    figure.update_layout(barmode='group')
    return figure

wb = xlrd.open_workbook("lastSundayResult/summary.xlsx") 
sheet = wb.sheet_by_index(0) 
# cell = float(str(sheet.cell_value(i,j)))

databases = []
single_results = []
and_results = []
or_results = []
phrase_results = []
for i in [ES_1, MSSQL]:
    databases += [str(sheet.cell_value(i, 0))]
    single_results += [float(str(sheet.cell_value(i, 1)))]
    and_results += [float(str(sheet.cell_value(i, 2)))]
    or_results += [float(str(sheet.cell_value(i, 3)))]
    phrase_results += [float(str(sheet.cell_value(i, 4)))]
values = [single_results, or_results, and_results, phrase_results]
names = ['Single', 'OR', 'AND', 'Exact Phrase']

fig = dbscomparefigure(databases, names, values, 'Databases', 'Average Latency (ms)', '')
save_png('./temp/', 'tempimg.png', fig, 1500, 750)