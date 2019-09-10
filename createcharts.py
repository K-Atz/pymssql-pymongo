import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as offline
from selenium import webdriver
import os

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

fig = dbscomparefigure(['db1', 'db2'], ['a','b'], [[1,2],[4,5]], 'x', 'y', 'charttitle')
save_png('./temppics/', 'tempimg.png', fig, 1000, 500)