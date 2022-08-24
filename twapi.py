from pyexpat.errors import XML_ERROR_NOT_STANDALONE
import tensorwatch as tw
# from matplotlib.widgets import Slider, Button, RadioButtons
from ipywidgets import *
from ipywidgets import widgets

class twapi:

    my_slider = widgets.IntSlider(
        value=10,
        min=1,
        max=100,
        step=1,
        description='My Slider:',
        disabled=False,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='d'
    )
    def __init__(self):
        self.client = tw.WatcherClient()
        return

    def stream(self,expr):
        self.expr=expr
        self.streamdata = self.client.create_stream(expr=expr)
        return self.streamdata

    def updateFunc(self,num):
        # print(x)
        self.line_plotx = tw.Visualizer(self.streamdata , vis_type='line',window_width=num)#,yrange=(0,1)),window_width=10#,Date=True
        # print(type(x))
        # print(x)
        return self.line_plotx
        # return

    def draw(self):
        x=10
        # self.temp=interact(self.updateFunc(x), x=10);
        self.widget=widgets.interact(self.updateFunc, num = self.my_slider)
        # print(type(self.temp))
        # print(self.temp)
        # self.line_plotx = tw.Visualizer(self.streamdata , vis_type='line',window_width=50)#,yrange=(0,1)),window_width=10#,Date=True
        print(type(self.line_plotx))
        return self.line_plotx,self.widget

    # class connector:
    #     def __init__(self,topic):
    #         self.topic=topic  
    # def connector(topic,host,parsetype="json",cluster_size=1,type="kafka"):
    #     twapi.connector2(twapi.self,topic,host,parsetype="json",cluster_size=1,type="kafka")
    
    def connector(self,topic,host,parsetype="json",cluster_size=1,type="kafka"):
        self.topic=topic
        # self.client = tw.WatcherClient()
        # self.type=type
        # self.topic=topic
        # self.host=host
        # self.cluster_size=cluster_size
        # self.parsetype=parsetype
        if type=="kafka":
            return tw.kafka_connector(topic=topic,hosts=host,parsetype=parsetype,cluster_size=cluster_size)
        elif type=="pykafka":
            return tw.pykafka_connector(topic=topic,hosts=host,parsetype=parsetype,cluster_size=cluster_size)
        else:
            print("Error wrong connector selected")
        

