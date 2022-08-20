import tensorwatch as tw
# from matplotlib.widgets import Slider, Button, RadioButtons
from ipywidgets import *

class twapi:
    def __init__(self):
        self.client = tw.WatcherClient()
        return

    def stream(self,expr):
        self.expr=expr
        self.streamdata = self.client.create_stream(expr=expr)
        return self.streamdata

    def updateFunc(self,x,streamdata):
        self.line_plotx = tw.Visualizer(streamdata , vis_type='line',window_width=x)#,yrange=(0,1)),window_width=10#,Date=True
        return self.line_plotx

    def draw(self,streamdata):
        x=10
        # interact(twapi.updateFunc(self,x,streamdata), x=10);
        self.line_plotx = tw.Visualizer(streamdata , vis_type='line',window_width=50)#,yrange=(0,1)),window_width=10#,Date=True
        print(type(self.line_plotx))
        return self.line_plotx

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
        

