import tensorwatchext as tw
# from matplotlib.widgets import Slider, Button, RadioButtons
from ipywidget import *

class twapi:
    def __init__(self,topic):
        self.topic=topic
        self.client = tw.WatcherClient()
    
    def stream(self,expr):
        self.expr=expr
        self.streamdata = self.client.create_stream(expr=self.expr)

    def updateFunc(self,x):
        line_plotx = tw.Visualizer(self.streamdata , vis_type='line',window_width=x)#,yrange=(0,1)),window_width=10#,Date=True
        line_plotx.show()
        return

    def draw(self):
        interact(updateFunc, x=10);
        line_plotx = tw.Visualizer(self.streamdata , vis_type='line',window_width=50)#,yrange=(0,1)),window_width=10#,Date=True
        line_plotx.show()

    # class connector:
    #     def __init__(self,topic):
    #         self.topic=topic  
        
    def connector(self,topic,host,parsetype,cluster_size,type="kafka"):
        self.type=type
        self.topic=topic
        self.host=host
        self.cluster_size=cluster_size
        self.parsetype=parsetype
        if self.type=="kafka":
            tw.kafka_connector(topic=self.topic,hosts=self.host,parsetype=parsetype,cluster_size=self.cluster_size)
        elif self.type=="pykafka":
            tw.pykafka_connector(topic=self.topic,hosts=self.host,parsetype=parsetype,cluster_size=self.cluster_size)
        else:
            print("Error wrong connector selected")
        

