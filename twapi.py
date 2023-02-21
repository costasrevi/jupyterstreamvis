from pyexpat.errors import XML_ERROR_NOT_STANDALONE
import tensorwatch as tw
# from matplotlib.widgets import Slider, Button, RadioButtons
from IPython.display import clear_output
from ipywidgets import *
from ipywidgets import widgets

class twapi:

    my_slider = widgets.IntSlider(
        value=10,
        min=1,
        max=100,
        step=1,
        description='Window Size : ',
        disabled=False,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='d'
    )
    my_slider2 = widgets.IntSlider(
        value=10,
        min=1,
        max=100,
        step=1,
        description='Window width : ',
        disabled=False,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='d'
    )
    def __init__(self):
        self.out = widgets.Output(layout={})
        self.client = tw.WatcherClient()
        return

    def stream(self,expr):
        self.expr=expr
        self.streamdata = self.client.create_stream(expr=expr)
        # return self.streamdata
        return self

    def updateFunc(self,num,num2):
        self.out.clear_output()
        clear_output(wait=True)
        #if hasattr(self, 'line_plotx'):
        #    self.line_plotx.close()
        self.line_plotx = tw.Visualizer(self.streamdata , vis_type='line',window_width=self.my_slider.value,window_size=self.my_slider2.value)#,yrange=(0,1)),window_width=10#,Date=True
        self.line_plotx.show()
        return

    # def updateFunc2(self,num):
    #     self.out.clear_output()
    #     self.line_plotx = tw.Visualizer(self.streamdata , vis_type='line',window_width=self.my_slider,window_size=num)#,yrange=(0,1)),window_width=10#,Date=True
    #     self.line_plotx.show()
    #     return

    def draw(self):
        # self.out.clear_output()
        #self.line_plotx = tw.Visualizer(self.streamdata , vis_type='line',window_width=self.my_slider.value,window_size=self.my_slider2.value)#,yrange=(0,1)),window_width=10#,Date=True
        widgets.interact(self.updateFunc, num = self.my_slider,num2 = self.my_slider2)
        #self.line_plotx.show()
        #widgets.interact(self.updateFunc, num = self.my_slider2)
        # self.widget2=widgets.interact(self.updateFunc, num = self.my_slider2)
        return


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
        

