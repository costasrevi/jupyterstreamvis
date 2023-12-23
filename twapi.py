from pyexpat.errors import XML_ERROR_NOT_STANDALONE
import tensorwatch as tw
from IPython.display import display
from ipywidgets import *
from ipywidgets import widgets


class twapi:

    default_value=10

    my_slider = widgets.IntSlider(
        value=default_value,
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

    button = widgets.Button(
        description='Reset',
        disabled=False,
        button_style='', # 'success', 'info', 'warning', 'danger' or ''
        tooltip='Click me to reset the stream',
        icon='check' # (FontAwesome names without the `fa-` prefix)
    )

    tab = widgets.Tab()
    

    my_slider2 = widgets.IntSlider(
        value=default_value,
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

    datebutton = widgets.Checkbox(
        value=False,
        description='Date',
        disabled=False
    )

    offsetbutton = widgets.Checkbox(
        value=False,
        description='useOffset',
        disabled=False
    )

    dimhistorybutton = widgets.Checkbox(
        value=True,
        description='dim_history',
        disabled=False
    )

    colorpicker = widgets.ColorPicker(
        concise=False,
        description='Pick a color',
        value='blue',
        disabled=False
    )

    def __init__(self):
        self.out = widgets.Output(layout={})
        self.tab.children  = [self.my_slider,self.my_slider2,self.button,self.datebutton,self.offsetbutton,self.dimhistorybutton,self.colorpicker]  
        titles = ['Window Size','Window Width','Reset','DateFormat','OffSet','dimHistory','colorpicker']    
        for i in range(len(titles)):
            self.tab.set_title(i, titles[i])
        self.client = tw.WatcherClient()
        # self.tab.set_title(0,"Window_Size2")
        # self.default_value=self.default_value
        return

    def stream(self,expr):
        self.expr=expr
        self.streamdata = self.client.create_stream(expr=expr)
        return self

    def updateFunc(self,num,num2,datebutton,offsetbutton,colorpicker):
        self.out.clear_output()
        # self.button.on_click(self.reset)
        self.line_plotx  = tw.Visualizer(self.streamdata , vis_type='line',window_width=self.my_slider.value,window_size=self.my_slider2.value,Date=self.datebutton.value,useOffset=self.offsetbutton.value,dim_history=self.dimhistorybutton.value,color=self.colorpicker.value)#,yrange=(0,1)),window_width=10#,Date=True
        self.line_plotx.show()
        return None

    def reset(self,d):
        self.my_slider.value=self.default_value
        self.my_slider2.value=self.default_value
        self.datebutton.value=False
        self.offsetbutton.value=False
        self.dimhistorybutton.value=True
        self.colorpicker.value='blue'
        self.out.clear_output()
        return

    def draw(self):
        widgets.interact(self.updateFunc, num = self.my_slider,num2 = self.my_slider2,datebutton=self.datebutton,offsetbutton=self.offsetbutton,dimhistorybutton=self.dimhistorybutton,colorpicker=self.colorpicker)
        display(self.button, self.out,self.tab)
        # widgets.interact(self.button.on_click(self.reset))
        # self.button.on_click(self.reset)
        return

    def connector(self,topic,host,parsetype="json",cluster_size=1,type="kafka"):
        self.topic=topic
        if type=="kafka":
            return tw.kafka_connector(topic=topic,hosts=host,parsetype=parsetype,cluster_size=cluster_size)
        elif type=="pykafka":
            return tw.pykafka_connector(topic=topic,hosts=host,parsetype=parsetype,cluster_size=cluster_size)
        else:
            print("Error wrong connector selected")
        