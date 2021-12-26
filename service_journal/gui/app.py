from tkinter import Frame, Button, Checkbutton, Text, Label, LabelFrame
from ..classifications.processors import DEFAULT_PROCESSOR_TYPES

# Author's note: I wish tkinter would update their parameters to have less problematic names.

class App(Frame):
    def __init__(self, master = None):
        super().__init__(master)
        self.pack()
        self.create_widgets()
    
    def create_widgets(self):
        self.headline = Label(self, text = 'TCAT Data Analysis Tool')
        self.headline.pack(side='top')

        self.date_range = LabelFrame(self, text='Date Range')
        
        self.start_date_label = Label(self.date_range, text='Start date:')
        self.start_date_label.pack(side='left')
        self.start_date_entry = Text(self.date_range, width=10, height=1)
        self.start_date_entry.pack(side='left')

        self.end_date_label = Label(self.date_range, text='End date:')
        self.end_date_label.pack(side='left')
        self.end_date_entry = Text(self.date_range, width=10, height=1)
        self.end_date_entry.pack(side='left')

        self.date_range.pack(side='top')

        self.processes = LabelFrame(self, text='Processes')

        for type_ in DEFAULT_PROCESSOR_TYPES:
            setattr(self, f'processor_{type_}', Checkbutton(self.processes, text=type_))
            getattr(self, f'processor_{type_}').pack(side='left')

        self.processes.pack(side='top')

        self.submit = Button(self, text='Start Processing', command=self.submit)
        self.submit.pack(side='top')

    def submit(self):
        print('Submitted!')
