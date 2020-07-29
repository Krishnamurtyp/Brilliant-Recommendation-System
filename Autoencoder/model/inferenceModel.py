import sys,os
sys.path.append(os.path.join("..","model"))
from BaseModel import BaseModel


class InferenceModel(BaseModel):

    def __init__(self, FLAGS):
        super(InferenceModel, self).__init__(FLAGS)
        self._init_parameters()