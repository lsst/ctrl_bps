
# genWorkflow = networkX graph
# bipartite:  nodeType = 0 (file), nodeType = 1 (executable)
# executable node
#    execName
#    execArgs (opt)
#    requestCpus (opt)
#    requestMemory (opt)
# file node
#    lfn
#    ignore = True/False
#    data_type = "science"


class workflowEngine(object):
    def __init__(config):
        pass
    def implementWorkflow(genWorkflow):
        return 
    def submit(self):
        pass
    def statusID(self, id):
        pass
    def statusAll(self):
        pass
    def remove():
        pass
    #future def history()
    #future def pause()
    #future def continue()
    #future def edit()
    #future def setRestartPoint()
    #future def restart()

class workflow(object):
    def __init__(config, genWorkflow):
        pass
    def implementWorkflow(genWorkflow):
        pass
    def getId():
        pass
