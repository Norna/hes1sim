import socket
from io import StringIO
from subprocess import Popen, PIPE
from molnframework.core.service.base import ServiceBase

def _execute(command):
	process = Popen(command, stdout=PIPE,stderr=PIPE,shell=True)
	output, error = process.communicate()
	retcode = process.poll()

	return (retcode,output,error)

class Hes1Wrapper (ServiceBase):

    input = ""

    # service configuration
    
    parameters = ['input']
    is_single_instance = True
    address='hes1'

    def execute(self):

        host = "%s" % (socket.gethostbyname(socket.gethostname()))
        
        output = ""
        try:
            retcode,output,error = _execute("%s %s %s" % ("/usr/bin/python","/hes1sim/hes1.py",self.input))
        except Exception as e:
            output = str(e)

        return "%s|%s" % (host,output)
