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

    k1_e = 1e9
    k2_e = 0.1


    # service configuration
    
    parameters = ['k1_e','k2_e']
    is_single_instance = True
    address='hes1'

    def execute(self):

        host = "%s" % (socket.gethostbyname(socket.gethostname()))
        
        output = ""
        try:
            retcode,output,error = _execute("%s %s %s %s" % ("/usr/bin/python","/hes1simulation/hes1.py",str(self.k1_e),str(self.k2_e)))
        except Exception as e:
            output = str(e)

        return "%s|%s" % (host,output)
