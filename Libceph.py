import os
import time
import sys
import tarfile
from Libfioxdiscover import *
from Libfiox import *
from Libtest import *

class ComplexTest:
	def __init__(self):
		self.stat_filenames=[]
		self.background_process_monitor=None
		pass
        def sleep(self,sleeptime):
                        try: time.sleep(sleeptime)
                        except KeyboardInterrupt: print "Request to abort sleep early succeeded"
                        except:
                                print "Unknown reason for aborting sleep"
                                traceback.print_exc()

	def run_remote_command(self,cmd,filename):
		self.stat_filenames.append(filename)
		self.background_process_monitor.start_bg_remote_cmd(cmd,filename)

	def gettime(self):
		return time.localtime()

        def stop_perfcapture(self):
                debug("Stopping STATS capture",2)
                self.background_process_monitor.remoteArray.stop_all_bg_processes()
                timeout=0
                while self.background_process_monitor.remoteArray.verify_all_bg_processes():
                        debug("waiting for BG processes to complete",2)
                        time.sleep(5)
                        timeout+=1
                        if timeout > 60:
                                debug("Timeout exceeds 300 seconds, killing one more time and moving on",2)
                                self.background_process_monitor.remoteArray.stop_all_bg_processes()
                                break
                debug("completed stop_perfcapture, see above for errors",2)
