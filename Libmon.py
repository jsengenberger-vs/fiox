from collections import OrderedDict
import os
import json
import commands
import signal
import subprocess
import time
import traceback
import sys
from Libtest import debug

class FioXProcessManager:
	def __init__(self):
		self.process_list=[]

	#probably won't use wrap or start_process_remote_command functionality
	#because we're dealing with a multi host/multi node cluster setup that 
	#lacks centralization
	#############
        def wrap_remote_command(self,cmd):
                if os.name == "nt":
                        cmd="plink -l %s -pw %s %s -t %s" % (self.user,self.password,self.ip,cmd)
                else:
                        cmd='ssh -n -l %s %s %s' % (self.user,self.ip,cmd)
			#old school, lets not
                        #cmd='rsh %s -l %s "%s"' % (self.ip,self.user,cmd)
                debug(cmd,level=1)
		return cmd

        def start_process_remote_cmd(self,cmd,output_file=""):
                #Wrap the command with appropriate access protocol (SSH/RSH/plink)
                #Ignore prompt_list, since we assume all comands are non-interactive
                cmd=self.wrap_remote_command(cmd)
                if output_file:
                        fileobj=file(output_file,'w')
                        self.start_process(cmd,fileobj)
                else:
                        self.start_process(cmd)
        def start_process(self,cmd,outputfile=subprocess.PIPE):
                debug(cmd,2)
                debug(outputfile,2)
                if hasattr(os.sys,'winver'):
                        process=subprocess.Popen(cmd,stdout=outputfile,stderr=outputfile,creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
                else:
                        process=subprocess.Popen(cmd.split(),stdout=outputfile)

                if process.poll() == None:
                        self.process_list.append(process)
			#print process
                        return 1
                else:
                        debug("ERROR: CMD %s terminated instantly, something is WRONG!" % cmd,2)
                        return 0

        def verify(self):
                for proc in list(self.process_list):
                        result=proc.poll()
                        debug("%s result %s" % (proc,result),2)
                        if result != None:
                                debug("PROCESS %s is no longer running" % proc,2)
                                self.process_list.remove(proc)

                return len(self.process_list)

        def stop(self):
                debug("Stopping all processes",2)
                for proc in self.process_list:
                        proc.kill()
                debug("Sleeping 10 seconds",2)
                debug("Checking that all processes have completed",2)
                while (self.process_list):
                        for proc in list(self.process_list):
                                if proc.poll() != None:
                                        debug("Process %s done" % proc,2)
                                        self.process_list.remove(proc)
                        debug("Waiting 10 seconds, %s jobs remaining" % (len(self.process_list)),2)
                        time.sleep(10)
class process_sar_output:
	"""
Process a set of sar binary files using sadf into a flat CSV structure
	"""

	#Define fields used for labels within each data structure, otherwise the index will be used as the identifier
	stat_section_labels=[
		{'section':'cpu-load-all','ids':['cpu']},
		{'section':'network','ids':['iface']}, # Default - always at the bottom
		{'section':'default','ids':[]}, # Default - always at the bottom
	]
	ignore_section_labels=['timestamp','io','power-management']
	ignore_print_strings=['interrupts.intr','disk.disk-device']
	def __init__(self,samplesize=5,samplecount=10):
		self.sar_files=[]
		self.samplesize=samplesize
		self.samplecount=samplecount
		self.processed_sadf=OrderedDict()
		#all timestamps are appended here
		self.timestamps=[]
	def add_sar_file(self,filename):
		#Assume all sar files contain same basic input
		self.sar_files.append(filename)
	def __get_sadf_json__(self,filename,samplecount=4,samplesize=1):
		output=commands.getoutput("sadf -j %s %s %s -- -A" % (self.samplesize,self.samplecount,filename))
		loaded_json=json.loads(output)
		return loaded_json
	def process_sar_files(self):
		time.sleep(1)
		skip_timestamp=0
		for filename in self.sar_files:
			sadf_json=self.__get_sadf_json__(filename)
			for hostdata in sadf_json['sysstat']['hosts']:
				#print hostdata['nodename'],len(hostdata['statistics'])
				nodename=hostdata['nodename'].split(".")[0]


				for stat_data in hostdata['statistics']:
					if not stat_data.has_key('timestamp'):
						continue
					#print "#############"
					#print type(stat_data)
					#print dir(stat_data)
					#print stat_data
					#print stat_data.keys()
					#print stat_data['timestamp']
					#print stat_data.values()
					#print "#############"
					if not skip_timestamp:
						try:timestamp=stat_data['timestamp']['date']+" " +stat_data['timestamp']['time']
						except:
							print stat_data.keys()
							traceback.print_exc()
							print stat_data
							#sys.exit()
						self.timestamps.append(timestamp)
					for section in stat_data:
						#ignore some things, like timestamps
						if section in self.ignore_section_labels:continue

						if isinstance(stat_data[section],list):
							self.process_sar_stat(section,stat_data[section],nodename=str(nodename))
						else:
							self.process_sar_stat(section,[stat_data[section]],nodename=str(nodename))


					#self.process_sar_stat(				
			skip_timestamp=1
	
	def process_sar_stat(self,section,jsondata=[],nodename=None):

		def process_row(section,rowlabel,row,thisSection,nodename):
			for key,value in row.items():
				if isinstance(value,dict):
					#print "EEEEEEE"
					#print row
					#print type(row)
					#print "EEEEEEE"
					if rowlabel.find(key) > -1:
						thisrowlabel=rowlabel
					else:
						thisrowlabel="%s.%s" % (rowlabel,key)

					if thisSection['ids'] !=[]:
						for label in thisSection['ids']:
							if value.has_key(label):
								labelstring=value.pop(label)
								if thisrowlabel.find(labelstring) == -1:
									thisrowlabel+=".%s" % labelstring
					process_row(section,thisrowlabel,value,thisSection,nodename)
				elif isinstance(value,list):
					i=0
					for row in value:
						thisrowlabel="%s.%s" % (rowlabel,i)
						if thisSection['ids'] != []:
							thisrowlabel=[rowlabel]
							for label in thisSection['ids']:
								#copy value of label from row and remove it using {}.pop()
								if row.has_key(label):
									labelstring=row.pop(label)
									if str(thisrowlabel).find(labelstring) == -1:
										thisrowlabel.append(labelstring)
							thisrowlabel=".".join(thisrowlabel)
						process_row(section,thisrowlabel,row,thisSection,nodename)
						i+=1
				else:
					if rowlabel.find(section) > -1:
						if rowlabel.find(key) > -1:
							subsection_rowlabel=".".join([nodename,rowlabel])
						else:
							subsection_rowlabel=".".join([nodename,key,rowlabel])
					else:
						subsection_rowlabel=".".join([nodename,section,key,rowlabel])
					if not self.processed_sadf[section].has_key(key):
						self.processed_sadf[section][key]=OrderedDict()
					if not self.processed_sadf[section][key].has_key(subsection_rowlabel):
						self.processed_sadf[section][key][subsection_rowlabel]=[]
					if value == 0.0:
						value=0
					self.processed_sadf[section][key][subsection_rowlabel].append(value)

		thisSection=self.stat_section_labels[-1]
		for row in self.stat_section_labels:
			if row['section']==section:
				thisSection=row
		#if nodename != None:
		#	section="%s.%s" % (nodename,section)
		if not self.processed_sadf.has_key(section):	
			self.processed_sadf[section]=OrderedDict()
		i=0
		for row in jsondata:

			#special handling for rowlabel
			rowlabel="%s" % (i)
			if thisSection['ids'] != []:
				rowlabel=[section]
				for label in thisSection['ids']:
					#copy value of label from row and remove it using {}.pop()
					if row.has_key(label):
						rowlabel.append(row.pop(label))
				rowlabel=".".join(rowlabel)
			process_row(section,rowlabel,row,thisSection,nodename)
			#print str(row),[row]
			#for key,value in row.items():
			#	subsection_rowlabel=".".join([section,key,rowlabel])
			#	if not self.processed_sadf[section].has_key(key):
			#		self.processed_sadf[section][key]=OrderedDict()
			#	if not self.processed_sadf[section][key].has_key(subsection_rowlabel):
			#		self.processed_sadf[section][key][subsection_rowlabel]=[]
			#	if value == 0.0:
			#		value=0
			#	self.processed_sadf[section][key][subsection_rowlabel].append(value)

			i+=1
	
	def __contains_all_zeroes__(self,rowdata):
		"""
		determine if all values in the row are of zero value
		reduces noise and removes useless data from test data
		"""
		for value in rowdata:
			if isinstance(value,str):
				continue
			elif value == 0:
				continue
			else:
				return False
		return True
	def report(self,filelocation=""):
		for section,sectiondata in self.processed_sadf.items():
			for subsection,subsectiondata in sectiondata.items():
				outputs=[]
				outputs.append(["#"+subsection])
				timerow=['time']+self.timestamps
				outputs.append(timerow)
				for rowlabel,rowdata in subsectiondata.items():
					#Lets not print empty/meaningless lines
					if self.__contains_all_zeroes__(rowdata):
						continue
					#rowdata=[rowlabel]+rowdata
					ignore=0
					for ignore_str in self.ignore_print_strings:
						if rowlabel.find(ignore_str) > -1:
							ignore=1
							break
					if not ignore:
						rowdata=[rowlabel]+rowdata
						outputs.append(rowdata)

				if len(outputs) <=2:
					continue

				self.print_output(outputs,filelocation)

		
		
	def print_output(self,outputs,filelocation):
		if filelocation:
			outputfile=file(filelocation,'a')
			for row in outputs:
				outputfile.write(",".join(map(str,row))+"\n")
			outputfile.write("\n")
		else:
			for row in outputs:
				print ",".join(map(str,row))
			#print ""
				

class SarMon:
	def __init__(self):
		self.hosts=[]
		self.manager=FioXProcessManager()
		self.sarfiles=[]
		self.samplesize=1
		self.samplecount=10
		pass
	def add_host(self,host):
		self.hosts.append(host)

	def set_sample_parms(self,samplesize,samplecount):
		self.samplesize=samplesize
		self.samplecount=samplecount
		self.manager.samplesize=samplesize
		self.manager.samplecount=samplecount

	def wait_for_complete(self):
		while 1:
			if self.manager.verify() == 0:
				break
			time.sleep(1)
		return 1
	def start_capture(self,extra=""):
		for host in self.hosts:
			sarfile="/tmp/%s%s_sar.db" % (extra,host)
			self.sarfiles.append([host,sarfile])
			self.manager.start_process('ssh -q %s  sar -d %s %s -o %s > /dev/null' % (host,self.samplesize,self.samplecount,sarfile))
	def copy_files(self):
                count=0
		for host,sarfile in self.sarfiles:
			self.manager.start_process('scp -q %s:%s /tmp' % (host,sarfile))
                        count+=1
		self.wait_for_complete()
                return count

	def cleanup_sar_files(self):
		for host,sarfile in self.sarfiles:
			self.manager.start_process('ssh -q %s rm %s' % (host,sarfile))
		self.wait_for_complete()

	def process_files(self,filelocation=""):
		a=process_sar_output(self.samplesize,self.samplecount)
		for host,sarfile in self.sarfiles:
			a.add_sar_file(sarfile)
		a.process_sar_files()
		a.report(filelocation)

	def cleanup(self):
		self.cleanup_sar_files()
		for host,sarfile in self.sarfiles:
			try:os.remove(sarfile)
			except:continue
		self.reset()
	def reset(self):
		self.sarfiles=[]
class HistogramFioMon:
    def __init__(self):
        self.hosts=[]
    def add_host(self,host):
        self.hosts.append(host)
    def process_files(self):
        cmd="python fiologparser_hist.py --percentiles 90:95:99:99.99:99.999 --divisor 1000000 z_clat_hist.*.log*"
        results=commands.getoutput(cmd)
        return results
    def cleanup(self):
        cmd="rm z_clat_hist.*.log*"
        commands.getoutput(cmd)
        return 1


def test2():
	foo=SarMon()
	foo.add_host('hp380-04')
	foo.add_host('hp380-05')
	foo.add_host('hp380-09')
	foo.add_host('hp360-08')
	foo.add_host('hp360-11')
	foo.start_capture()
	foo.wait_for_complete()
	foo.copy_files()
	foo.process_files("/tmp/final.csv")
	foo.cleanup()
		
def test():
	print "test"
	foo=FioXProcessManager()
	foo.start_process('sar -d 1 10 -o /tmp/08.txt')
	foo.start_process('ssh hp360-11 sar -d 1 10 -o /tmp/11.txt > /dev/null')
	foo.start_process('ssh hp380-04 sar -d 1 10 -o /tmp/04.txt > /dev/null')
	while 1:
		if foo.verify() == 0:
			print "all done"
			break
		time.sleep(1)
	foo.start_process('scp hp360-11:/tmp/11.txt /tmp')
	foo.start_process('scp hp380-04:/tmp/04.txt /tmp')
	while 1:
		if foo.verify() == 0:
			print "all done"
			break
		time.sleep(1)

	a=process_sar_output()
	a.add_sar_file('/tmp/04.txt')
	a.add_sar_file('/tmp/08.txt')
	a.add_sar_file('/tmp/11.txt')
	a.process_sar_files()
	#print a.processed_sadf.keys()
	#return a
	a.report()
	#output=commands.getoutput("sadf -j foo.txt")
	#b=json.loads(output)
	#for key,item in b.items():
	#	for  x,y in item.items():
	#		print key,x,y

if __name__ == "__main__":
	test2()
