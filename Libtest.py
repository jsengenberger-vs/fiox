#from parser3par import AutomationInserv,debug
import sys
import subprocess
import os
import time

#global  StdoutLog
global DEBUG
DEBUG=-1
class stdout_wrapper:
	def __init__(self,logfile="./logs/defaultlog.log",basic=0):
		self.logfilelist=[]

		self.add_logfile(logfile)
		self.basic=basic
	def write(self,text):
		if self.basic:
			sys.__stdout__.write(text)
			for logfile in self.logfilelist:
				file(logfile,'a').write(text)

		else:

			#Multiple log files may need to be updated, but sys.stdout
			#should only be updated once
			##################
			printed=0
			for logfile in self.logfilelist:
				printed=debug(text,-1,logfile,printed=printed)
		#sys.__stdout__.write(text)
		#file(self.logfile,'a').write(text)
	def set_logfile(self,logfile):
		self.add_logfile(logfile)

	def add_logfile(self,logfile):
		self.logfilelist.append(logfile)
	def flush(self):
		pass


def wait_for_reboot(ip_list=[]):
	gone_list=[]
	back_list=[]
	debug("Waiting for the following IP addresses to reboot",2)
	debug(" ".join(ip_list),2)

	while (ip_list):
		for ip in list(ip_list):
			if os.name == "nt":
				output=os.popen("ping %s" % ip).read().lower()
				if output.find("is alive") > -1 or output.find("ttl=64") > -1:
					back_list.append(ip)
					ip_list.remove(ip)
			else:
				output=os.popen("ping -c 3 %s" % ip).read()
				if output.find("icmp_seq") > -1 or output.lower().find("ttl=63") > -1:
					back_list.append(ip)
					ip_list.remove(ip)
		debug("Waiting for %s to return" % " ".join(ip_list),2)

	debug("All systems have returned!",2)
	return
		
	
def debug(txt,level=0,logfile="./logs/tigerbox.log",raw=0,printed=0):
	timestamp=time.ctime()
	if level == -1:
		classname="TEST"
	elif level == 0:
		classname="LOG"
	elif level == 1:
		classname="DEBUG"
	elif level == 2:
		classname="DEVEL"

	fileOUT=file(logfile,'a')

	output_base="%s -- %s -- %s"

	if raw:
		print "%s -- %s RAW -- %s" % (timestamp,classname,txt)

	#Convert all strings to a list to simplify output parsing
	if isinstance(txt,str):
		txt=txt.split("\n")
	elif isinstance(txt,int):
		txt=[str(txt)]
	elif isinstance(txt,float):
		txt=[str(txt)]
	elif isinstance(txt,list):
		try:
			txt=[" ".join(txt)]
		except TypeError:
			txt=[" ".join(map(str,txt))]
	elif isinstance(txt,tuple):
		txt=[" ".join(txt)]
	else:
		txt=[str(txt)]

	for row in txt:
		if not str(row).strip():
			continue
		output=output_base % (timestamp,classname,row)
		if level <= DEBUG:
			if not printed:
				sys.__stdout__.write(str(output)+'\n')
				printed = 1
		fileOUT.write(output+'\n')
	return printed



class RemoteArray:
	def __init__(self,ip,user,password,default_prompt_list=[],command_header=""):
		self.ip=ip
		self.user=user
		self.password=password
		self.default_prompt_list=default_prompt_list
		self.bg_processes=[]
		self.command_header=command_header

	def __run_with_prompt__(self,cmd,prompt_list,cmd_list=[]):
		#print "###### RUNNING WITH PROMPT INPUT %s #####" % cmd

		prompt_list+=self.default_prompt_list
		if not cmd_list:
			cmd_list=cmd.split()


		#process=subprocess.Popen(cmd.split(),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
		process=subprocess.Popen(
						cmd_list,
						stdin=subprocess.PIPE,
						stdout=subprocess.PIPE,
						stderr=subprocess.PIPE,
						)
						#close_fds=1,
						#shell=1,
						#preexec_fn=os.setsid)
		output=""

		line=""
		while prompt_list:
			char=process.stdout.read(1)
			#if char:
			#	print [line]
			if not char:
				char=process.stderr.read(1)
				if not char:
					break
			line+=char

			for expected_str,send_string in prompt_list:
				if line.find(expected_str) > -1:
					debug(line,2)
					debug("found string [%s]" % expected_str,2)
					debug( "Sending %s" % send_string,2)
					process.stdin.write(send_string)
					output+=line
					line=""
			if char == "\n":
				#print line
				output+=line
				line=""
		#print line
		output+=line
		return output

	def wrap_remote_command(self,cmd,prompt_list=[]):				
		if os.name == "nt":
			cmd="plink -l %s -pw %s %s -t %s" % (self.user,self.password,self.ip,cmd)
		elif os.uname()[0] == "HP-UX":	 
			cmd='ssh -n -l %s %s %s' % (self.user,self.ip,cmd)
			prompt_list=[["Password",self.password+"\n"]]
		else:
			cmd='rsh %s -l %s "%s"' % (self.ip,self.user,cmd)
		debug(cmd,level=1)
		return cmd,prompt_list
		
	def __run_plink__(self,cmd,interactive=0,prompt_list=[],cmd_list=[]):

		#TODO FIX all of this path issues and stuff
		cmd,prompt_list=self.wrap_remote_command(cmd,prompt_list)

		if prompt_list or self.default_prompt_list:
			if cmd_list:
				cmd=cmd.split()
				cmd_list=cmd+cmd_list
			output=self.__run_with_prompt__(cmd,prompt_list,cmd_list)

		elif interactive:
			print "#############"
			print "# RUNNING COMMAND %s in INTERACTIVE MODE" % cmd
			print "#"
			os.system(cmd)
			print "#"
			print "#############"
			output="OK command done"
		else:		
			fd=os.popen(cmd)
			output=fd.read()
		return output

	def start_bg_remote_cmd(self,cmd,output_file=""):
		#Wrap the command with appropriate access protocol (SSH/RSH/plink)  
		#Ignore prompt_list, since we assume all comands are non-interactive
		cmd,prompt_list=self.wrap_remote_command(cmd)
		if output_file:
			fileobj=file(output_file,'w')
			self.start_bg_cmd(cmd,fileobj)
		else:
			self.start_bg_cmd(cmd)
	def start_bg_cmd(self,cmd,outputfile=subprocess.PIPE):
		debug(cmd,2)
		debug(outputfile,2)
		if hasattr(os.sys,'winver'):
			process=subprocess.Popen(cmd,stdout=outputfile,stderr=outputfile,creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
		else:
			process=subprocess.Popen(cmd.split(),stdout=outputfile)
		
		if process.poll() == None:
			self.bg_processes.append(process)
			return 1
		else:
			debug("ERROR: CMD %s terminated instantly, something is WRONG!" % cmd,2)
			return 0

	def verify_all_bg_processes(self):
		
		for proc in list(self.bg_processes):
			result=proc.poll()
			debug("%s result %s" % (proc,result),2)
			if result != None:
				debug("PROCESS %s is no longer running" % proc,2)
				self.bg_processes.remove(proc)

		return len(self.bg_processes)

	def stop_all_bg_processes(self):
		debug("Stopping all bg processes",2)
		for proc in self.bg_processes:
			proc.kill()
		debug("Sleeping 10 seconds",2)
		debug("Checking that all processes have completed",2)
		while (self.bg_processes):
			for proc in list(self.bg_processes):
				if proc.poll() != None:
					debug("Process %s done" % proc,2)
					#try:
					#	output=process.read().strip()
					#	if output:
					#		print "OUTPUT FROM PROC %s BELOW" % proc
					#		print output
					#except:
					#	print "Error reading output from %s" % proc
					#	traceback.print_exc()
					self.bg_processes.remove(proc)
			debug("Waiting 10 seconds, %s jobs remaining" % (len(self.bg_processes)),2)
			time.sleep(10)

		
		

	def run(self,cmd,raw=0,interactive=0,prompt_list=[],skip_default=0,level=2,cmd_list=[]):
		#cmd - command line to run on remote system
		#raw - 0=Return output as a list, 1=return output unparsed
		#######

		#Assume windows..covert to SSH for linux support
		debug(cmd,2)
		if not skip_default:
			cmd=self.command_header+cmd#.split()
		debug(cmd,level=level)
		output= self.__run_plink__(cmd,interactive,prompt_list,cmd_list)
		debug(output,level=level)
		if not raw:
			outputFinal=[]
			outputList=output.split("\n")
			for row in outputList:
				#Ignore all data after the ending --------- delimiter
				if row.find("------------------") > -1:
					break
				#Ignore empty rows
				elif not row:
					continue
				outputFinal.append(row)
			return outputFinal
		else:
			return output

class ParseConfig:
	def __init__(self):

		#specify a function to validate the value of the config value that was inputted
		#TODO:  Create field specific check functions
		#presently this is stictly doing type checking
		############

		self.valid_lun_key={
		'LABEL':str,
		'ARRAYIP':str,
		'ARRAYNAME':str,
		'ARRAYUSER':str,
		'ARRAYROOTUSER':str,
		'ARRAYPASS':str,
		'LUNS':int,
		'RAID':str,
		'DRIVECOUNT':int,
		'OCCUPANCY':float,
		'PDTYPE':str,
		'CPGPATTERN':str,
		'HATYPE':str,
		'THIN':str,
		'DEDUPE':str,
		'SHARED':str,
		'FABRICPORTS':str,
		}

		self.valid_host_key={
		'hostname':str,
		'username':str,
		'password':str,
		'ipaddress':str,
		'persona':str,
		'os':str,
		'wwn':str
		}

	def parse_lun_config(self,configfile):
		configlist=[]
		start=0
		for row in file(configfile):
			row=row.strip()
			if not row:
				continue
			elif row[0] == "#":
				continue
			elif row.find("====") > -1:
				if start:
					configlist.append(config)
				else:
					start=1
				config={}
			elif start:
				field=row.split(":")[0]
				value=":".join(row.split(":")[1:]).strip()

				fieldtype=self.valid_lun_key[field]

				config[field]=fieldtype(value)

		return configlist

	def parse_host_config(self,configfile):
		configlist=[]
		start=0
		for row in file(configfile):
			row=row.strip()
			if not row:
				continue
			elif row[0] == "#":
				continue
			elif row.find("====") > -1:
				if start:
					configlist.append(config)
				else:
					start=1

				config={
					'hostname':'',
					'persona':'',
					'wwn':[]
					}
			elif start:
				field=row.split(":")[0]
				value=":".join(row.split(":")[1:]).strip()

				fieldtype=self.valid_host_key[field]

				if field == 'wwn':
					config['wwn'].append(fieldtype(value))
				else:
					config[field]=fieldtype(value)
				
		return configlist		

def main():

	lunconfig_filename=sys.argv[1]
	hostconfig_filename=sys.argv[2]

	configparser=ParseConfig()
	lunconfigs = configparser.parse_lun_config(lunconfig_filename)
	hostconfigs = configparser.parse_host_config(hostconfig_filename)


	arrayIP="10.0.0.199"
	arrayUser="john"
	arrayPassword="P@ssword"
	#print "EXIT"
	#sys.exit()

	print lunconfigs

	#foo=RemoteArray(arrayIP,arrayUser,arrayPassword)

	#showpd_txt=foo.run("showpd")
	#print showpd_txt

	#a=AutomationInserv(remoteArray=foo)
	#a.parse_showpd(showpd_txt)
	#a.fetchData(foo)


	#a.wait_for_all_chunklets()

	print "Building luns"
	#a.create_hosts(hostconfigs)
	#a.create_luns(lunconfigs)
	#a.create_vluns(lunconfigs,hostconfigs)
	print "Created building luns"


	#a.create_snapshots(lunconfigs[0],4,1)
	print "Sleeping 5 seconds before promoting"
	#a.promote_snapshots(lunconfigs[0],4,1)

	#a.remove_snapshots(lunconfigs[0],4,1)
	#a.remove_snapshots(lunconfigs[0])
	#print a.remoteArray.run("showcpg")
	#print a.remoteArray.run("showvv")
	#print "sleepin 10 seconds before removing stuff"
	#time.sleep(10)
	#a.remove_vluns(lunconfigs,hostconfigs)
	#a.remove_luns(lunconfigs)
	#a.remove_hosts(hostconfigs)
	#print "Sleeping 5 seconds"
	#time.sleep(5)
	#print "\n".join(a.remoteArray.run("showcpg"))
	#print "\n".join(a.remoteArray.run("showvv"))
	#print a.storagenodes



if __name__ == "__main__":
	main()
