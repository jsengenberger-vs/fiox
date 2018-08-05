#!/usr/bin/env python
##################
### Frequent I/O eXerciser - FIOX
### Author: john@sengenberger.us
### Version 4.0.1
###
### NOTES:
###
### Special thanks:  
###			Memo Navarro		- Creating dpx in the Performance Lab
###			Bryan Carroll		- Bug Fixes
##################
import os
import commands
import random
import time
import signal
import sys
import traceback
import subprocess
#import fiox_discover
from subprocess import *
from optparse import OptionParser
from decimal import *
from Libtest import stdout_wrapper,debug


try:	os.mkdir("logs")
except: pass

#getcontext().prec=3
#global debug
#def debug(text):
#	if os.environ.has_key("DEBUG"):
#		if os.environ["DEBUG"] == "1":
#			print "DEBUG: %s" % text

class FioxCaptureTool:
	def __init__(self):
		import Libmon
		self.sarmon=Libmon.SarMon()
                self.histmon=Libmon.HistogramFioMon()

	def add_host(self,hostname):
		self.sarmon.add_host(hostname)

	def set_sample_parms(self,samplesize,samplecount):
		self.sarmon.set_sample_parms(samplesize,samplecount)

	def pretest_capture(self,measurementname):
		self.sarmon.start_capture(measurementname)

	def posttest_capture(self,measurementname):
                output=None
		self.sarmon.wait_for_complete()	
		self.sarmon.copy_files()
                output=self.histmon.process_files()

                return output

	def posttest_cleanup(self,measurementname):
		try:
			self.sarmon.process_files('/root/fiox//op_logs/perflogs/sarmon.%s.csv' % measurementname)
		except:
			traceback.print_exc()
		#print "posttest cleanup"
		self.sarmon.cleanup()

class Commands:
	def __init__(self):
		self.procs=[]
	def run(self,cmd):
		#print "Adding %s" % cmd
		p=Popen(cmd, shell=True)
		#print "pid %s" % p.pid
		self.procs.append(p.pid)

	def wait(self):
		"""
		Returns an empty list if all pids return 0 status (Good status)
		Returns a list of unique status codes when statuscode > 0
		"""
		print "Beginning wait %s" % time.ctime()
		starttime=time.time()

		allstatus=[]
		while self.procs:
			pid,status=os.waitpid(0,0)
			if status:
				if status not in allstatus:
					allstatus.append(status)
	
			self.procs.remove(pid)
			print "removing %s with status %s, num remaining: %s" % (pid,status,len(self.procs))
		endtime=time.time()
		time_minutes=(endtime-starttime)/60
		print "Completed, total run time: %s" % time_minutes

		return allstatus

class CSVPerfData:
	def __init__(self,filename="output",extra=""):
		self.data={}
		self.datakeys={}
		filename=filename.replace(".","_")
		self.filename=filename+extra+".csv"
		debug("CSV Data will be saved to %s" % self.filename,1)

	def add_data(self,timestamp,key,value,dataclass='generic'):
		#assumes all timestamps are synced between systems...
		#TODO: Fix timestamp assumptions
		#if timestamp not in self.timestamps:
		#	self.timestamps.append(timestamp)

		#Convert timestamp to datetime object

		if not self.data.has_key(timestamp):
			self.data[timestamp]={}

		if self.data[timestamp].has_key(key):
			print timestamp
			print self.data[timestamp]
			raise AttributeError,"Duplicate key %s found in timestamp section, aborting" % key

		if not self.datakeys.has_key(dataclass):
			self.datakeys[dataclass]=[]

		if key not in self.datakeys[dataclass]:
			self.datakeys[dataclass].append(key)
		self.data[timestamp][key]=value	


	def report(self,dataclass="DEVICE"):
		timestamps_sorted=self.data.keys()
		timestamps_sorted.sort()
		timestamps_sorted_strings=[]
		for timestamp in timestamps_sorted:
			timestamp=time.strftime("%d/%b/%y %H:%M:%S.000",timestamp)
			timestamps_sorted_strings.append(timestamp)
	
		#generate output, where CSV data is output in a Row, rather than a column type format
		#
		#timestamps will have duplicate output for each unique key
		###############
		print "Saving CSV report to %s" % self.filename
		outputfile=file(self.filename,'w')
		for iotype in ['Reads','Writes','Total']:
			for metric in ['iops','mbs','resp']:
				outputfile.write(",".join(timestamps_sorted_strings)+"\n")
				for key in self.datakeys[dataclass]:
					if key.find(iotype) == -1 or key.find(metric) == -1:
						continue
						
					data=[key]
					dataclass_timestamps=["timestamps"]
					for timestamp in timestamps_sorted:
						if self.data[timestamp].has_key(key):
							#dataclass_timestamps.append(timestamp)
							data.append(self.data[timestamp][key])
						else:
							data.append("N/A")
							print "ERROR: Missing timestamp %s from dataset %s" % timestamp
		
					#outputfile.write(",".join(dataclass_timestamps)+"\n")
					outputfile.write(",".join(data)+"\n")
				outputfile.write("\n")


	#We will start with dataclass=TOTAL..and proceed from there
	############################
	def report_timeperiod_performance(self,timestamp_start,timestamp_stop,dataclass,iotype,metric):
		timestamps=self.data.keys()
		timestamps.sort()

		count_of_values=0
		sum_of_values=0
		max_value=0
		min_value=999999999999999

		for timestamp in timestamps:
			if timestamp < timestamp_start:
				continue
			elif timestamp > timestamp_stop:
				break
			else:
				for key in self.datakeys[dataclass]:
					if key.find(iotype) == -1 or key.find(metric) == -1:
						continue
					else:
						if self.data[timestamp].has_key(key):
							value=float(self.data[timestamp][key])
							if value > max_value:
								max_value=value
							if value < min_value:
								min_value=value
							sum_of_values+=value
							count_of_values+=1
	
		if count_of_values > 0:
			average_value=float(sum_of_values)/float(count_of_values)
		else:
			average_value=0

		return average_value,max_value,min_value

		
		
				
				
	
class Measurement:
	xfer=0
	qdepth=0
	devList=[]
	warmupTime=0
	measurementTime=0
	access=""
	skipcleanup = 0
	tool="Default"

	def __init__(self,xfer=0,qdepth=0,warmupTime=0,measurementTime=0,access="",devList=[],readpct=0,offset="DP ONLY",seekpct=100,sampleCount=1,hostConfig={},extralabel="",experimentmsg="",histogram_buckets=[],aoConfig=[],iops_override=0,pattern=0,cachehit=[0,0],ioengineConfig={}):
		#Move self.ioengine to fiox, its not used in other benchemarks
		self.ioengine="sync"
		#self.seekpercent=100
		self.direct=1
		self.xfer=xfer
		self.qdepth=qdepth
		self.readpercent=readpct
		self.seekpercent=seekpct
		self.warmupTime=warmupTime
		self.measurementTime=measurementTime
		self.sampleCount=sampleCount
		self.hostConfig=hostConfig
		self.ioengineConfig=ioengineConfig # only used by FIO today
		self.aoConfig=aoConfig
		self.experimentmsg=experimentmsg.replace(" ","_").strip()
		self.histogram_buckets=histogram_buckets
		self.iops_override=iops_override
		self.pattern=pattern
		self.cachehit=cachehit
		self.inputFilemytime=int(time.time())
		self.seed=random.randrange(0,100000)
		self.numtcpPortsPerHost=1
		#print "MY PATTERN: %s" % self.pattern
		#print "MY CACHEHIT: %s" % self.cachehit

		self.devList=devList
		self.filldisks=0
		self.__translate_access__(access)
		self.tool="Default"
		self.offset=offset

		self.csvData=[]
		self.extralabel=extralabel

		self.inputFile="%(tool)s_%(xfer)s_rdpct%(readpercent)s_seekpct%(seekpercent)s_%(qdepth)s_%(inputFilemytime)s_%(seed)s.%(tool)s" % vars(self)
		#Optional defintion.  Only used by DPold
		##########################
		self.outputfile=None

		#Defaults
		self.errorFound=0
		self.deviceCount=0
		self.extraOptions=[]

		self.detailed_results=[]
		self.read_latency_percentile=[]
		self.write_latency_percentile=[]

		#Used if forking process via subprocess
		self.process=None

	def __is_sun_os__(self):
		try:
			os_name=os.uname()[0].lower()
			if os_name == "sunos":
				return 1
		except:
			pass
		return 0
	def run(self):
		pass

	def parse_output(self,output):
		print output


	def fill(self):
		"""
		For device/block devices, use a sequential write stream to write every address on a device
		For Filesystems/files, create the files and sequentially write to every address in the file.  Files are created 1 at a time per filesystem mount, to ensure
		data is placed on the file system sequentially.  This prevents the contents of each file from being intermixed with each other.
		
		This method uses the dd tool.  DD is universal for all unix platforms.  Other tools, like DT, vdbench, DP, and fio
		have methods of performing a fill, but may not be available in all cases

		This method may be overridden to create tool specific implementations
		"""

		blocksize=256	#in KiBytes
		count=0

		if self.extraOptions.size_of_files:
			size=self.extraOptions.size_of_files.strip().lower()
			if size[-1] == "m":
				size=float(size.replace("m",""))
				count=int((size*1024)/blocksize)
			elif size[-1] == "g":
				size=float(size.replace("g",""))
				count=int((size*1024*1024)/blocksize)
			elif size[-1] == "t":
				size=float(size.replace("t",""))
				count=int((size*1024*1024*1024)/blocksize)
			else:
				print "ERROR: Invalid size of file %s, must specify m|M|g|G|t|T for size, e.g. 100g or 150M"
				return 0
	
		commander=Commands()
		extra=""
		if self.direct:
			extra+=" oflag=direct"
		if count:
			extra+=" count=%s" % count
		for dev in self.devList:
			commander.run("dd if=/dev/zero of=%s bs=%sk %s >> fiox_fill.log 2>fiox_fill.log" % (dev,blocksize,extra))

		status=commander.wait()
		if status:
			print "###############"
			print "##"	
			print "## ERROR: 1 or more fill processes exited with status > 0"
			print "## CODES: %s" % ",".join(map(str,status))
			print "##"	
			print "## - It is likely the fill did not work correctly.  Look at fiox_fill.log.  Fixx the problem and try again"
			print "###############"
		return 1
	def setExtraOptions(self,options):
		self.extraOptions=options
		if self.extraOptions.o_sync:
			self.setDirect(0)
		if self.extraOptions.fill or self.extraOptions.fillstop:
			self.filldisks=1
			#self.setDirect(0)

	def setDirect(self,direct):
		#Used to toggle O_DIRECT.  Default=1 (enabled)
		self.direct=direct

	def __translate_access__(self,access):
		self.access=access

	def setErrorFound(self):
		if not self.getErrorFound():
			print "#############################"
			print "## ERRORS during execution ##"
			print "#############################"
			self.errorFound=1

	def getErrorFound(self):
		return self.errorFound
	def get_qdepth(self):
		return self.qdepth

	def supports_detailed_results(self):
		#by default, detailed results are not supported
		if self.detailed_results:
			return 1
		else:
			return 0
	def supports_percentiles(self):
		#by default, detailed results are not supported
		if len(self.read_latency_percentile) >0 or len(self.write_latency_percentile) > 0:
			return 1
		else:
			return 0
	def get_percentiles(self,limit=7):
		#limit to the last 7 percentile items, which should be 90,95,99,99.5,99.9,99.99,99.95
		reads='\tr '+",".join(map(str,self.read_latency_percentile[-limit:]))
		writes='\tw '+",".join(map(str,self.write_latency_percentile[-limit:]))
		return reads + ' ' + writes

	def get_detailed_results(self):
		return self.detailed_results
	
	def print_long_logfile(self,output,logfile=None):
		if not logfile:
			logfile="./logs/%(tool)s_ll.log" % vars(self)

		logfileOutput=file(logfile,'a')

		for row in output.split("\n"):
			row=row.strip()
			text="%s -- %s\n" % (time.ctime(),row)
			logfileOutput.write(text)
		logfileOutput.close()

	def cleanup(self):
		#Cleanup any tempoary or input files generated by this tool
		if not self.skipcleanup:
			if os.name == "nt":
				os.system("del %s" % self.inputFile)
			else:
				#Skip cleaning up multi host for now
				if len(self.hostConfig.keys()) == 0:
					os.system("rm %s" % self.inputFile)
	def __get_ao_config(self,aofile,aoname):
		for row in file(aofile,'r'):
			if not row.strip():
				continue
			row=row.strip().split()
			if row[0].lower() == aoname.lower():
				return row
		return []

	def __setup_ao_qdepth_and_disks__(self):
		#cutoff in milliseconds to plan on for controlling delays
		LATENCY_CUTOFF=30
		#Base IO release time on a 1 minute
		IODELAYTIME=60
		#print self.aoConfig
		aofile,aoname,aocapacity=self.aoConfig
		aocapacity=int(aocapacity)

		row=self.__get_ao_config(aofile,aoname)
		if not row:
			print "ERROR: Unable to find AOpolicy %s in %s" % (aoname,aofile)
			raise AttributeError,"ERROR: AO config specified but no matching AO policy could be found"

		#Create a map file so we can use multiple disk targets
		disk_alias=self.__setup_file_for_ao__()
		#read in data from csv file and convert to int.  Be aware decimal points abound so converting to 
		#float first is a quick way around this issue
		#yes I could use Double but this is what I'm doing
		#-JS 7/15/2014
		####################

		ao_policy_space_gb=float(row[1])

		#region density io reports are based on a capacity/IO per minute ratio
		#When we simulate a workload using a region density report, you need to take
		#into account the total capacity of luns being tested vs the Space GiB value in the 
		#region density report
		#
		#If the capacity is identical, then we can simulate the workload using the same IOPS
		#
		#However if the capacity is less or more than the original, the number of IOPS will need to be
		#scaled appropriately
		#
		#TAke the Space GIB / Total capacity in luns being tested, this gives us a derate factor that we will
		#multiply by the IOPS to get a reasonable target
		##
		#This can significantly reduce the IOPS.  If the original report has 19 TiB worth of capacity, and your target luns 
		#for testing have 1 TiB of capacity, you will have 1/19th the IOPS
		#
		#######################
		if aocapacity <= 0:
			iop_derate_factor=1
		else:
			iop_derate_factor=aocapacity/float(ao_policy_space_gb)
		#print "AO SPACE GB %s" %  ao_policy_space_gb
		##print "CAPACITY %s" % aocapacity
		#print "IOP DERATE FACTOR: %s" % iop_derate_factor

		total_io_per_min=0
		space_gib_data=map(float,row[5:29])
		acc_gib_data=map(float,row[29:53])
		#print space_gib_data
		#print acc_gib_data
		#print len(space_gib_data)
		#print len(acc_gib_data)

		assert len(space_gib_data) == len(acc_gib_data)

		percentage_of_iops=[]

		last_offset=0
		last_percent=0
		total_qdepth=0
		total_iops=0
		deviceTextList=[]
		blocksize=16384
		ConfigDetails=[]
		for i in range(0,len(space_gib_data)):
			space_accessed = space_gib_data[i]
			offset=""
			if space_accessed > 0:
				percent=float(space_accessed) / float(ao_policy_space_gb)
				percentage_of_iops.append(percent)
				current_offset=last_offset+(float(ao_policy_space_gb) * float(percent))
		#		print "PERCENT: %s " % percent
		#		print "last_offset: %s " % last_offset
		#		print "current_offset: %s " % current_offset
				current_percent = last_percent + percent
				if current_percent > 1:
					current_percent = 1
				elif current_percent < 0:
					current_percent=0
				#offset="[%s:%s]" % (last_offset,current_offset)
				#offset="[%s,%s]" % (last_percent,current_percent)
				start_offset_block=int((aocapacity*last_percent*1024*1024*1024)/blocksize)
				#Always subtract 1 from the end block, ensuring we never run out of capacity and that there is "space" between each iteration
				end_offset_block=int((aocapacity*current_percent*1024*1024*1024)/blocksize) -1

				#offset="[%s,%s]" % (start_offset_block,end_offset_block)
				offset=start_offset_block,end_offset_block#"[%s,%s]" % (start_offset_block,end_offset_block)

				debug("SPACE OFFSETS %s %s" % (start_offset_block,end_offset_block))

				
				actual_io_per_minute = acc_gib_data[i]
				actual_io_per_minute_per_gib=actual_io_per_minute / space_accessed

				target_io_per_minute=actual_io_per_minute_per_gib * float((percent * aocapacity))
				target_io_per_second = target_io_per_minute / 60

				if actual_io_per_minute > 0 and target_io_per_minute <= 0:
		#			print "ADJUSTING TARGET IOPM to 1"
					target_io_per_minute = 1
				if target_io_per_minute <= 0:
					debug("SKILLING THIS OFFSET, IOPM is LESS THAN or EQUAL to 0")
					continue


				#actual_iops = actual_io_per_minute/60
#
#				print space_accessed,actual_io_per_minute
#
#				target_io_per_minute = actual_io_per_minute * iop_derate_factor
#				target_iops=actual_iops * iop_derate_factor
		#		print "space accessed: %s" % space_accessed
				debug("TARGET IOPM %s: %s     ACTUAL IOPM: %s" % (i,target_io_per_minute,actual_io_per_minute))
				last_offset = current_offset+1
				last_percent = current_percent

				#setup delay to deliver IO requests per minute
				#if the delay is less than 1 second
				#delay = 60 / actual_io_per_minute
				###########
				#NOTE: This may be redundant.  We've already calculated the number of IOPS above..why are we doing this?
				#-JJS 7/23/2014
				delay = IODELAYTIME / target_io_per_minute
				#If less than a 30 millisecond delay, determine how many I/O generating processes we require
				###########################
				qdepth=1
				#?Use the interactive response time law to calculate queue depth and delay X=N/r
				#For delays less than 30ms, it may be difficult to ensure accurate timing.  IN those cases increase
				#the target queue depth and delay to compensate
				#
				#For example, 30ms delay and qdepth of 2, for this simulation, is effectively the same as 15ms delay and qdepth of 1
				####################
				if delay < 0.2:
					#Delay in this case is the response time
					while (delay < 0.2):
						qdepth+=1
						delay=qdepth/target_io_per_second
					qdepth = target_io_per_second * delay

				total_iops+=target_io_per_second
				#If its unlikely we'll even send an IO, don't bother adding this to the workload, skip this disk and continue
				if (delay) > (self.measurementTime/2):
					debug( "DELAY TIME exceeds HALF of measurement time")
					debug( "DELAY %s  measurement %s" % (delay,self.measurementTime))
					continue
				else:
					#Delay in ms then into microseconds for vesper
					delay = int(delay * 1000)*1000
					total_qdepth+=qdepth
		#			print "DELAY %s qdepth %s \t\toffset:%s" % (delay,qdepth,str(offset))
	
					#deviceTextList.append("%s qdepth=%s offset=%s delay=%s" % (disk_alias,int(qdepth),offset,delay))
					ConfigDetails.append([disk_alias,int(qdepth),offset,delay])
	
					#print acc_gib_data[i],iops,delay,offset
			else:
				percentage_of_iops.append(0)
			#total_io_per_min+=acc_gib_data[i]

		#total_iops=total_io_per_min / 60
		#self.deviceText="\n".join(deviceTextList)
		print "### ESTIMATED IOPS for %s: %s %s ####" % (aoname,total_iops,aocapacity)
		return ConfigDetails
	def poll(self):
		if self.process:
			#print "process poll %s" % self.cmd
			return self.process.poll()
		else:
			return -1

	def stop(self,skip_cleanup=0,LIMIT=60):
		if not self.process:
			raise AttributeError,"No self.process defined, must run self.run(fork=1) to use this method"	


		print "Terminating process"
		####################
		#NOTE: 
		#NOTE: THIS REQUIRES VESPER VERSION 5.6.0+ TO WORK
		#NOTE: PREVIOUS VERSIONS DO NOT HANDLE CTRL BREAK CORRECTLY OR SIGQUIT
		#NOTE: -JJS 10/3/2013 - AFTER MANY LONG HOURS DEBUGGING THIS
		#NOTE:
		#NOTE: SIGNALS ARE IGNORED DURING non-measurement WINDOWS.  This means
		#NOTE: WARMUP AND COOLDOWN TIMES will IGNORE the signal
		#NOTE: -JJS 7/22/2014 - after ANOTHER day of debugging
		#NOTE: 
		####################
		if self.process.poll() == None:
			if hasattr(os.sys,'winver'):
				print "Sending Ctrl Break"
				#self.process.send_signal(signal.CTRL_BREAK_EVENT)
				self.process.send_signal(signal.CTRL_BREAK_EVENT)
				subprocess.Popen("TASKKILL /F /PID {pid} /T".format(pid=self.process.pid))
			else:
				print "sending SIGQUIT"
				self.process.send_signal(signal.SIGQUIT)
		print "Signal sent"
		if skip_cleanup:
			return 0,0,0,0,0
		else:
			return self.cleanup_process(LIMIT)
	def cleanup_process(self,LIMIT=60):

		#Check for updates..and poll process for final termination
		wait_60_seconds=0
		output=""
		print "starting loop"
		print self.process.poll()
		while self.process.poll()==None:
			#print "in loop"
			#line=self.process.stdout.read()
			#if line:
			#	output+=line
			#print self.process.poll()
			self.process.send_signal(signal.CTRL_BREAK_EVENT)
			time.sleep(1)
			wait_60_seconds+=1
			if wait_60_seconds == LIMIT:
				print "PROCESS POLL STATUS: [ %s ]" % str(self.process.poll())
				print "READ OUTPUT below: " 
			#	print line
				self.process.kill()
			elif wait_60_seconds == LIMIT*5:
				print "Still waiting after 5 minutes...doing something more invasive"
				print "PROCESS POLL STATUS: [ %s ]" % str(self.process.poll())
				print "READ OUTPUT below: " 
			#	print line
				self.process.terminate()
		print "outside of loop"
		print self.process.poll()
		output+=self.process.stdout.read()
		errors=self.process.stderr.read().strip()
		print "############"
		print "############"
		print "############"
		print "OUTPUT"
		print output
		print "ERRORS"
		print errors
		try:print self.cmd
		except:pass
		print "############"
		print "############"
		print "############"
		print "#######"
		if errors:
			print "STDERR: %s" % errors
			self.print_long_logfile("STDERR: %s" % errors)
		else:
			print "NO errors detected"
		self.print_long_logfile(output)
		print "Confirming DP is ready to exit"
		self.process.wait()
		if self.outputfile:
			print "Reading in DP outputfile %s" % self.outputfile
			fileoutput=file(self.outputfile).read()
			self.print_long_logfile(fileoutput)
		else:
			fileoutput=output
		print "DP has exited, parsing output"
		return self.parse_output(fileoutput)
	



class DPold_Measurement(Measurement):
	cmd="vesper"
	tool="vesper"
	def __init__(self,*args,**kargs):
		Measurement.__init__(self,*args,**kargs)

		self.deviceCount=len(self.devList)
		self.tool="vesper"
		self.pattern_text=""
		self.cachehit_text=""

		#debug("MY READ PERCENT: %s "% self.readpercent)
		#debug(self)

		self.inputFile="%(tool)s_%(xfer)s_rdpct%(readpercent)s_seekpct%(seekpercent)s_%(qdepth)s_%(inputFilemytime)s_%(seed)s.%(tool)s" % vars(self)

		self.csvdata=CSVPerfData(self.inputFile,extra=self.extralabel)

		#self.interval=self.measurementTime
		self.sampleInterval=self.measurementTime / self.sampleCount

		self.cooldownTime=self.warmupTime
		#if the warmupTime is less than self.inteval or not evenly divisble, set interval=1
		#if (self.warmupTime < self.interval) or (self.warmupTime % self.interval):
		#	self.interval=1

		#Convert transfer size in kb to bytes
		self.xferSize=self.xfer*2
		self.xferBlocksize=512
		self.xferBytes=self.xfer*1024

		#self.histogram_buckets=[1,2,3,4,5,7,10,12,15,20,25,30,35,40,50,60,70,80,100,120,150,200,500,750,1000]

		
		self.deviceText=""
		self.mapfile=""
		#self.aoConfig=[]
		self.extra=""


	def fill(self,fork=0):
		self.__create_fill_text__()
		self.run(fork=fork,skipparse=1)

	def __translate_access__(self,access):
		if access == "sequential read":
			self.readpercent=100
			self.access="sequential"
		elif access == "segmented read":
			self.readpercent=100
			self.access="segmented"
		elif access == "segmented write":
			self.readpercent=0
			self.access="segmented"
		elif access == "sequential write":
			self.readpercent=0
			self.access="sequential"
		elif access == "random read":
			self.readpercent=100
			self.access="random"
		elif access == "random write":
			self.readpercent=0
			self.access="random"
		elif access == "OLTP":
			self.access="random"
		elif access == "SEQOLTP":
			self.access="sequential"
		elif access == "SEGOLTP":
			self.access="segmented"
		elif access == "CUSTOM_OLTP":
			self.access="mixed"
		elif access == "fill":
			self.access="sequential"
			self.filldisks=1
		else:
			raise AttributeError,"Invalid access mode"
	def get_extra_dp_parms(self):
		extra=[]
		if not self.direct:
			extra.append("direct = false")
		else:
			extra.append("direct = true")

		if self.iops_override:
			#Convert to usecs
			#delay = (self.iops_override / self.qdepth)*1000
			#extra.append("delay = %s" % delay)
			extra.append("delay = 50000")#%s" % delay)

		#if self.histogram_buckets:
		#	extra.append('histogram = true buckets = "%s"' % ",".join(map(str,self.histogram_buckets)))

		#Add in percentage of random seeks
		if self.access == "mixed":
			if self.seekpercent == -1:
				print "ERROR: seekpercentage not specified.  Skipping"
			else:
				extra.append("random = %s" % self.seekpercent)

		self.extra=" ".join(extra)
	def __setup_qdepth_and_disks__(self,fill=0):
	
		#If using Multi-Host mode, change self.devList and behave differently
		#print "HOST CONFIG %s" % self.hostConfig
		if self.hostConfig:
			self.__setup_qdepth_and_disks_multihost()
		if fill:
			self.deviceText="\n".join(self.devList)
		else:
			if self.aoConfig:
				debug( "AO Config detected, using alternate qdepth construction")
				debug( "USING AO QDEPTH")
				ConfigDetails=self.__setup_ao_qdepth_and_disks__()
				self.__setup_ao_qdepth_and_disks_config__(ConfigDetails)
			else:
				debug( "USING STANDARD QDEPTH")
				self.__setup_qdepth_and_disks_localhost()
		#print "HOST CONFIG AFTER %s" % self.hostConfig

	def __setup_ao_qdepth_and_disks_config__(self,ConfigDetails):
		deviceTextList=[]
		for mapgroup_alias,qdepth,offset,delay in ConfigDetails:
			offset_txt="[%s,%s]" % (offset[0],offset[1])
			deviceTextList.append("%s qdepth=%s offset=%s delay=%s" % (mapgroup_alias,int(qdepth),offset_txt,delay))
		self.deviceText="\n".join(deviceTextList)
		return


	def __setup_qdepth_and_disks_multihost(self,shared=0):
		"""
		Assumes all hosts have access to different host systems
		"""
		hostnames=self.hostConfig.keys()
		self.devList=[]
		if self.aoConfig:
			print "AO Config is present, handling multi host disks differently"
		for host in hostnames:
			for device in self.hostConfig[host]:
				if device == 'localhost':
					self.devList.append(device)
				else:
					self.devList.append("%s@%s" % (host,device))
			
		#print self.devList
	
#	def __get_ao_config(self,aofile,aoname):
#		for row in file(aofile,'r'):
#			if not row.strip():
#				continue
#			row=row.strip().split()
#			if row[0].lower() == aoname.lower():
#				return row
#		return []

	def __create_mapfile(self,mapgroup_alias="mapgroup"):
		self.mapfile="mapfile_%s.map" % time.time()
		mapfile=file(self.mapfile,'w')
		mapfile.write("%s %s\n" % (mapgroup_alias,",".join(self.devList)))
		mapfile.close()
		#Give the system 1 second to catch up.  May help address an error where DP was unable to open a file immediately after it was written
		##################
		time.sleep(1)
		return self.mapfile,mapgroup_alias

	def __remove_mapfile__(self):
		if self.mapfile:
			os.remove(self.mapfile)


	def __setup_file_for_ao__(self):
		mapfile,mapgroup_alias=self.__create_mapfile()
		return mapgroup_alias

	def __setup_qdepth_and_disks_localhost(self):
		deviceTextList=[]
		devId=0
		if self.aoConfig:
			print "AO Config is present, handling local host disks differently"
		for dev in self.devList:
			devId+=1

			#If we have more disks than qdepth, throw out disks not being used
			if devId == self.qdepth:
				break

		self.deviceCount=devId
		if self.deviceCount > self.qdepth:
			self.qdepth=1
		else:
			self.qdepth=int(self.qdepth/self.deviceCount)

		devId=0
		for dev in self.devList:
			#dev=self.devList[devId]

			deviceTextList.append("%s qdepth = %s" % (dev,self.qdepth))

			devId+=1
			if devId == self.deviceCount:
				break
		self.deviceText="\n".join(deviceTextList)

		return self.deviceText	

	def __setup_pattern_value__(self):
		if self.pattern == 0:
			self.pattern_text="pattern = default"
		elif self.pattern == 1:
			self.pattern_text="pattern = randomized"
		else:
			dedupenum=1/Decimal(self.pattern)
			dedupenum=100-int(dedupenum*100)
			if dedupenum == 0:
				self.pattern_text="pattern = randomized patterndepth=100"
			else:
				self.pattern_text="pattern = dup%s patterndepth=100" % dedupenum

	def __create_fill_text__(self):
		self.__setup_qdepth_and_disks__(fill=1)
		self.__setup_pattern_value__()
		fileText="""
!phase prefill = true access = %(access)s size = 1  blocksize = %(xferBytes)s reads = 0 offset = [0.0,1.0] %(pattern_text)s
%(deviceText)s
		""" % vars(self)

		return fileText.strip()

	def __setup_cachehits__(self):
		cachehit,cachdepth=self.cachehit
		try:
			if cachehit == 0:
				self.cachehit_text="cachehits = 0"
			elif int(cachehit) != 0:
				cachehits=self.cachehit[0]
				cachedepth=self.cachehit[1]
				self.cachehit_text="cachehits = %s cachedepth = %s" % (cachehits,cachedepth)
		except:
			traceback.print_exc()
			print "Invalid input to cachehit, aborting"
			self.cachehit_text="cachehits = 0"
		return self.cachehit_text
		
	def __create_file_text__(self):
		self.__setup_qdepth_and_disks__()
		self.__setup_pattern_value__()
		self.__setup_cachehits__()
		self.get_extra_dp_parms()
		if self.histogram_buckets:
			bucketFinal=[]
			for bucket in self.histogram_buckets:
				bucketFinal.append(bucket*1000)
			self.histogramText='histogram = true buckets = "%s"' % ",".join(map(str,bucketFinal))
		else:
			self.histogramText=''

		if not self.aoConfig:
			self.extra+=" offset = %(offset)s " % vars(self)
		fileText="""
warmup = %(warmupTime)s measure = %(measurementTime)s sample = %(sampleInterval)s cooldown = %(cooldownTime)s rest = 0 update = 0 timestamps = true
verbose = false pretty = false align = 0 accelerator = 1 rwstats = true catchup = true stagger = false
%(cachehit_text)s
%(pattern_text)s
%(histogramText)s
!phase prefill = false access = %(access)s size = 1  blocksize = %(xferBytes)s reads = %(readpercent)s 
%(extra)s
%(deviceText)s
""" % vars(self)
		return fileText.strip()


	def __parse_perf_data__(self,output=[]):
		"""
sample output
  30 second warm up...done.
  90 second measurement interval...done.
  30 second cool down...done.
localhost go delta = 0.000305 secs
localhost start delta = 0.000234 secs
localhost finish delta = 0.000196 secs
localhost processing overhead = 0.676672%
localhost@/dev//dm-1|1|50885.7|416.9|0.156|103.2
localhost@/dev//dm-1|T|50885.7|416.9|0.156|103.2
localhost@TOTAL|1|50885.7|416.9|0.156|103.2
localhost@TOTAL|T|50885.7|416.9|0.156|103.2
GRANDTOTAL|1|50885.7|416.9|0.156|103.2
GRANDTOTAL|T|50885.7|416.9|0.156|103.2

Phase 1 complete

		"""

		try:
			total_iops=output[2]
			total_mbs=output[3]
			total_latency=output[4]
			try:
				bucketLen=len(self.histogram_buckets)+6
				histogram=",".join(self.parse_histogram(output[5:bucketLen]))
			except:
				traceback.print_exc()
				histogram=None


			#check to see if total_iops is zero
			if float(total_iops) == 0:
				total_iops=0
				total_mbs=0
				total_latency=0
		except:
			total_iops=0
			total_mbs=0
			total_latency=0
			histogram=0

		cpu_user=0
		total_latency=Decimal(total_latency).quantize(Decimal('0.01'))

		return total_iops,total_mbs,total_latency,cpu_user,histogram
	
	def parse_histogram(self,histogram):
		####
		# Parse a string in this format: [0-1999]=10,[2000-3999]=114,[4000-5999]=312
		####
		output=[]
		for data in histogram:#.split(","):
			count=data.split("=")[-1]
			output.append(count)
		return output
		

	def parse_dp_output_for_timeseries(self,output):
		print "Parsing timeseries output"
		
		for row in output.split("\n"):
			if not row.strip():
				continue

			row=row.split("|")
			#Check if all of the columns line up.  DP/DPold output parsing is straightforward
			try:
				timestamp,device,sample,iops,mbs,resp,maxresp = row
				timestamp=time.strptime(timestamp,"%d/%b/%y %H:%M:%S.000")
			except:
				continue

			if not timestamp:
				continue

			#Determine if this is reads, writes or total
			sampletype=sample[-1]
			if sampletype == "r":
				sampletype = "Reads"
			elif sampletype == "w":
				sampletype = "Writes"
			elif sampletype == "t":
				sampletype = "Total"
			else:
				raise AttributeError,"Unknown column sampletype of %s found...aborting" % sampletype

			if device == "GRANDTOTAL":
				keyname="Grandtotal_%s_%s" % (sampletype,devicename)
				dataclass="TOTAL"
			elif device.find("TOTAL") == -1:	
				#Focus on parsing individual disk devices only
				###########################
				for dev in self.devList:
					if device.find(dev) > -1:
						#Remove the host designation for now
						devicename=device.split("@")[1]
	
						keyname="%s_%s" % (sampletype,devicename)
						keyname=keyname.replace("/","")
						keyname=keyname.replace("\\","")
						keyname=keyname.replace(".","")
						dataclass="DEVICE"
			else:
				#print "SKIPPING %s" % keyname
				#SKip all other types
				continue
				

			#print "KEYNAME: %s" % keyname
			self.csvdata.add_data(timestamp,"%s_iops" % keyname,iops,dataclass)
			self.csvdata.add_data(timestamp,"%s_mbs" % keyname,mbs,dataclass)
			self.csvdata.add_data(timestamp,"%s_resp" % keyname,resp,dataclass)

	def parse_output(self,output):
		read_iops,write_iops,total_iops=0,0,0
		read_mbs,write_mbs,total_mbs=0,0,0
		read_latency,write_latency,total_latency=0,0,0
		cpu_user,cpu_system=0,0
		w=0
		read_mbs=0
		read_latency=0
		histogram=""

		dp_output_line=""

		found=0
		for row in output.split("\n"):
			if not row.strip():
				continue
			row=row.split("|")

			#If timestamps is present on the line, remove the timestamp and continue with previous logic
			if len(row) > 1 and row[1] == "GRANDTOTAL":
				row=row[1:]
			if row[0] == "GRANDTOTAL" and row[1] == "Tr":
				read_iops,read_mbs,read_latency,cpu_user,histogram=self.__parse_perf_data__(row)
				found=1
			elif row[0] == "GRANDTOTAL" and row[1] == "Tw":
				write_iops,write_mbs,write_latency,cpu_user,histogram=self.__parse_perf_data__(row)
				found=1
			elif row[0] == "GRANDTOTAL" and ( row[1] == "Tt" or row[1] == "T" ):
				total_iops,total_mbs,total_latency,cpu_user,histogram=self.__parse_perf_data__(row)
				found=1
			else:
				continue

		if read_iops and write_iops:
			self.detailed_results=(read_iops,read_mbs,read_latency.quantize(Decimal('0.01')),write_iops,write_mbs,write_latency.quantize(Decimal('0.01')))

		if not found:
			self.setErrorFound()
			print "##### %s #####" % output

		#If a sample interval > is defined, then a timeseries was taken
		#Parse the output from DP into the CSVPerfDAta structure and generate a report
		###############
		if self.sampleCount != 1:
			debug("### CSV OUTPUT",1)
			debug(output,2)
			self.parse_dp_output_for_timeseries(output)
			self.csvdata.report()
		else:
			debug("### NON-CSV OUTPUT",1)

		return total_iops,total_mbs,total_latency,cpu_user,histogram

			
	def run(self,fork=0,skipparse=0):
		if self.filldisks:
			fileText=self.__create_fill_text__()
		else:
			fileText=self.__create_file_text__()

		debug(fileText,1)
		#print fileText
		#print self.inputFile

		outputFile=file(self.inputFile,'w')
		outputFile.write(fileText+'\n')
		outputFile.close()
		seed=str(random.randrange(0,999999009))

		self.outputfile=time.strftime("%d_%b_%y_%H_%M_%S_"+seed+".dplog")

		cmd="vesper %(inputFile)s -o %(outputfile)s %(mapfile)s" % vars(self)
		self.cmd = cmd
		self.print_long_logfile("COMMAND: %s" % cmd)
	
		if fork:
			print "starting fork process"
			if hasattr(os.sys,'winver'):
				self.process=subprocess.Popen(cmd,stdout=PIPE,stderr=PIPE,creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
			else:
				self.process=subprocess.Popen(cmd.split(),stdout=PIPE)
			return self.process
		else:
			#process=subprocess.Popen(cmd,shell=True,close_fds=True,stdin=PIPE,stdout=PIPE,stderr=PIPE)
		#	print cmd
			try:
				process=subprocess.Popen(cmd,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE)
				#print "PID: %s" % process.pid
				output=""
				while process.poll() == None:
					time.sleep(1)
			except:
				try:process.kill()
				except: pass
		#	print process
			#output=os.popen(cmd).read()
			output=process.stdout.read().strip()
			errors=process.stderr.read().strip()
		#	print output
		#	print errors
			self.print_long_logfile(output)
			if errors:
				self.print_long_logfile("STDERR: %s" % errors)
				debug(errors,2)

			logfile=file(self.outputfile,'r')
			
			fileoutput=logfile.read()
			self.print_long_logfile(fileoutput)

			if skipparse:
				return_output="parse skipped"
			else:
				return_output= self.parse_output(fileoutput)

			logfile.close()

			#Remove/delete the .dplog file from the file system
			#os.remove(self.outputfile)
			self.__remove_mapfile__()
			return return_output

	def cleanup_process(self,LIMIT=60):

		#Check for updates..and poll process for final termination
		wait_60_seconds=0
		output=""
		print "starting loop"
		print self.process.poll()
		while self.process.poll()==None:
			#print "in loop"
			#line=self.process.stdout.read()
			#if line:
			#	output+=line
			#print self.process.poll()
			self.process.send_signal(signal.CTRL_BREAK_EVENT)
			time.sleep(1)
			wait_60_seconds+=1
			if wait_60_seconds == LIMIT:
				print "PROCESS POLL STATUS: [ %s ]" % str(self.process.poll())
				print "READ OUTPUT below: " 
			#	print line
				self.process.kill()
			elif wait_60_seconds == LIMIT*5:
				print "Still waiting after 5 minutes...doing something more invasive"
				print "PROCESS POLL STATUS: [ %s ]" % str(self.process.poll())
				print "READ OUTPUT below: " 
			#	print line
				self.process.terminate()
		print "outside of loop"
		print self.process.poll()
		output+=self.process.stdout.read()
		errors=self.process.stderr.read().strip()
		print "############"
		print "############"
		print "############"
		print "OUTPUT"
		print output
		print "ERRORS"
		print errors
		try:print self.cmd
		except:pass
		print "############"
		print "############"
		print "############"
		print "#######"
		if errors:
			print "STDERR: %s" % errors
			self.print_long_logfile("STDERR: %s" % errors)
		else:
			print "NO errors detected"
		self.print_long_logfile(output)
		print "Confirming DP is ready to exit"
		self.process.wait()
		print "Reading in DP outputfile %s" % self.outputfile
		fileoutput=file(self.outputfile).read()
		self.print_long_logfile(fileoutput)
		print "DP has exited, parsing output"
		return self.parse_output(fileoutput)
		
	def __check_and_handle_errors(self,output):
		"""
		Parses vdbench output for various known error conditions
		"""
		
		#Warnings and errors in vdbench have a "*" character at the beginning of the line
	
		for row in output.split("\n"):
			if not row.strip():
				continue
			if row.find("failed") > -1:
				self.setErrorFound()
				print "## ERROR: ",row
				if row.find("Invalid Argument") > -1:
					print "## ERROR: Consider disabling O_DIRECT (default)"

	def get_qdepth(self):
		return self.qdepth * self.deviceCount

	

		

class VDBENCH_Measurement(Measurement):
	cmd="vdbench"
	tool="vdbench"
	def __init__(self,*args,**kargs):
		Measurement.__init__(self,*args,**kargs)

		self.deviceCount=len(self.devList)
		self.tool="vdbench"

		#self.inputFile="%(tool)s_%(xfer)s_rdpct%(readpercent)s_seekpct%(seekpercent)s_%(qdepth)s_.%(tool)s" % vars(self)
		self.inputFile="%(tool)s_%(xfer)s_rdpct%(readpercent)s_seekpct%(seekpercent)s_%(qdepth)s_%(inputFilemytime)s_%(seed)s.%(tool)s" % vars(self)

		#SD = storage device. 
		self.sdConfigLines=""
		self.wdConfigLines=""
		self.sdIdsText=""
		self.wdIdsText=""
		self.maxiops=0

		self.interval=5		#default interval to 5 seconds
		#if the warmupTime is less than self.inteval or not evenly divisble, set interval=1
		if (self.warmupTime < self.interval) or (self.warmupTime % self.interval):
			self.interval=1

		#Convert transfer size in kb to bytes
		self.xferSize=self.xfer*2
		self.xferBlocksize=512
		self.xferBytes=self.xfer*1024

		self.extra=""
	def __translate_access__(self,access):
		if access == "sequential read":
			self.readpercent=100
			self.seekpercent="sequential"
		elif access == "sequential write":
			self.readpercent=0
			self.seekpercent="sequential"
		elif access == "random read":
			self.readpercent=100
			self.seekpercent="random"
		elif access == "random write":
			self.readpercent=0
			self.seekpercent="random"
		elif access == "OLTP":
			self.seekpercent="random"
		elif access == "SEQOLTP":
			self.seekpercent="sequential"
		elif access == "CUSTOM_OLTP":
			#Leave the default seek percent that was passed it
			pass
			#self.seekpercent="sequential"
		else:
			raise AttributeError,"Invalid access mode"

	def __setup_qdepth_and_disks__(self,fill=0):
		if self.aoConfig:
			#self.__setup_qdepth_and_sds_ao__()
			ConfigDetails=self.__setup_ao_qdepth_and_disks__()
			self.__setup_ao_qdepth_and_disks_config__(ConfigDetails)
		else:
			self.__setup_qdepth_and_sds__()
	def __setup_file_for_ao__(self):
		#mapfile,mapgroup_alias=self.__create_mapfile()
		#Use the first device from the device list...We assume there is only 1
		dev=self.devList[0]
		return dev
	def __setup_ao_qdepth_and_disks_config__(self,ConfigDetails):
		print "### AOCONFIG"
		deviceTextList=[]
		sdList=[]
		wdList=[]
		sdIdList=[]
		wdIdList=[]
		sdId=0

		#Get total iops
		self.maxiops=0
		maxoffset=0
		for disk_alias,qdepth,offset,delay in ConfigDetails:
			#config delay into iops
			self.maxiops += (float(1) / ((float(delay)/1000)/1000))*qdepth
			# make sure we at least have 32 blocks of data to access
			#if (offset[1]-offset[0]) < 8192:
			#	continue
			maxoffset=offset[1]


		lastoffset=[0.0,1.0]
		for disk_alias,qdepth,offset,delay in ConfigDetails:
			iops = (float(1) / ((float(delay)/1000)/1000))*qdepth
			#Revise the queue depth for VDBench, assume 1 thread is required for concurrency per 30 iops
			#per device
			################
			newqdepth=iops/30
			if newqdepth < 1:
				newqdepth=1
			priority=sdId+1
			print "Derived IOPS %s" % iops
			#offset percentage
			#start_offset=(offset[0]*16384)
			start_offset=(offset[0]*16384)
			stop_offset=(offset[1]*16384)

			# make sure we at least 1 full region to access
			#
			###############################
			#if (offset[1]-offset[0]) < 32:
			#	#If we have less than a region, double using the previous start offset
			#	print "WARNING: SMALL OFFSET FOUND FOR %s - %s - %s -%s" % (disk_alias,qdepth,offset,delay)
			#	raw_input()
			#	start_offset_blocks=lastoffset[0]
			#else:
			#	start_offset_blocks=(offset[0])
			#	lastoffset=offset
			start_offset_blocks=(offset[0])
			stop_offset_blocks=(offset[1])

			start_offset_percent=round(float(start_offset_blocks)/float(maxoffset),4)

			#if start_offset_percent < .0001:
			#	start_offset_percent = .0001
			stop_offset_percent=round(float(stop_offset_blocks)/float(maxoffset),4)

			if stop_offset_percent == start_offset_percent:
				stop_offset_percent+=.0001
			##jjif stop_offset_percent < .0001:
			#	stop_offset_percent = .0001

			skew = (iops/self.maxiops)*100
			sdList.append("sd=sd%s,lun=%s,threads=%s,range=(%s,%s)" % (sdId,disk_alias,int(newqdepth),start_offset,stop_offset))
			#sdList.append("sd=sd%s,lun=%s,threads=%s,range=(%s,%s)" % (sdId,disk_alias,int(newqdepth),start_offset_percent,stop_offset_percent))
			#wdList.append("wd=wd%s,sd=(sd%s),iorate=%s,priority=%s,xfersize=%s,rdpct=%s" % (sdId,sdId,iops,priority,self.xferBytes,self.readpercent))
			wdList.append("wd=wd%s,sd=(sd%s),skew=%s,priority=%s,xfersize=%s,rdpct=%s" % (sdId,sdId,skew,priority,self.xferBytes,self.readpercent))
			sdIdList.append("sd%s" % sdId)
			wdIdList.append("wd%s" % sdId)
			sdId+=1
		#self.deviceText="\n".join(deviceTextList)


		self.sdConfigLines="\n".join(sdList)
		self.wdConfigLines="\n".join(wdList)
		self.sdIdsText=",".join(sdIdList)
		self.wdIdsText=",".join(wdIdList)

		fileText="""
		%(sdConfigLines)s
		%(wdConfigLines)s
		wd=wd1,sd=(%(sdIdsText)s),xfersize=%(xferBytes)s,rdpct=%(readpercent)s
		rd=run1,wd=(%(wdIdsText)s),iorate=max,warmup=%(warmupTime)s,elapsed=%(measurementTime)s,distribution=det
		""" % vars(self)

	def __setup_qdepth_and_sds__(self):
		"""
		Creates lines of text for config file that define sd/storage devices and
		ensures that self.qdepth is appropriate for the number of sds being used

		self.qdepth must be evenly divisble by len(self.devList)
		"""

		sdList=[]
		sdIdList=[]
		sdId=0
		#self.deviceCount=0

		for dev in self.devList:
			#If using direct mode.  VD bench requires all /dev/* devices to be opened
			#with o_direct
			#
			#Should only be overridden if using files
			#####################
			if self.direct:
				if self.__is_sun_os__():
					#Don't specify an openflags parm on Solaris
					sd="sd=sd%(sdId)s,lun=%(dev)s" % vars()
				elif hasattr(os.sys,'winver'):
					#specify an openflags parm on of directio Windows
					sd="sd=sd%(sdId)s,lun=%(dev)s,openflags=directio" % vars()
				else:	
					sd="sd=sd%(sdId)s,lun=%(dev)s,openflags=o_direct" % vars()
			else:
				sd="sd=sd%(sdId)s,lun=%(dev)s" % vars()
			sdList.append(sd)

			sdIdList.append("sd%s" % sdId)

			sdId+=1

			#If we have more disks than qdepth, throw out disks not being used
			if sdId == self.qdepth:
				break

		self.deviceCount=sdId

		#If we have more devices than qdepth, resize the qdepth to 1.  This qdepth will translate to
		#1 process per device
		if self.deviceCount > self.qdepth:
			self.qdepth=1
		else:
			self.qdepth=self.qdepth/self.deviceCount

		self.sdConfigLines="\n".join(sdList)
		self.sdIdsText=",".join(sdIdList)

	def __translate_access__(self,access):
		#For now treat sequential and segmented as identical in vdbench
		if access == "sequential read":
			self.readpercent=100
			self.seekpercent="sequential"
		elif access == "segmented read":
			self.readpercent=100
			self.seekpercent="sequential"
		elif access == "sequential write":
			self.readpercent=0
			self.seekpercent="sequential"
		elif access == "segmented write":
			self.readpercent=0
			self.seekpercent="sequential"
		elif access == "random read":
			self.readpercent=100
			self.seekpercent="random"
		elif access == "random write":
			self.readpercent=0
			self.seekpercent="random"
		elif access == "OLTP":
			self.seekpercent="random"
		elif access == "SEQOLTP":
			self.seekpercent="sequential"
		elif access == "CUSTOM_OLTP":
			#LEave default and try to take custom seek percent
			pass 
			#self.seekpercent="sequentials"
		else:
			print "###"
			print access
			print "###"
			raise AttributeError,"Invalid access mode"
	def __check_and_handle_errors(self,output):
		"""
		Parses vdbench output for various known error conditions
		"""
		
		#Warnings and errors in vdbench have a "*" character at the beginning of the line
	
		for row in output.split("\n"):
			if not row.strip():
				continue
			if row.strip().split()[0] == "*":
				self.setErrorFound()
				print "## ERROR: ",row
			elif row.find("java.lang.RuntimeException") > -1:
				self.setErrorFound()
				print "## ERROR: ",row

	def __parse_perf_data__(self,output=[]):
		"""
		
		#Sample output, parsing interval line avg_6-35
				Jul 08, 2011  interval   i/o   MB/sec   bytes   read     resp     resp     resp    cpu%  cpu%
                                              	         rate  1024**2     i/o    pct     time      max   stddev sys+usr   sys
			     09:58:56.048        31    8635.00    33.73    4096 100.00    3.703   18.697    1.768     7.1   6.1
			     09:58:57.048        32    8471.00    33.09    4096 100.00    3.775   31.621    1.863     6.7   5.7
			     09:58:58.048        33    8553.00    33.41    4096 100.00    3.739   36.494    1.858     6.4   5.7
			     09:58:59.048        34    8525.00    33.30    4096 100.00    3.752   20.782    1.801     7.4   6.2
			     09:59:00.048        35    8515.00    33.26    4096 100.00    3.757   33.936    1.971     6.9   5.9
			     09:59:00.051  avg_6-35    8482.60    33.14    4096 100.00    3.770  372.672    2.940     7.4   5.8
			     09:59:00.610 Vdbench execution completed successfully. Output directory: /root/vdbench_test/output

		"""
		intervalTime=output[0]
		interval=output[1]
		total_iops=output[2]
		total_mbs=output[3]
		total_latency=output[6]
		cpu_total=Decimal(output[9])

		cpu_system=Decimal(output[10])
		cpu_user=cpu_total-cpu_system

		#convert latency to 2 decimal points
		total_latency=Decimal(total_latency).quantize(Decimal('0.01'))
		#total_latency=str(total_latency)+"ms"

		return total_iops,total_mbs,total_latency,cpu_user,cpu_system
	def __create_file_text_ao__(self):
		self.__setup_qdepth_and_disks__()

		fileText="""
		%(sdConfigLines)s
		%(wdConfigLines)s
		rd=run1,wd=(%(wdIdsText)s),iorate=%(maxiops)s,warmup=%(warmupTime)s,elapsed=%(measurementTime)s,distribution=deterministic
		""" % vars(self)

		debug(fileText,2)

		return fileText.strip()

	def __create_file_text__(self):

		self.__setup_qdepth_and_disks__()

		fileText="""
		%(sdConfigLines)s
		wd=wd1,sd=(%(sdIdsText)s),xfersize=%(xferBytes)s,rdpct=%(readpercent)s,seekpct=%(seekpercent)s
		rd=run1,wd=wd1,iorate=max,warmup=%(warmupTime)s,elapsed=%(measurementTime)s,interval=%(interval)s,threads=%(qdepth)s
		""" % vars(self)

		return fileText.strip()

	def get_socket_port(self):
		import random
		#If this is an AO config, generate a random socket number
		############
		#if self.aoConfig:
			#Generate a random port between 5000 and 8000, and try to see if it works
			#why not?  
		return random.randrange(5000,8000)
		#else:
		#	return "5570"

	def run(self,fork=None):
		#fileName="vdbench_%(xfer)s_rdpct%(readpercent)s_seekpct%(seekpercent)s_%(qdepth)s_%(ioengine)s.vdbench" % vars(self)
		if self.aoConfig:
			fileText=self.__create_file_text_ao__()
			self.extracli="-m 1"
		else:
			fileText=self.__create_file_text__()
			self.extracli=""

		debug(fileText,1)

		outputFile=file(self.inputFile,'w')
		outputFile.write(fileText)
		outputFile.close()

		bufferoutputfile="buffer_%s" % self.inputFile
		self.socketport=self.get_socket_port()
	
		if hasattr(os.sys,'winver'):
			cmd="c:\\perf\\LabPerfTool\\tools\\vdbench\\vdbench %(extracli)s -f %(inputFile)s -p %(socketport)s" % vars(self)
		else:
			cmd="../LabPerfTool/tools/vdbench/vdbench %(extracli)s -f %(inputFile)s -p %(socketport)s" % vars(self)
		debug(cmd,2)
		self.print_long_logfile("COMMAND: %s" % cmd)
		if fork:
			print "starting fork process"
			if hasattr(os.sys,'winver'):
				cmd="%s > %s" % (cmd,bufferoutputfile)
				self.process=subprocess.Popen(cmd,shell=True,stdout=PIPE,stderr=PIPE,creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
			else:
				self.process=subprocess.Popen(cmd.split(),stdout=PIPE)
			return self.process
		try:
			process=subprocess.Popen(cmd,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE)
			#print "PID: %s" % process.pid
			output,errors=process.communicate()
			#output=""
			#while process.poll() == None:
			#	print process.poll()
			#	time.sleep(1)
		except:
			try:
				output=process.stdout.read().strip()
				errors=process.stderr.read().strip()
			except:
				output=""
				errors=""
			try:process.kill()
			except: pass
	#	print process
		#output=os.popen(cmd).read()
		output=output.strip()
		errors=errors.strip()
		#output=process.stdout.read().strip()
		#errors=process.stderr.read().strip()
		debug(output,2)
		debug(errors,2)

		#output=os.popen(cmd).read()
		self.print_long_logfile(output)

		self.__check_and_handle_errors(output)

		
		vdbench_average=[]
		vdbench_average_line=""
		for row in output.split("\n"):
			if not row.strip():
				continue
			row=row.split()
			if len(row) > 2 and row[1].find("avg") > -1:
				vdbench_average_line=row


		#check if we parsed the output correctly.  If not, return all zeros
		if vdbench_average_line:
			total_iops,total_mbs,total_latency,cpu_user,cpu_system=self.__parse_perf_data__(vdbench_average_line)
			return total_iops,total_mbs,total_latency,cpu_user,cpu_system
		else:
			total_iops=0
			total_mbs=0
			total_latency=0
			cpu_user=0
			cpu_system=0
		return total_iops,total_mbs,total_latency,cpu_user,cpu_system
	def get_qdepth(self):
		return self.qdepth * self.deviceCount
			

class FIO_Measurement(Measurement):
	cmd="fio"
	tool="fio"

	def __init__(self,*args,**kargs):
		Measurement.__init__(self,*args,**kargs)

		self.deviceFiles=":".join(self.devList)
		self.fioxRunTime=self.warmupTime+self.measurementTime
		self.deviceCount=len(self.devList)

		self.tool="fio"
		self.inputFile="%(tool)s_%(xfer)s_rdpct%(readpercent)s_seekpct%(access)s_%(qdepth)s_.%(tool)s" % vars(self)
		self.extra=""


	def __translate_access__(self,access):
		if access == "sequential read":
			self.access="read"
			self.readpercent=100
		elif access == "sequential write":
			self.access="write"
			self.readpercent=0
		elif access == "random read":
			self.access="randread"
			self.readpercent=100
		elif access == "random write":
			self.access="randwrite"
			self.readpercent=0
		elif access == "OLTP":
			self.access="randrw"
		elif access == "SEQOLTP":
			self.access="rw"
		elif access == "fill":
			self.access="fill"
		else:
			raise AttributeError,"Invalid access mode"
	
	def getjson(self,output):	
		json=""
		start=0
		for row in output.split("\n"):	
			if row.strip() == "{":
				start=1
			if start:
				json+=row+"\n"
		return json

	def __check_and_handle_errors(self,output):
		"""
		Parses FIO output for various known error conditions
		"""
		import json
		#print [self.getjson(output)]
		fio_output=json.loads(self.getjson(output))
		try:checkoutput=fio_output['jobs'][0]
		except:checkoutput=fio_output['client_stats'][0]

		if int(checkoutput['error']) > 0 or output.find("failed") > -1:
			print "#############################"
			print "## ERRORS during execution ##"
			print "#############################"
			self.setErrorFound()
			print "#### %s ####" % output
			for row in output.split("\n"):
				if row.find("size doesn't match") > -1:
					print "ERROR: ### Disk devices included in map are of different sizes.  Edit your AO group input file and move the device below to a different group"
					print "ERROR: #### %s" % row
				if row.find("error") > -1 or row.find("failed") > -1:
					if row.find("Too many open files") >-1:
						print "###"
						print "## ",row
						print "## FIOX FIX: Run 'ulimit -a' and look at open files -n parm.  Consider increasing, e.g. 'ulimit -n 4096'"
						print "###"

					else:
						print "## ",row
			print "###########################"

	def __parse_perf_data__(self,output):
		"""
		#NOTE: 6/15/2018 - Changing to support json output format - JJS
		Parses FIO json output for:
			total iops, total MB/sec, total I/O latency, cpu_user %, cpu_system %

		"""

		#helps sort percentiles from multiple clients by taking the 'max' value from each clients
		#independent percentiles
		#
		#It then will normalize by the normalize factor, by default 1000000, which converts
		#from nanoseconds to milliseconds
		##############
		def get_percentile(x,latency_percentile,iotype='read',normalize=1000000):
			#modify the passed in latency histogram
			ptile=x[iotype]['clat_ns']['percentile']
			i=0
			for bin in sorted(ptile):
				lat=round(float(ptile[bin])/normalize,4)
				if latency_percentile[i] < lat:
					latency_percentile[i]=lat
				i+=1

		#Extract read  kb/sec, and divide by transfer size to get IOP/s, and convert for MB/s
		import json
		json_output=json.loads(self.getjson(output))
		self.read_latency_percentile=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
		self.write_latency_percentile=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

		try:
			job=json_output["jobs"][0]
			get_percentile(job,self.read_latency_percentile,'read')
			get_percentile(job,self.write_latency_percentile,'write')
		except:
			job=None
			for x in json_output['client_stats']:
				if str(x['jobname']) == "All clients":
					job=x
					break
				else:
					get_percentile(x,self.read_latency_percentile,'read')
					get_percentile(x,self.write_latency_percentile,'write')
			if job == None:
				job=json_output["client_stats"][0]
		#readTotalKB_transferred=output[4]
		readTotalKB_transferred=job["read"]["io_bytes"]
		#read_kbs=output[5]
		read_kbs=job["read"]["bw"]
		read_iops=job['read']['iops']
		read_mbs=int(read_kbs)/1000

		#Extract write  kb/sec, and divide by transfer size to get IOP/s, and convert for MB/s
		writeTotalKb=job["write"]["io_bytes"]
		write_kbs=job['write']['bw']
		write_iops=job['write']['iops']
		write_mbs=int(write_kbs)/1000


		#Extract Read adn Write latencies, convert from microseconds to milliseconds
		#read_latency=Decimal(output[17])/1000
		#write_latency=Decimal(output[37])/1000
		read_latency=Decimal(job['read']['lat_ns']['mean'])/1000/1000
		write_latency=Decimal(job['write']['lat_ns']['mean'])/1000/1000
	
		#Derived Total IOPS and MB/s
		total_iops=read_iops+write_iops
		total_mbs=read_mbs+write_mbs


		#Derived total latency by combining read/write latency
		if read_latency and write_latency:
			total_latency=(read_latency+write_latency)/2
		elif read_latency:
			total_latency=read_latency
		elif write_latency:
			total_latency=write_latency
		else:
			print "ERROR: NO LATENCY: output:%s" % (json_output)
			print "Consider running command by hand and looking for errors"
			total_latency=Decimal(0)

		#Convert total latency to 2 decimal conversion
		total_latency=(total_latency).quantize(Decimal('0.01'))
		#total_latency=str(total_latency)+"ms"

		#cpu_user=Decimal(output[44].replace("%","")).quantize(Decimal('0.1'))
		#cpu_system=Decimal(output[45].replace("%","")).quantize(Decimal('0.1'))
		cpu_user=job['usr_cpu']
		cpu_system=job['sys_cpu']


		#only define self.detailed_results if we have read and write metrics to report
		if read_iops and write_iops:
			self.detailed_results=(read_iops,read_mbs,read_latency.quantize(Decimal('0.01')),write_iops,write_mbs,write_latency.quantize(Decimal('0.01')))

		total_iops=Decimal(str(total_iops)).quantize(Decimal('0.1'))

		return total_iops,total_mbs,total_latency,cpu_user,cpu_system

	def get_extra_fio_parms(self):
		extra=[]

		if self.extraOptions.size_of_files:
			extra.append("filesize=%s" % self.extraOptions.size_of_files)
			extra.append("size=%s" % self.extraOptions.size_of_files)

		if self.extraOptions.histo_timeseries:
                    extra.append("write_hist_log=z")
                    extra.append("log_hist_coarseness=z")
                    extra.append("log_hist_msec=1000")

		#If we are running a mixture of reads and writes
		if self.readpercent != 100 and self.readpercent != 0:
			extra.append("rwmixread=%s" % self.readpercent)
		#if self.extraOptions.num_of_files:
		#	extra.append("nrfiles=%s" % self.extraOptions.num_of_files)


		if self.extraOptions.ioengine:
			self.ioengine=self.extraOptions.ioengine
		if self.extraOptions.numtcpportsperhost:
			self.numtcpPortsPerHost=int(self.extraOptions.numtcpportsperhost)

		self.extra="\n".join(extra)

	def __remove_from_fileText__(self,fileText,removeText):
		newtext=[]
		for row in fileText.split("\n"):
			if row.find(removeText) == -1:
				newtext.append(row)
		return "\n".join(newtext)
			
	def __create_file_multihost__(self,numPerHost=1):

		cliArgs=" "

		#fileText=self.__create_file_text__(empty=True)
		
		#NOTE: Hardcode FIO to port 4444 on all clients
		tcpPort=4444

		for host,deviceFileList in self.hostConfig.items():
			fileText=self.__create_file_text__(empty=True)
			hostFioConfig_filename="%s_fio.cfg" % host

			if self.ioengine == "rbd":
				jobid=0
				fileText=self.__remove_from_fileText__(fileText,"deviceFiles")
				fileText=self.__remove_from_fileText__(fileText,"deviceCount")

				hostFioConfig=str(fileText) % vars(self)
				for devFile in self.ioengineConfig[host]:
					jobname="host.%s" % jobid
					pool=devFile.split(".")[0]
					rbdname=".".join(devFile.split(".")[1:])
					hostFioConfig+="\npool=%s\nrbdname=%s" % (pool,rbdname)
					jobid+=1

			else:
				self.deviceFiles=":".join(deviceFileList)
				hostFioConfig=str(fileText) % vars(self)

			file(hostFioConfig_filename,'w').write(hostFioConfig)

			for i in range(0,numPerHost):
				cliArgs+="--client=%s,%s %s " % (host,tcpPort+i,hostFioConfig_filename)

		self.deviceFiles=""

		return cliArgs
			
	def __create_file_text__(self,empty=False):
		self.get_extra_fio_parms()
                self.warmupText="#No Warmup specified - skipping"
                if self.warmupTime and int(self.warmupTime) > 0:
                    self.warmupText="""
## Work around some buggy FIO behavior, double warmup time to compensate
#[job_warmup]
#ramp_time=%(warmupTime)s
numjobs=%(qdepth)s
runtime=%(warmupTime)s
stonewall
                    """


		fileText="""
[global]
ioengine=%(ioengine)s
rw=%(access)s
bs=%(xfer)sk
direct=%(direct)s
group_reporting
time_based

#Ensure a unique random seed is used for each measurement.  By default, FIO uses the same seed. 
randrepeat=0

#Set to support more processes
norandommap

#set to use threads instead of processes
#thread

filename=%(deviceFiles)s
nrfiles=%(deviceCount)s
###Extra parameters from command line
%(extra)s
###End extra parameters

%(warmupText)s

##Actual measurement
[job2_measurement]
#ramp_time=%(warmupTime)s
runtime=%(fioxRunTime)s
numjobs=%(qdepth)s
stonewall
""" 
		if not empty: fileText=fileText % vars(self)
		return fileText


	def run(self,fork=None):
		#fileName="fio_%(xfer)s_%(access)s_%(qdepth)s_%(ioengine)s.fio" % vars(self)

		self.get_extra_fio_parms()

		if len(self.hostConfig.keys()) > 0:
			#print "We have multiple hosts"

			cliArgs=self.__create_file_multihost__(self.numtcpPortsPerHost)
			cmd="fio --output-format=json %(cliArgs)s" % vars()
			#print cmd
		else:
			fileText=self.__create_file_text__()
			debug(fileText,1)

			outputFile=file(self.inputFile,'w')
			outputFile.write(fileText)
			outputFile.close()
			cmd="fio --output-format=json %(inputFile)s" % vars(self)

		#cmd="fio --minimal %(inputFile)s" % vars(self)
		#cmd="fio --output-format=json %(inputFile)s" % vars(self)
		self.print_long_logfile("COMMAND: %s" % cmd)

		output=os.popen(cmd).read()

		self.print_long_logfile(output)

		self.__check_and_handle_errors(output)

		#fio_output_line=output.split("\n")[-2].split(";")

		#total_iops,total_mbs,total_latency,cpu_user,cpu_system=self.__parse_perf_data__(fio_output_line)
		total_iops,total_mbs,total_latency,cpu_user,cpu_system=self.__parse_perf_data__(output)

		return total_iops,total_mbs,total_latency,cpu_user,cpu_system


class OptimalPerformance:
	def __init__(self,qdepths=[],accessList=["randread","randwrite","read","write"],xferList=[8,128],fixedQdepths=1,stdoutWrapper=None):

		if not qdepths:
			self.qdepths=[1,2,4,8,16,24,32,48,65,96]
		else:
			self.qdepths=qdepths

		self.fixedQdepths=fixedQdepths	#Default, only use qdepths in self.qdepths.  Disable to autosense response times
		self.accessList=accessList
		self.xferList=xferList
		self.targetResponseTime=30 #Time in milliseconds to target
		self.warmupTime=60
		self.measurementTime=120
		self.extraOptions=[]
		self.devList=[]
		self.IOTool=None
		self.restTime=30
		self.skipcleanup=0
		self.logfile_id=None

		#Setup capture tool, by default the class does not define any functionality, so all calls
		#will result in an immediate completion
		############
		self.capturetool = FioxCaptureTool()
		#self.histogram_buckets=[1,2,3,4,5,6,7,8,9,10,12,14,16,18,20,25,30,35,40,50,60,70,80,90,100,120,150,200,500,750,1000]
		#self.histogram_buckets=[5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100,200,400,800,1000]
		#Now used for percentile bins
		#self.histogram_buckets=[1,5,10,20,30,40,50,60,70,80,90,95,99,99.5,99.9,99.95,99.99]
		#lets limit this to 90 and above
		self.histogram_buckets=[90,95,99,99.5,99.9,99.95,99.99]

		self.stdoutWrapper=stdoutWrapper	#Provide wrapper around stdout to control prints to sys.stdout and log redirection

	def setup_capture_tool(self,capture_tool_obj):
		self.capturetool=capture_tool_obj

	def __arg_parse_workload__(self,option,opt_str_original,value,parser):
		#Pythonversion 2.4 does not support the append_const action in OptionParser.  This method
		#provides similiar functionality
		opt_str=opt_str_original.replace("-","")
		if not parser.values.workloads:
			parser.values.workloads=[]

		#This is rather repetative, but itsneeded for command line checks
		#we cannot setup self.accessList here because the order commands are supplied on the CLI
		#can be random.  Mixtures could be supplied needed for OLTP.  
		#its better just to be excessive here ensure we build up the correct structure here
		####################
		if opt_str in ["rr","ranread"]:
			parser.values.workloads.append("rr")
		elif opt_str in ["rw","ranwrite"]:
			parser.values.workloads.append("rw")
		elif opt_str in ["sr","seqread"]:
			parser.values.workloads.append("sr")
		elif opt_str in ["sw","seqwrite"]:
			parser.values.workloads.append("sw")
		elif opt_str in ["segr","segread"]:
			parser.values.workloads.append("segr")
		elif opt_str in ["segw","segwrite"]:
			parser.values.workloads.append("segw")
		elif opt_str in ["oltp"]:
			parser.values.workloads.append("oltp")
		elif opt_str in ["seqoltp"]:
			parser.values.workloads.append("seqoltp")
		elif opt_str in ["segoltp"]:
			parser.values.workloads.append("segoltp")
		elif opt_str in ["custom_oltp"]:
			parser.values.workloads.append("custom_oltp")
		else:
			print "ERROR: Invalid input %s" % opt_str_original
			sys.exit()


	def check_usage(self,parser=None,cli_args=[]):
		usage = """
%prog [options] <devices>|<filenames>

###
# EXAMPLE:
# 	%prog /dev/dm-1 /dev/dm-2
# 	%prog --fillstop /dir1/myFile1 /dir1/myFile2
#	SEE README for more
#
###"""
		if parser == None:
			parser = OptionParser(usage)
		#parser.add_option("-d", "--devices", 	dest="devices",		help="Space delimitered list of device files or file names to test",action="append")
		#parser.add_option("-c","--create",	dest="create",		help="Create files in the directories specified on command line")
		#parser.add_option("-n","--number",	dest="create_number",	help="Specify the number of files to create in each directory")
		parser.add_option("--ioengine",		dest="ioengine",	help="FIO ioengine to use.  Default is sync",type="string"),
		parser.add_option("--numtcp",		dest="numtcpportsperhost",help="Num TCP ports per host. Defaults to 1",type="string"),
		parser.add_option("--hosts",		dest="hostlist",	help="List of Hosts to run against.  Default is localhost",type="string"),
                parser.add_option("--sar",		dest="sarcapture",	help="Enables capture of 1 second granular SR data for all [hosts|localhost]",action='store_true'),
                parser.add_option("--mons",		dest="monlist",		help="List of extra systems like ceph nodes to monitor",type="string"),
                parser.add_option("--histo_timeseries",	dest="histo_timeseries",help="(FIO-doesnotwork)histogram timeseries for each measurement",action='store_true'),
                #parser.add_option("--sa",	        dest="regsam",          help="capture --regsam [#] number of samples for each queue depth, using me as the sample size",action='store_true'),
                parser.add_option("--ao_input",		dest="aoConfig",	help="Filename and ao policy to use.  --ao_input <aosample.csv>,<evening_policy>,<total_capacity_in_gb>.  Default is diabled",type="string"),
                parser.add_option("-x","--xferlist",	dest="xferlist",	help="List of I/O transfer sizes to use.  Default: [8,128]",type="string"),
                parser.add_option("-q","--tl",		dest="qdepthlist",	help="List of Qdepths to start with. Default: [1,2,4,8,12,16,32,64]",type="string"),
                parser.add_option("-s","--sizeOfFile",	dest="size_of_files",	help="Specify the the size of each file created in each directory, when creating files for first time.")
                #parser.add_option("-n","--numOfFiles",	dest="num_of_files",	help="Specify the the number of files to create of size -s")
                parser.add_option("-r","--rsplimit",	dest="responselimit",	help="Response time in milliseconds to give up measuring past, default 60, (60ms)", type="int")
                parser.add_option("--demo",		dest="demo",		help="Overrides parameters to ensure a very short duration - For Demo purposes only. Do NOT make sizing decisions based on these results",action="store_true")
                parser.add_option("--fq","--fixedqd",	dest="fixedqd",		help="Use Fixed queue depths, and ignore response time limits",action="store_true")
                parser.add_option("--iops","--iopgoal",	dest="iop_override",	help="Specify a single IOP Goal to use, intended for use with aotest.py.  Not compatible with --fq or -q",type="int")
                parser.add_option("--wu",		dest="warmup",		help="Warmup time before, measurement begins, in seconds.  default 30 (30s)",type="int")
                parser.add_option("--me",		dest="measurement",	help="Measurement time - length of time to ru nmeasurement, in seconds. default 60 (60s)",type="int")
                parser.add_option("--cd",		dest="cooldown",	help="Cooldown (Rest Time) between measurements to let system recover, default 30 (30s)",type="int")
                parser.add_option("--sa",		dest="sampleinterval",	help="Length of time for each sample.  Defaults to measurement period",type="int")
                parser.add_option("--tool",		dest="tool",		help="specifies name of tool to use: [fio|vdbench] default: fio",type="string")
                parser.add_option("--all",		dest="all",		help="include --rr, --rw, --sr,--sw,--oltp",action='store_true')
                parser.add_option("--rr","--ranread",	dest="workloads",	help="Measure random reads",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--rw","--ranwrite",	dest="workloads",	help="Measure random writes",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--sr","--seqread",	dest="workloads",	help="Measure sequential reads",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--sw","--seqwrite",	dest="workloads",	help="Measure sequential writes",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--segr","--segread",	dest="workloads",	help="Measure segmented sequential reads ( DP only!)",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--segw","--segwrite",dest="workloads",	help="Measure segmented sequential writes ( DP only!)",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--oltp",		dest="workloads",	help="Measure Random (mixed reads/writes) with specified percentages (-m --mix)",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--custom_oltp",	dest="workloads",	help="Measure Random (mixed reads/writes) with specified percentages (-m --mix) and seek percent(--seekper)",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--seqoltp",		dest="workloads",	help="Measure Sequential (mixed reads/writes) specified percentages (-m --mix)",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("--segoltp",		dest="workloads",	help="Measure segmented Sequential (mixed reads/writes) specified percentages (-m --mix) ( DO only!)",action="callback",callback=self.__arg_parse_workload__)
                parser.add_option("-m","--mix",		dest="mixtures",	help="Override default Read percentage for OLTP measurements.  80,60 means 1x80% read measurement, 1x60% read measurement",type="string")
                parser.add_option("--seekper",		dest="seekper",		help="Percentage of seeking for custom oltp",type="string")
                parser.add_option("-o","--offset",	dest="offset",		help="Limit addressable space to a set of block multiple addressess  (DP only!)",type="string")
                parser.add_option("--o_sync",		dest="o_sync",		help="Open devices/files using O_SYNC instead of O_DIRECT",action="store_true")
                parser.add_option("--fill",		dest="fill",		help="Fill the device/files specified using a sequential pattern then begin measurement",action="store_true")
                parser.add_option("--fillstop",		dest="fillstop",	help="Same as --fill, but stop after fill completes. Do not run measurements",action="store_true")
                parser.add_option("--ds","--debug_skipcleanup",	dest="skipcleanup",	help="Skips cleanup of workload generator files.",action="store_true"),
                parser.add_option("--msg",		dest="expmsg",		help="Specify message describing this experiment.  Added to logfile",type="string")
                parser.add_option("--pattern",		dest="pattern",		help="Specify target dedupe ratio. 0=deault,1=1:1,2=2:1,3=3:1 default is default behavior for specified tools",type="int")
                parser.add_option("--cachehit",		dest="cachehit",	help="Specifiy cachehit and depth ratio separated by comma.  --cachehit 10,100 (10% hit, 100 depth) Disabled by default.  ",type="string")
                parser.add_option("--align",		dest="align",		help="Override alignment, specify blocksize of alignment in bytes ",type="string")
                if cli_args:
                    (options,args) = parser.parse_args(args=cli_args)
                else:
                    (options,args) = parser.parse_args()

                debug(options,1)
                debug(args,1)
                print options
                print args

                self.devList=args
                #if options.hostlist == None:
                #	options.hostlist='127.0.0.1'

                if len(args) < 1 and options.hostlist == None and not cli_args:
                    print "Exiting"
                    return 0
                self.setExtraOptions(options)

                self.logfile_config_name()

                return 1

        def get_dev_list(self):
            return self.devList


        def get_logfile_id(self,testclass="op",force=False):
            if self.logfile_id == None or force == True:
                self.logfile_id="%s.%s" % (testclass,time.strftime("%y%m%d.%H%M%S"))
                return self.logfile_id

        def logfile_config_name(self):

                try: os.mkdir("./op_logs/")
                except:pass

                if self.experiment_message.strip():
                    msg=".%s" % self.experiment_message.replace(" ","_").strip()
                    self.get_logfile_id(testclass="op")
                else:
                    msg=""
                    self.get_logfile_id(testclass="exp")

                logfile="./op_logs/%s%s.log" % (
                        self.logfile_id,
                        msg
                        )
                print logfile
                self.stdoutWrapper.add_logfile(logfile)

        def __config_demo__(self):
            if self.extraOptions.demo:
                self.warmupTime=2
                self.measurementTime=5
                #NOTE: JJS
                #self.sampleCount=self.measurementTime
                self.sampleCount=1#self.measurementTime
                self.sampleInterval=self.measurementTime
                self.restTime=2
                self.cooldownTime=2
                self.targetResponseTime=10

        def __parse_comma_list__(self,commaList,objtype=Decimal):
                """
                Parses a comma separated list as input from the command line.  Terminates program through sys.exit() if an error is found
                """
                itemList=[]
                for item in commaList.split(","):
                        if not item.strip():
                                continue
                        try:
                                itemValue=objtype(item)
                                itemList.append(itemValue)
                        except:
                                traceback.print_exc()
                                print "ERROR: Invalid integer %s in list %s" % (item,commaList)
                                sys.exit()
                return itemList


        def __parse_workload_list(self):
                if "rr" in self.extraOptions.workloads:
                        self.accessList.append(["random read",100,100])
                if "rw" in self.extraOptions.workloads:
                        self.accessList.append(["random write",0,100])
                if "sr" in self.extraOptions.workloads:
                        self.accessList.append(["sequential read",100,0])
                if "segr" in self.extraOptions.workloads:
                        self.accessList.append(["segmented read",100,0])
                if "segw" in self.extraOptions.workloads:
                        self.accessList.append(["segmented write",0,0])
                if "sw" in self.extraOptions.workloads:
                        self.accessList.append(["sequential write",0,0])
                if "oltp" in self.extraOptions.workloads:
                        for mix in self.mixtureList:
                                self.accessList.append(["OLTP",mix,100])
                if "seqoltp" in self.extraOptions.workloads:
                        for mix in self.mixtureList:
                                self.accessList.append(["SEQOLTP",mix,0])
                if "segoltp" in self.extraOptions.workloads:
                        for mix in self.mixtureList:
                                self.accessList.append(["SEGOLTP",mix,0])
                if "custom_oltp" in self.extraOptions.workloads:
                        for mix in self.mixtureList:
                                #print self.seekPercentList
                                for perc in self.seekPercentList:
                                        self.accessList.append(["CUSTOM_OLTP",mix,perc])


        def __check_hosts_config_files__(self,hostList):
                """
                Checks if all hosts in self.hostList have .disk config files

                Will raise AttributeError if any problems are detected
                """

                #If no hosts are defined, return true
                if not hostList:
                        return 1

                self.hostConfigs={}
                self.ioengineConfigs={}
                errors=[]

                for host in hostList:
                        if self.extraOptions.sarcapture:
                                self.capturetool.add_host(host)
                        #Replace all decimal points with underscores incase IP address was supplied
                        ############
                        filelabel=host.replace(".","_")

                        filename="%s.disk" % filelabel

                        filedata=file(filename,'r').read()

                        if not filedata:
                                errors.append("Host %s missing file %s or file is empty" % (host,filename))
                        else:
                                #Create a list of device targets for this host
                                #####################
                                self.hostConfigs[host]=[]
                                self.ioengineConfigs[host]=[]

                                count=0
                                for row in filedata.split("\n"):
                                        row=row.strip()
                                        if not row or row[0] == "#":
                                                continue
                                        row=row.split()
                                        #check for Windows style disk
                                        if row[0].find("PHYSICALDRIVE") > -1:
                                                self.hostConfigs[host].append(row[0])
                                                count+=1
                                        #check for unix/linux style disk
                                        elif row[0].find("/dev/") > -1:
                                                self.hostConfigs[host].append(row[0])
                                                #Assume the last column is a logica/software defined path, 
                                                #For librbd, we assume the path is <pool>.<rbdimage>
                                                self.ioengineConfigs[host].append(row[-1])
                                                count+=1
                                        else:
                                                continue
                                if not count:
                                        errors.append("Host %s missing valid device files in %s" % (host,filename))

                if not errors:
                        return 1
                else:
                        for row in errors:
                                print "ERROR: %s" % row
                        raise AttributeError,"Invalid hosts config files"


        def add_multiple_hosts(self,hostList=[]):
                self.hostConfigs={}
                self.ioengineConfigs={}
                self.__check_hosts_config_files__(hostList)

        def refresh_hosts_config_files(self,hostList):
                self.__check_hosts_config_files__(hostList)

        def setExtraOptions(self,options):
                debug("setExtraOptions called")
                self.extraOptions=options

                if self.extraOptions.responselimit:
                        self.targetResponseTime=int(self.extraOptions.responselimit)

                #Retrieve optional qdepths from the command line
                if self.extraOptions.qdepthlist:
                        self.qdepths=self.__parse_comma_list__(self.extraOptions.qdepthlist)

                if self.extraOptions.iop_override:
                        if self.extraOptions.qdepthlist:
                                print "ERROR: Unable to use qdepths and IOPS at the same time, please abort your command and use -iops or -q, not both"
                                raise AttributeError,"Unable to use qdepth and IOPS simultaneously"
                        else:
                                #Assume IOP Goal is reasonable
                                #Secondly, assume 50ms baseline latency
                                LATENCY_BASELINE=0.05
                                self.iops_override=int(self.extraOptions.iop_override)
                                #Run a single queue depth only
                                self.qdepths=[self.iops_override*LATENCY_BASELINE]
                                #if self.iops_override > 250:
                                #	self.usetimer="usetimer = true"
                                ##else:
                                #	self.usetimer = "usetimer = system"

                                print "LATENCY BASELINE %s" % LATENCY_BASELINE
                                print "qdepths  %s" % str(self.qdepths)
                                print "IOPS: %s" % self.iops_override
                else:
                        self.iops_override=None

                #Get the list of transfer sizes
                if self.extraOptions.xferlist:
                        self.xferList=self.__parse_comma_list__(self.extraOptions.xferlist)

                if self.extraOptions.aoConfig:
                        debug("SETTING AO CONFIG")
                        self.aoConfig=self.extraOptions.aoConfig.split(",")
                else:
                        debug("SETTING AO CONFIG TO EMPTY")
                        debug(self.extraOptions)
                        self.aoConfig=[]
                #get list of extra systems to monitor
                if self.extraOptions.monlist:
                        monList=self.__parse_comma_list__(self.extraOptions.monlist,objtype=str)
                        for mon in monList:
                                if self.extraOptions.sarcapture:
                                        self.capturetool.add_host(mon)
                #Get the list of Hosts (if any)
                if self.extraOptions.hostlist:
                        hostList=self.__parse_comma_list__(self.extraOptions.hostlist,objtype=str)
                        self.add_multiple_hosts(hostList)
                else:
                        self.hostConfigs={}
                        self.ioengineConfigs={}
                        self.hostList=[]

                #Get the list of transfer sizes
                if self.extraOptions.skipcleanup:
                        self.skipcleanup=1
                else:
                        self.skipcleanup=0

                #Set self.fixedQdepths and disable autosensing response times
                if self.extraOptions.fixedqd:
                        self.fixedQdepths=1

                #Set self.fixedQdepths and disable autosensing response times
                if self.extraOptions.offset:
                        self.offset = "["+self.extraOptions.offset+"]"
                else:
                        self.offset= "[0.0,1.0]"

                if self.extraOptions.seekper:
                        self.seekPercentList=self.__parse_comma_list__(self.extraOptions.seekper)
                else:
                        #No Seek value was specified. SHould default to default beahvior according to other workload specs
                        self.seekPercentList=[-1]

                #get the list of read/write mixtures to use on OLTP runs
                if self.extraOptions.mixtures:
                        self.mixtureList=self.__parse_comma_list__(self.extraOptions.mixtures)
                else:
                        self.mixtureList=[60,80]

                #Parse workloads and read/write mixtures
                if self.extraOptions.workloads:
                        self.accessList=[]
                        self.__parse_workload_list()
                else:
                        self.accessList=[
                                ["random read",100,100],
                                ["random write",0,100],
                                ["sequential read",100,0],
                                ["sequential write",0,0],
                        ]
                        for mix in self.mixtureList:
                                self.accessList.append(["OLTP",mix,100])

                if self.extraOptions.warmup:
                        self.warmupTime=self.extraOptions.warmup

                if self.extraOptions.measurement:
                        self.measurementTime=self.extraOptions.measurement

                if self.extraOptions.cooldown:
                        #Rename self.restTime to self.cooldownTime
                        self.restTime=self.extraOptions.cooldown

                if self.extraOptions.expmsg:
                        self.experiment_message=self.extraOptions.expmsg
                else:
                        self.experiment_message=""

                if self.extraOptions.pattern:
                        self.pattern=self.extraOptions.pattern
                else:
                        self.pattern=0
                if self.extraOptions.cachehit:
                        self.cachehit=self.extraOptions.cachehit.split(",")
                else:
                        self.cachehit=[0,0]

                #If no sample rate is specified, assume 1 sample over the measurement interval		
                #NOTE JJS
                if self.extraOptions.sampleinterval:
                        #self.sampleCount=self.extraOptions.sampleinterval
                        if self.measurementTime % self.extraOptions.sampleinterval:
                                print "######"
                                print "#ERROR: Invalid measurement and sampleinterval specified.  Measurement time must be evenly divisble by sample time"
                                print "Measurement: %s    Sample: %s" % (self.measurementTime,self.extraOptions.sampleinterval)
                                print "######"
                                sys.exit()
                        self.sampleCount=self.measurementTime/self.extraOptions.sampleinterval
                        self.sampleInterval=self.extraOptions.sampleinterval
                else:
                        self.sampleCount=1#self.measurementTime

                        self.sampleInterval=self.measurementTime

                self.fioxRunTime=self.warmupTime+self.measurementTime
                if self.extraOptions.tool:
                        if self.extraOptions.tool == "fio":
                                self.IOTool=FIO_Measurement
                                self.IOTool_text="FIO"
                        elif self.extraOptions.tool == "vdbench":
                                self.IOTool=VDBENCH_Measurement
                                self.IOTool_text="Vdbench"
                        elif self.extraOptions.tool == "vesper":
                                self.IOTool=DPold_Measurement
                                self.IOTool_text="DPold"
                        else:
                                print "ERROR: Invalid --tool value"
                else:
                        self.IOTool=FIO_Measurement
                        self.IOTool_text="FIO"
                        #self.IOTool=FIO_Measurement
                        #self.IOTool_text="FIO"


        def get_increment(self,qdepth):
                #control the qdepth scaling based on the current qdepth
                if qdepth < 256:
                        increment=32
                elif qdepth < 512:
                        increment=64
                elif qdepth < 1024:
                        increment=128
                else:
                        increment=256

                return increment


        def __get_measurement_name__(self,accessMode,xfer,readpct,seekpct):
                if accessMode == "random read":
                        name= "Random Reads %(xfer)sKB" % vars()
                elif accessMode == "random write":
                        name= "Random Writes %(xfer)sKB" % vars()
                elif accessMode == "segmented read":
                        name= "Segmented Sequential Reads %(xfer)sKB" % vars()
                elif accessMode == "sequential read":
                        name= "Sequential Reads %(xfer)sKB" % vars()
                elif accessMode == "segmented write":
                        name= "Segmented Sequential Writes %(xfer)sKB" % vars()
                elif accessMode == "sequential write":
                        name= "Sequential Writes %(xfer)sKB" % vars()
                elif accessMode == "OLTP":
                        writepct=100-readpct
                        name= "OLTP %(readpct)s-%(writepct)s Reads-Writes %(xfer)sKB" % vars()
                elif accessMode == "SEQOLTP":
                        writepct=100-readpct
                        name= "SEQUENTIAL OLTP %(readpct)s-%(writepct)s Reads-Writes %(xfer)sKB" % vars()
                elif accessMode == "CUSTOM_OLTP":
                        writepct=100-readpct
                        name= "CUSTOM OLTP %(readpct)s / %(writepct)s %(xfer)sKB Seek %(seekpct)s" % vars()
                else:
                        name= "ERROR: UNKNOWN <access: %(accessMode)s xfer:%(xfer)s readpct: %(readpct)s seek: %(seekpct)s percent> " % vars()	
                return name

        def __get_open_flags__(self):
                if self.extraOptions.o_sync:
                        return "O_SYNC"
                else:
                        return "O_DIRECT"

        def __print_report_header__(self):
                print "#HEADER"
                if self.extraOptions.demo:
                        print "     Test Name: DEMO DEMO DEMO DEMO DEMO"

                else:
                        print "     Test Name: Optimal State Performance using %s" % self.IOTool_text
                if self.experiment_message:
                        print "EXPERIMENT: %s" % self.experiment_message
                else:
                        print "EXPERIMENT: NO MESSAGE PROVIDED"
                print "     Warm Up Time: %s" % self.warmupTime
                print "     Measurement Time: %s" % self.measurementTime
                print "     Rest Time: %s" % self.restTime
                print "     Number of Samples: %s" % self.sampleCount
                print "     Sample Interval: %s" % self.sampleInterval
                print "     Response Time Limit: %sms" % self.targetResponseTime
                print "     IO Benchmark Tool: %s" % self.IOTool_text
                print "     Working Set Offset/limits: %s" % self.offset
                print "     Pattern Dedupe Value: %s" % self.pattern
                print "     Cachehit Ratio: %s" % self.cachehit
                print "     Target open flags: %s" % self.__get_open_flags__()
                print "     Workloads to Measure:"

                if self.extraOptions.fill or self.extraOptions.fillstop:
                        print "        Fill target disks"

                if self.experiment_message:
                        print "EXPERIMENT: %s" % self.experiment_message

                if not self.extraOptions.fillstop:
                        i=1
                        #print self.accessList
                        for accessMode,readpct,seekpct in self.accessList:
                                for xfer in self.xferList:
                                        print "         %s - %s " % (i,self.__get_measurement_name__(accessMode,xfer,readpct,seekpct))
                                        i+=1

                print "     command line: %s" % " ".join(sys.argv)
                self.__print_warning_report__()
                print "#HEADER"
                print "#MEASUREMENTS"


        def __print_warning_report__(self):
                #Check for any configuration problems that may lead to errnoeous measurements
                #for example, using a  warmup time of less than to 10 seconds, or measurement
                #time of less than 30 seconds
                warnings=[]

                if self.extraOptions.demo:
                        warnings.append("This is a Demo/trial run only! DO NOT MAKE SIZING DECISIONS WITH THIS DATA")
                if self.warmupTime < 60:
                        warnings.append("Warmup Time Less Than 60 seconds, Disk/Controller Cache Transients will prevent steady-state measurement")
                if self.measurementTime < 90:
                        warnings.append("Measurement Time Less Than 90 seconds, Measurement will be influenced by transients")
                if self.restTime < 30:
                        warnings.append("Rest Time < 30 seconds.  Cache activity after measurement completes may impact future measurements")
                if warnings:
                        print "#WARNINGS_REPORT"
                        print ""
                        print "     All warnings should be evaluated carefully in regard to the experiment you are trying to run"
                        print "     Depending on the RAID array controller cache, long warmup times,5min+ may be required for accurate measurements"
                        print "     Rest times may also be required. Careful evaluation may need to be done to determine the optimum warmup, measurement, and rest times"
                        print ""
                        for warning in warnings:
                                print "     WARNING: %s" % warning
                        print ""
                        print "#WARNINGS_REPORT"


        def __print_report_footer__(self):
                devList=self.get_dev_list()
                print "#FOOTER"
                print "#Disabled fiox_discover - JJS 6/15/2018"
                #print os.popen("./fiox_discover %s" % " ".join(devList)).read()
                print "#FOOTER"

        def run(self):
                #warmupTime=30
                #measurementTime=90
                if self.extraOptions.demo:
                        self.__config_demo__()

                self.__print_report_header__()
                #Ensure that if we get any source code errors/exceptions, 
                #we hadnle them and print the report footer
                if self.extraOptions.fill or self.extraOptions.fillstop:
                        self.__run_fill__()
                if not self.extraOptions.fillstop:
                        try:
                                self.__run_measurements__()
                        except KeyboardInterrupt:
                                print ""
                                print "########################"
                                print "# ERROR: USER Interrupted via keyboard - now cleaning up processess, please wait" 
                                print "########################"
                                print ""
                                self.capturetool.posttest_capture("exit")
                                self.capturetool.posttest_cleanup("exit")
                        except:
                                traceback.print_exc()
                                traceback.print_exc()
                self.__print_report_footer__()

        def __run_fill__(self):
                #Calls to self.IOTool for fill operations are done here.  Make changes to variables being passed in as needed
                #JJS - 10/29/2014 - Adding self.pattern  to support dedupe patterns
                ###################
                myMeasure=self.IOTool(256,1,30,90,"fill",self.devList,readpct=0,hostConfig=self.hostConfigs,pattern=self.pattern,ioengineConfig=self.ioengineConfigs)
                myMeasure.setExtraOptions(self.extraOptions)

                print "#FILL"
                print myMeasure
                myMeasure.fill()
                print "#FILL"

        def __run_measurements__(self):
                """
                This method is runs the specified measurements in the order specified from the command line

                It is responsible for initializing a seprate instance of the Tool (self.IOTool) for each qdepth
                value to be tested.
                """
                self.capturetool.set_sample_parms(1,self.warmupTime+self.measurementTime)

                qdepthLimit=2048
                for accessMode,readpct,seekpct in self.accessList:
                        for xfer in self.xferList:
                                print "-"
                                print "Date: %s" % time.ctime()
                                print "-"
                                print "TEST: %s" % self.__get_measurement_name__(accessMode,xfer,readpct,seekpct)

                                if self.histogram_buckets:
                                        print "p io/s mb/s rt        Percentile %s" % ",".join(map(str,self.histogram_buckets))
                                else:
                                        print "p io/s mb/s rt        cpuUser cpuSystem"

                                #Iterate through all the queudepths.
                                #When we hit a response time that exceeds variable self.targetResponseTime,
                                #break out of the loop
                                ##########################
                                i=0	#used for index into qdepth

                                #create a copy of self.qdepths
                                qdepthList=list(self.qdepths)

                                while i < len(qdepthList):
                                    #This while loop is horribly overloaded
                                    #
                                    #This "Samplerun" section allows multiple samples to be captured using the primary
                                    #workload generator, and "Restarting" it after every sample period.
                                    #
                                    #This is being intrudoced 7/2018 due to issues with FIO and collecting garbage in is
                                    #write_hist_log functionality.
                                    #
                                    #We need to be able to record and monitor histogram during long running measurements
                                    #without relying on the write_hist_log functionality
                                    ########################
                                    samplerun=0
                                    while (samplerun <= sampleCount):
                                        samplerun+=1

                                        qdepth=qdepthList[i]

                                        if qdepthLimit and qdepth > qdepthLimit:
                                                i+=1
                                                continue

                                        #myMeasure=FIO_Measurement(xfer,qdepth,self.warmupTime,self.measurementTime,accessMode,self.devList)
                                        debug("READ PERCENT AT INIT %s" % readpct,2)

                                        #capture_measurement_time=time.ctime().replace(":","").replace(" ","_")
                                        capture_measurementname="%s.%s.m%s.x%s.q%s" % (
                                                                                        self.get_logfile_id(),
                                                                                        accessMode.replace(" ",""),
                                                                                        readpct,
                                                                                        xfer,
                                                                                        qdepth
                                                                                        )

                                        self.capturetool.pretest_capture(capture_measurementname)
                                        myMeasure=self.IOTool(
                                                                xfer,
                                                                qdepth,
                                                                self.warmupTime,
                                                                self.measurementTime,
                                                                accessMode,
                                                                self.devList,
                                                                readpct=readpct,
                                                                offset=self.offset,
                                                                seekpct=seekpct,
                                                                sampleCount=self.sampleCount,
                                                                hostConfig=self.hostConfigs,
                                                                ioengineConfig=self.ioengineConfigs,
                                                                aoConfig=self.aoConfig,
                                                                histogram_buckets=self.histogram_buckets,
                                                                iops_override=self.iops_override,
                                                                pattern=self.pattern,
                                                                cachehit=self.cachehit
                                                                )


                                        myMeasure.setExtraOptions(self.extraOptions)
                                        runTime=time.time()
                                        iops,mbs,rt,cpu_user,cpu_system=myMeasure.run()

                                        # If we encountered an error, limit max qdepth to previous qdepth
                                        # This is usually a server config issue, like max open processes, memory, etc
                                        ############
                                        if myMeasure.getErrorFound():
                                                try:
                                                        if i > 1:
                                                                qdepthLimit=qdepthList[i-1]
                                                        else:
                                                                qdepthLimit=qdepthList[i]
                                                        print "## ERROR: Limiting Max qdepth to %s to reduce future errors" % qdepthLimit
                                                except IndexError:
                                                        print "## ERROR: Insufficient qdepths <2 to handle errors, forcing measurement end"
                                                        break

                                        runTime=time.time() - runTime

                                        capturetool_output=self.capturetool.posttest_capture(capture_measurementname)

                                        #Get the qdepth actually being used. If 
                                        actualqdepth=myMeasure.get_qdepth()

                                        r_and_w_percentiles=""
                                        if myMeasure.supports_percentiles():
                                                r_and_w_percentiles=myMeasure.get_percentiles()

                                        indepthperf="/root/fiox/op_logs/perflogs/%s.csv" % capture_measurementname

                                        if myMeasure.supports_detailed_results():
                                                read_iops,read_mbs,read_latency,write_iops,write_mbs,write_latency=myMeasure.get_detailed_results()
                                                print "%s %s %s %s  r %s %s %s   w %s %s %s %s	\t%s" % (actualqdepth,iops,mbs,rt,read_iops,read_mbs,read_latency,write_iops,write_mbs,write_latency,r_and_w_percentiles,indepthperf)

                                        else:
                                                print "%s %s %s %s %s %s %s	\t%s" % (actualqdepth,iops,mbs,rt,cpu_user,cpu_system,r_and_w_percentiles,indepthperf)

                                        if capturetool_output != None:
                                            print "#############################"
                                            print capturetool_output
                                            print "#############################"

                                        if not self.fixedQdepths:
                                                if i == len(qdepthList)-1:
                                                        if Decimal(str(rt).replace("ms","")) < self.targetResponseTime:
                                                                increment=self.get_increment(qdepth)
                                                                qdepthList.append(qdepth+increment)
                                                        else:
                                                                break
                                        if not self.skipcleanup:
                                                myMeasure.cleanup()

                                        self.capturetool.posttest_cleanup(capture_measurementname)
                                        time.sleep(self.restTime)
                                        i+=1
                print "#MEASUREMENTS"
                return 1

class PerformanceFramework(OptimalPerformance):
        def __init__(self,*args,**kargs):
                OptimalPerformance.__init__(self,*args,**kargs)

        def section_print(self,text):
                print "#%s" % text


        def prefill(self):
                print "Running fill"
                self.__run_fill__()
                print "Fill complete"
        def poll(self):
                try:
                        if self.workload:
                                return self.workload.poll()
                        else:
                                return -1
                except:
                        traceback.print_exc()
                        return -1


        def start_workload(self,accessMode=None,readpct=None,seekpct=None,xfer=None,qdepth=None,extra="",forkfill=0):
                if not accessMode:
                        accessMode=self.accessList[0][0]
                if not readpct:
                        readpct=self.accessList[0][1]
                if not seekpct:
                        seekpct=self.accessList[0][2]
                if not qdepth:
                        qdepth=self.qdepths[0]
                if not xfer:
                        xfer=self.xferList[0]
                iops_override=self.iops_override
                debug("Executiong FIOX and VESPER via Performance Framework")
                self.workload=self.IOTool(
                                        xfer,
                                        qdepth,
                                        self.warmupTime,
                                        self.measurementTime,
                                        accessMode,
                                        self.devList,
                                        readpct=readpct,
                                        offset=self.offset,
                                        seekpct=seekpct,
                                        aoConfig=self.aoConfig,
                                        sampleCount=self.sampleCount,
                                        hostConfig=self.hostConfigs,
                                        ioengineConfig=self.ioengineConfigs,
                                        extralabel=extra,
                                        pattern=self.pattern,
                                        cachehit=self.cachehit,
                                        iops_override=iops_override
                                        )
                self.workload.setExtraOptions(self.extraOptions)
                self.runTime=time.ctime()
                print "#######"
                print "Process below"
                if forkfill:
                        self.workload.fill(fork=1)
                else:
                        print self.workload.run(fork=1)
                print "#######"

        def cleanup_workload(self):
                self.workload.cleanup_process()
        def stop_workload(self,skip_cleanup=0,LIMIT=60):
                print "stop_Workload called...attempting to exit DP cleanly"

                try:
                        iops,mbs,rt,cpu_user,cpu_system=self.workload.stop(skip_cleanup,LIMIT)
                except TypeError:  
                        self.stopTime=time.ctime()
                        print "Nothing to parse"
                        return

                if skip_cleanup:
                        return

                self.stopTime=time.ctime()

                actualqdepth=self.workload.get_qdepth()

                if self.workload.supports_detailed_results():
                        read_iops,read_mbs,read_latency,write_iops,write_mbs,write_latency=self.workload.get_detailed_results()
                        print actualqdepth,iops,mbs,rt,"  r",read_iops,read_mbs,read_latency,"  w",write_iops,write_mbs,write_latency,"         ",cpu_user,cpu_system

                else:
                        print actualqdepth,iops,mbs,rt,"         ",cpu_user,cpu_system




def main(stdoutWrapper=None):
        if not stdoutWrapper:
                sys.stdout=stdoutWrapper=stdout_wrapper("./logs/fiox_default_logfile.log",basic=1)

        #use these intensities
        qdepths=[1,2,4,8,16,24,32,48,64]#,96,128,256,384,512]
        #qdepths=[256,384,512]

        #Set Transfer sizes to 40k, 64k, 512k, 1024k
        #xferList=[4,64,512,1024]
        xferList=[8,256]#,64,512,1024]

        #Use sequentialreads and writes
        accessList=[
                        ["random read",100,100],
                        ["random write",0,100],
                        ["sequential read",100,0],
                        ["sequential write",0,0],
                        ["OLTP",60,100]
        ]

        debug(qdepths,1)
        debug(accessList,1)
        debug(xferList,1)

        #Create OptimalPerformance instance
        op=OptimalPerformance(qdepths,accessList,xferList,fixedQdepths=0,stdoutWrapper=stdoutWrapper)

        def exit_handler(signal,frame):
                sys.__stdout__.write("EXIT HANDLER CALLED %s" % str(signal))
                print "EXIT HANDLER CALLED %s" % str(signal)
                print sys.argv

        if hasattr(os.sys,'winver'):
                signal.signal(signal.SIGTERM,exit_handler)
                signal.signal(signal.SIGBREAK,exit_handler)
                signal.signal(signal.SIGINT,exit_handler)
        else:
                signal.signal(signal.SIGQUIT,exit_handler)


        if op.check_usage():
                try:
                        op.run()
                except:
                        traceback.print_exc()
                        print "Attempting to stop workload (if its running"
                        exit_handler(None,None)

        else:
                print "ERROR, use parm -h for help"

#if __name__ == "__main__":
#	sys.stdout=stdout_wrapper()
#	main()
