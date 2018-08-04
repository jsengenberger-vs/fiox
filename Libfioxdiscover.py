#!/usr/bin/env python
##################
### FIOX Disk Discovery Tool, part of the FIOX tool
### Author: john.j.sengenberger@hp.com
### Version 2.0.1
###
### NOTES:
###
###   - Will scan and report info for all mapper managed devices
###   - use space separated list of device files to query specific devices from the command line
###   - ./fiox_discover /dev/dm-1 /dev/cciss/c0d0
###
###   - Devices tested: dm,hda,sda,cciss,ram
###
#####################


import os
import commands
import time
import traceback
import sys
import subprocess
from subprocess import PIPE

from decimal import *


class hwDiscover:
	def __init__(self,devList,remote=0,hostname=None,username=None,password=None):
		self.devList=devList
		self.remote=remote
		self.hostname=hostname
		self.username=username
		self.password=password
		self.data={}



	def add_data(self,key,value):
		self.data[key]=value

	def run(self,cmd):
		result=commands.getoutput(cmd)
		result=result.strip()
		return result

	def printData(Self):
		print "#DISCOVERY"
		for key,item in self.key:
			print "%s : %s" % (key,item)
		print "#DISCOVERY"
	def discover_device_data(self):
		print "NOTE: discover_device_data not implemented"
	def cleanup(self):
		print "NOTE: hwDiscover.cleanup() not implemented in %s.  Override to implement functionality" % self
			
	def discover_device_files(self):
		#Discovers relavant device files given a particular input
		#Currently depends on multipath
		self.devList=[]
		return []

	def print_columns_justified(self,listToPrint):
		colWidth = max(len(word) for row in listToPrint for word in row) + 2
		for row in listToPrint:
			print "".join(word.ljust(colWidth) for word in row)

class DeviceLinux:
	def __init__(self,device="",shortdevice="",sysblock_base="/sys/block/"):

		if device and shortdevice:
			self.device=device
			self.shortdevice=shortdeice
		elif device:
			self.device=device
			self.shortdevice=device.split("/")[-1]
		elif shortdevice:	
			self.device="/dev/%s" % shortdevice
			self.shortdevice=shortdevice
		else:
			raise AttributeError,"device and/or shortdevice must be specified"

		self.sysblock="%s%s" % (sysblock_base,self.shortdevice)


class DeviceLinuxDisk(DeviceLinux):
	def __init__(self,*args,**kargs):
		DeviceLinux.__init__(self,*args,**kargs)

		self.sizeFile="%s/size" % self.sysblock
		self.sysqueue="%s/queue" % self.sysblock
		self.sysdevice="%s/device" % self.sysblock

		self.schedulerFile="%s/scheduler" % self.sysqueue
		self.read_aheadFile="%s/read_ahead_kb" % self.sysqueue
		self.nr_requestsFile="%s/nr_requests" % self.sysqueue
		self.max_sectors_kbFile="%s/max_sectors_kb" % self.sysqueue
		self.queue_depthFile="%s/queue_depth" % self.sysdevice
		self.modelFile="%s/model" % self.sysdevice
		self.vendorFile="%s/vendor" % self.sysdevice
		self.revFile="%s/rev" % self.sysdevice

	def get_scheduler(self):
		try:
			schedulerList= file(self.schedulerFile).read()
			if schedulerList.strip() == "none":
				return "none"
			#extract out the highlighed scheduler
			scheduler=schedulerList.split("[")[1].split("]")[0]
			return scheduler	
		except:
			return "UNKNOWN"
	def set_scheduler(self,scheduler):
		try:
			foo=file(self.schedulerFile,'w')
			foo.write(scheduler)
			foo.close()
			return 1
		except:
			return 0
	def get_model(self):
		try: return file(self.modelFile).read().strip()
		except: return "UNKNOWN"
	def get_vendor(self):
		try: return file(self.vendorFile).read().strip()
		except: return "UNKNOWN"
	def get_rev(self):
		try: return file(self.revFile).read().strip()
		except: return "UNKNOWN"

	def get_read_ahead(self):
		try: return file(self.read_aheadFile).read().strip()
		except: return "UNKNOWN"
	def get_nr_requests(self):
		try: return file(self.nr_requestsFile).read().strip()
		except: return "UNKNOWN"
	def set_nr_requests(self,num):
		try: 
			foo=file(self.nr_requestsFile,'w')
			foo.write(str(num))
			foo.close()
			return 1
		except: 
			#traceback.print_exc()
			return 0
	def get_max_sectors_kb(self):
		try: return file(self.max_sectors_kbFile).read().strip()
		except: return "UNKNOWN"
	def get_queue_depth(self):
		try: return file(self.queue_depthFile).read().strip()
		except: return "UNKNOWN"
	def set_queue_depth(self,num):
		try: 
			foo=file(self.queue_depthFile,'w')
			foo.write(str(num))
			foo.close()
			return 1
		except: 
			traceback.print_exc()
			return 0
	def get_size(self):
		size=file(self.sizeFile).read().strip()
		#convert to GB
		sizeblocks=Decimal(size)/2

		#kb
		size=sizeblocks/1024

		#if size < 1000:
		#	return str(size.quantize(Decimal('0.01')))+"KB"
		#size=size/1024
		if size < 1000:
			return str(size.quantize(Decimal('0.01')))+"MB"
		size=size/1024
		if size < 1000:
			return str(size.quantize(Decimal('0.01')))+"GB"
		size=size/1024
		return str(size.quantize(Decimal('0.01')))+"TB"

	def print_discovery(self):
		print "Device: %s" % (self.device)
		print "   Device Slaves: N/A"
		print "   Size: %s" % self.get_size()
		print "   Model: %s %s v%s" % (self.get_vendor(),self.get_model(),self.get_rev())
		print "   IO Scheduler: %s" % self.get_scheduler()
		print "   Max IO Size(max_sectors_kb): %s" % self.get_max_sectors_kb()
		print "   Max Number I/Os per device: %s" % self.get_nr_requests()
	def print_discovery_summary(self):
		output="%s\t%s\t%s\t%s\t%s\t%s" % (
						self.device,
						self.get_size(),
						self.get_vendor(),
						self.get_scheduler(),
						self.get_max_sectors_kb(),
						self.get_nr_requests())
		print output


class DeviceLinuxRBD(DeviceLinuxDisk):
	def __init__(self,device):
		rbdimageRealpath=os.path.realpath(device)
		self.rbdimage=device.split("/")[-1]
		self.rbdpool=device.split("/")[-2]
		DeviceLinuxDisk.__init__(self,rbdimageRealpath)
	def print_discovery_summary(self):
		output="%s\t%s\t%s\t%s\t%s\t%s\t%s.%s" % (
						self.device,
						self.get_size(),
						self.get_vendor(),
						self.get_scheduler(),
						self.get_max_sectors_kb(),
						self.get_nr_requests(),
						self.rbdpool,self.rbdimage)
		print output
		



class DeviceLinuxCCISS(DeviceLinuxDisk):
	def __init__(self,device):
		dev=device.strip().split("/")

		#take c0d1p4 and get "c0d1"
		dev_c_d_p=dev[-1]
		cnum=dev_c_d_p.split("c")[1].split("d")[0]

		self.device=device
		if dev_c_d_p.find("p") > -1:
			print "Handle child slice"
			dev_c_d=dev_c_d_p.split("p")[0]
			self.shortdevice="cciss!%s" % dev_c_d
			#self.shortdevice="cciss!%s/cciss!%s" % (dev_c_d,dev_c_d_p)
			self.sysblock="/sys/block/%s" % self.shortdevice

			#contructs path like this
			#/sys/block/cciss!c0d0/device/cciss0/c0d0
			#################

			#self.sysdevice="/sys/block/cciss!%s/device/cciss%s/%s/" % (self.shortdevice,cnum,dev_c_d)
			self.sizeFile="%s/cciss!%s/size" % (self.sysblock,dev_c_d_p)
		else:
			dev_c_d=dev_c_d_p
			self.shortdevice="cciss!%s" % dev_c_d
			self.sysblock="/sys/block/%s" % self.shortdevice
			self.sizeFile="%s/size" % self.sysblock
			#self.sysdevice="%s/device" % self.sysblock

		self.sysdevice="/sys/block/%s/device/cciss%s/%s/" % (self.shortdevice,cnum,dev_c_d)


		self.sysqueue="%s/queue" % self.sysblock

		self.schedulerFile="%s/scheduler" % self.sysqueue
		self.read_aheadFile="%s/read_ahead_kb" % self.sysqueue
		self.nr_requestsFile="%s/read_ahead_kb" % self.sysqueue
		self.max_sectors_kbFile="%s/max_sectors_kb" % self.sysqueue
		self.modelFile="%s/model" % self.sysdevice
		self.vendorFile="%s/vendor" % self.sysdevice
		self.revFile="%s/rev" % self.sysdevice
	def get_queue_depth(self):
		return "N/A"

class DeviceLinuxFile(DeviceLinux):
	def __init__(self,*args,**kargs):
		DeviceLinux.__init__(self,*args,**kargs)
		self.statinfo = os.stat(self.device)
	def get_size(self):
		num=float(self.statinfo.st_size)
		num=num/1024/1024/1024
		num=Decimal(str(num)).quantize(Decimal('.01'))
		
		return "%s GB" % num
	def get_dev(self):
		return self.statinfo.st_dev
	def get_blocks(self):
		num=float(self.statinfo.st_blocks)
		#convert blocks to 1k blocks
		num=num/2
		#convert 1k blocks to 1g
		num=num/1024/1024
		num=Decimal(str(num)).quantize(Decimal('.01'))
		return "%s GB" % num
		
	def print_discovery(self):
		print "File: %s" % (self.device)
		print "   Size Allocated: %s" % self.get_size()
		print "   Size Used: %s" % self.get_blocks()
		
		
	
class DeviceLinuxMapper(DeviceLinux):
	def __init__(self,*args,**kargs):
		DeviceLinux.__init__(self,*args,**kargs)

	def get_slaves(self):
		slaves=os.listdir("%s/slaves" % self.sysblock)
		return slaves


	def __get_unique_list_from_slaves__(self,methodToCall,toString=0,parm_to_pass=None):
		"""
		Utility method to call the same method across multiple instances of the same class
		"""
		slaves=self.get_slaves()
		uniqueList=[]
		for slave in slaves:
			#print "YES"
			#shortdevice="%s/slaves/%s" % (self.sysblock,slave)
			slave=DeviceLinuxDisk(shortdevice=slave,sysblock_base=self.sysblock+"/slaves/")

			#call the passed in method
			if parm_to_pass:
				value=methodToCall(slave,parm_to_pass)
			else:
				value=methodToCall(slave)

			if value not in uniqueList:
				uniqueList.append(str(value))
		if toString:
			return ",".join(uniqueList)	
		else:
			return uniqueList

	def get_schedulers(self):
		schedulers=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.get_scheduler,toString=1)
		return schedulers
	def set_schedulers(self):
		schedulers=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.set_scheduler,toString=1,parm_to_pass="noop")
		return schedulers
	def get_max_sectors_kb(self):
		max_sectors_kb=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.get_max_sectors_kb,toString=1)
		return max_sectors_kb
	def get_nr_requests(self):
		nr_requests=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.get_nr_requests,toString=1)
		return nr_requests

	def set_nr_requests(self):
		nr_requests=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.set_nr_requests,toString=1,parm_to_pass=256)
		return nr_requests

	def get_rev(self):
		revs=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.get_rev,toString=1)
		return revs
	def get_queue_depths(self):
		qdepths=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.get_queue_depth,toString=1)
		return qdepths
	def set_queue_depths(self):
		qdepths=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.set_queue_depth,toString=1,parm_to_pass=128)
		return qdepths
	def get_size(self):
		size=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.get_size,toString=1)
		return size
	def get_vendor(self):
		vendor=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.get_vendor,toString=1)
		return vendor
	def get_model(self):
		model=self.__get_unique_list_from_slaves__(DeviceLinuxDisk.get_model,toString=1)
		return model


	def configure(self):
		self.set_schedulers()
		self.set_queue_depths()
		self.set_nr_requests()
	def print_discovery(self):
		print "Device: %s" % (self.device)
		print "   Device Slaves: %s" % self.get_slaves()
		print "   Size: %s" % self.get_size()
		print "   Model: %s %s v%s" % (self.get_vendor(),self.get_model(),self.get_rev())
		print "   Queue Depth(per slave): %s" % self.get_queue_depths()
		print "   IO Scheduler: %s" % self.get_schedulers()
		print "   Max IO Size(max_sectors_kb): %s" % self.get_max_sectors_kb()
		print "   Max Number I/Os per device: %s" % self.get_nr_requests()
	def print_discovery_summary(self):
		try:
			output="%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
						self.device,
						self.get_size(),
						self.get_vendor(),
						self.get_schedulers(),
						self.get_max_sectors_kb(),
						self.get_nr_requests(),
						self.get_queue_depths(),
						)
		except:
			output="%s\t N/A" % (self.device)
		print output



class DeviceSunDisks:
	def __init__(self,dev_list=[]):
		self.devices=dev_list
		self.diskData={}

	def get_multipath_devs(self):
		output=os.popen("mpathadm list lu").read()

		path=None
		tCount=None
		oCount=None
		paths=[]
		for line in output.split("\n"):
			line=line.strip()
			if not line:	
				continue
			if not path:
				path=line
			elif not tCount:	
				tCount=line.split(":")[1]
			elif not oCount:
				oCount=line.split(":")[1]
				paths.append([path,tCount,oCount])
				path=None
				tCount=None
				oCount=None
		return paths
	def get_path_info(self,path):
		#Path must be /dev/rdsk/c6t600000000d0s0 format
		##########
		data={'paths':[],'target_ports':[],'data':{}}
		output=os.popen("mpathadm show lu %s" % path)

		parent="data"
		for row in output:
			row=row.strip()
			if not row:
				continue
			key,value=row.split(":")
			if key == "Paths":
				parent="paths"
			elif parent == "paths":
				continue
				
			elif key == "Target Ports":
				parent = "target_ports"
			elif parent == "target_ports":
				continue
			else:
				data['data'][key]=value
		return data

	def get_good_disk_list(self):
		good_disk_list=[]
		#First, get the disk instacnes we're interested in
		prtconf_short=os.popen("prtconf").read()
		for line in prtconf_short.split("\n"):
			#Ok, we found a disk, now lets see if its good or not
			if line.find("disk, instance") > -1:
				#Oops, disk is gone or needs a driver isntalled.  Lets skip it
				if line.find("driver not attached") > -1:
					continue
				else:
					good_disk_list.append(line.strip())

		return good_disk_list


	def get_disk_info(self):

		prtconf_long=os.popen("prtconf -v").read()
		good_disk_list=self.get_good_disk_list()

		disks={}
		found=0
		diskname=None
		key=None
		value=None

		for line in prtconf_long.split("\n"):
			line=line.strip()
			if line in good_disk_list:
				diskname=line.split("#")[1]
				if disks.has_key(diskname):
					raise AttributeError,"Unexpectedly found %s more than once" % diskname

				disks[diskname]={}
			elif diskname:
				#Ignore path information right now
				if line.find("Paths from") > -1:
					diskname=None
					key=None
					value=None
				else:
					if line[0:4] == "name":
						key=line.split()[0].split("'")[1]
					elif key and line[0:5] == "value":
						value=line.split("=")[1]
						if disks[diskname].has_key(key):
							raise AttributeError,"Unexpected found key %s for disk %s more than once" % (key,diskname)
						disks[diskname][key]=value

		return disks		

        def print_short_discovery(self):
		"""
		A method that provides a CLI friendly space delimitered list of available devices
		"""
                devs=[]
                for dev,tcount,ocount in self.get_multipath_devs():
			#only include rdsk devices
			if not dev.find("/dev/rdsk") > -1:
				continue
                        devs.append(dev)
                devs.sort()
                print " ".join(devs)


	def print_discovery(self):
		keys=[
			'mpath-support',
			'Vendor',
			'Product',
			'Revision',
			'Name',
			'Current Load Balance',
			'Asymmetric'
			]

		for dev,tcount,ocount in self.get_multipath_devs():
			data=self.get_path_info(dev)['data']
			if self.devices and dev not in self.devices:
				continue
			print "Device: %s" % dev
			for key in keys:
				print "\t%s - %s" % (key,data[key])

	def print_discovery_summary(self,short=0,filter=""):
		fields=['Vendor','Product','Revision','Current Load Balance','Name']

		columns=["Device"]+fields
		print "#" + "\t\t".join(fields)

		for dev,tcount,ocount in self.get_multipath_devs():
			data=self.get_path_info(dev)['data']
			if self.devices and dev not in self.devices:
				continue

			if filter and data['Vendor'].find(filter) == -1:
				continue

			#Skip non-disk devices
			if dev.find("scsi_vhci") > -1:
				continue

			row=[dev]
			#print "Device: %s" % dev
			for field in fields:
				row.append(data[field])
			print "\t\t".join(row)
			
		
	def print_prtconf_discovery(self):

		keys=[
			'client-guid',
			'inquiry-vendor-id',
			'inquiry-product-id',
			'inquiry-revision-id',
			'class',
			'device-nblocks',
			]

		disks=self.get_disk_info()
		diskIds=disks.keys()
		diskIds.sort()

		for diskId in diskIds:
			print "Disk ID: %s" % diskId
			for key in keys:
				if disks[diskId].has_key(key):
					print "\t%s - %s" % (key,disks[diskId][key])
				else:
					print "\t%s - None" % key

		
class DeviceWindowsDisks:
	def __init__(self,dev_list=[],remote=0,hostname="localhost",username="Administrator",password="P@ssword"):
		self.devices=dev_list
		self.diskData={}
		self.remote=remote
		self.hostname=hostname
		self.username=username
		self.password=password
		self.process=None		#Initialize to None

		self.wmic_scan_short()


	def runshell(self,cmd,command_prompt):

		if self.process:
			raise AttributeError,"Unable to start new shell, self.process != None"

		self.command_prompt=command_prompt


		if self.remote:
			cmd="psexec \\\\%s -u %s -p %s %s" % (self.hostname,self.username,self.password,cmd)
		#This can raise subprocess.CalledProcessError..don't handle for now to simplify debuging

		cmd=cmd.split()
		print cmd
		self.process=subprocess.Popen(cmd,stdin=PIPE,stdout=PIPE)
		print self.process

		return 


	def send_and_read(self,cmd):
		print "Writing %s" % cmd

		self.process.stdin.write(cmd+"\n")
		self.process.stdin.flush()
		time.sleep(2)

		output=""
		line=""
		while 1:
			data=self.process.stdout.read(1)
			print [data]
			line+=data
			if data == "\n":
				output+=line
				line=""
				if line.find(self.command_prompt) > -1:
					break
				print [line]

		return output
			


	def cleanupshell(self):
		if not self.process.poll():
			self.process.terminate()
			time.sleep(5)
			#TODO !Yuck!
			if not self.process.poll():
				raise AttributeError,"Error, unable to close process in 5 seconds"

	def run(self,cmd):
		#if self.remote:
		#	cmd="psexec \\\\%s -u %s -p %s %s" % (self.hostname,self.username,self.password,cmd)
		#This can raise subprocess.CalledProcessError..don't handle for now to simplify debuging
		output=subprocess.check_output(cmd,stderr=subprocess.PIPE)
		return output
	

	

	def diskpart_cleanup(self,caption="3PARdata VV"):
		"""
		Prepares all Offline disks on the system for by bringing them online
		This does not scan WMI or perform filtering

		TODO: 
		"""
		def diskpart(script):
			file('c:\\perf\\diskpart.script','w').write(script)
			#print "DONE RUNNING 3par cleanup"

			cmd="diskpart /s c:\\perf\\diskpart.script"
			output=self.run(cmd)
			print "OUTOUT: %s" % output
			return output

		def diskpartscan():
			print "Beginning diskpart rescan process"
			diskpart("rescan")
			diskpart("list disk")
			print "sleeping 30 seconds after rescan for disks to be discovered"
			time.sleep(30)
			print "Running list disk, and determining which disks are Offline"
			disknames=[]
			for row in diskpart("list disk").split("\n"):
				row=row.strip()
				if row.find("Offline") > -1:
					diskname,disknumber=row.split("Offline")[0].strip().split()
					disknames.append([diskname,disknumber])
			return disknames
					

		diskpart("SAN POLICY=onlineAll")
		offline_disks=diskpartscan()
		self.wmic_scan_short()

		for diskname,disknumber in offline_disks:
	
			#for disk in diskpartscan():

			#skip drives that don't match caption above
			#if diskdata['Caption'].find(caption) == -1:
			#	continue
			#remove extraneous text and convert to a "disk #" as used by diskpart
			#disk=disk.replace("\\\\.\\PHYSICALDRIVE","").strip()

			#self.send_and_read("SELECT DISK %s" % disk)
			#self.send_and_read("ATTRIBUTES DISK CLEAR READONLY")
			#self.send_and_read("ONLINE DISK")

			#assume disk is currently offline..If we get an error..then we try again
			#
			#TODO run list disk, get current status, then run appropraite script
			script="""
SELECT DISK %s
ATTRIBUTES DISK CLEAR READONLY
ONLINE DISK
			""" % disknumber
			print "MY SCRIPT: %s" % script

			try:
				diskpart(script)
			except subprocess.CalledProcessError:
				traceback.print_exc()
				print "ERROR, something is seriously wrong"
				raise AttributeError,"Unable to bring disk %s online" % disk

		print "DONE WITH DISKPART ITEMS"	


	def get_headers(self,row):

		data={}
		headers=row.split()
		#find and store starting position 
		for header_name in headers:
			data[header]=row.index(header_name)

	def wmic_scan_short(self):
		"""
		This method will report information about disks drives on a Windows host via WMIC

		New disk drives will nee to be scanned via diskpart_cleanup before they appear here
		"""
		fields=['Caption','DeviceID','name','model','Size','BytesPerSector','SystemName','SCSILogicalUnit']

		output=self.run("wmic diskdrive get %s /format:csv" % ",".join(fields))

		fieldnames=[]
		for row in output.split("\n"):
			row=row.strip()
			if not row:
				continue
			#Extra the Fields in the order they are in wmic output
			#Windows will add additional fields not requested
			################
			if row.find("SCSILogicalUnit") > -1:
				row=row.split(",")
				fieldnames=row

			else:
				devdata={}
				row=row.split(",")
				i=0
				for field in fieldnames:
					devdata[field]=row[i]
					i+=1
				self.diskData[devdata['DeviceID']]=devdata
			
	def wmic_scan(self):
		"""Run the wmic diskdrive command, parse output and find relevant info"""
		#TODO I should have used "wmic diskdrive full"..would simplify this parsing greatly
		#TODO its also extensible to other commands
		#TODO This could also work: "wmic diskdrive get model, name,size,scsilogicalunit,deviceid,SCSITargetId"
		#
		output=self.run("wmic diskdrive")
		headers=[]
		for row in output.split("\n"):
			#row=row.strip()
			if not row.strip():
				continue
			if row.find("Availability") > -1:
				header_row=row
				headers=row.split()
				continue
			else:
				if not headers:
					raise AttributeError,"Unable to find header row"
				i=0
				devData={}
				for header_name in headers:
					if i == 0:
						startCol=header_row.index(header_name+" ")
					else:
						startCol=header_row.index(" " + header_name+" ")
					if i+1 >= len(headers):
					#	print 'end'
						endCol=-1
					else:
					#	print i,len(headers),headers[i+1]
					#	print 'go',headers[i],headers[i+1]
					#	print 'go',header_row.index(headers[i])
					#	print 'go',header_row.index(headers[i+1])

						endCol=header_row.index(" " + headers[i+1]+" ")
					#print header_row.index(header_name)
					#nxt_header=headers.index(header_name)+1
					#if nxt_header > len(headers):
					#	nxt_header=-1

					#print header_name,startCol,endCol,row[startCol:endCol]
					#raw_input()
					devData[header_name]=row[startCol+1:endCol-1]
					i+=1
				#print devData.keys()
				self.diskData[devData['DeviceID']]=devData
			#inq=row.split("}  ")[2]	

	def lookup(self,deviceID):
		if self.diskData.has_key(deviceID):
			return self.diskData[deviceID]
		else:
			return None

	def print_discovery(self):
		keys=self.diskData.keys()
		keys.sort()

		fields=['Caption','Size','Firmware Version','Decsription','InterfaceType','Manufacturer','MediaType','Model','Partitions','SCSIBus','SCSILogicalUnit','SCSIPort','SCSITargetId','BytesPerSector','SystemName']
		for diskId in keys:
			print ""
			print "Device: %s" % self.diskData[diskId]['DeviceID']
			for field in fields:
				if self.diskData[diskId].has_key(field):
					print "  %s: %s" % (field,self.diskData[diskId][field])
				else:
					print "  %s: N/A" % field
	def print_discovery_summary(self,short=0,filter=""):
		keys=self.diskData.keys()
		keys.sort()
		fields=['Caption','Size','BytesPerSector','SystemName','SCSILogicalUnit']
		if not short:
			print "#DEVICE\t\t\t" + "\t\t".join(fields)

		deviceids=[]

		for diskId in keys:
			#print ""
			#print "Device: %s" % self.diskData[diskId]['DeviceID']

			if short and self.diskData[diskId]['Caption'].find(filter) > -1:
				deviceids.append(self.diskData[diskId]['DeviceID'])
			elif not short and self.diskData[diskId]['Caption'].find(filter) > -1:
				values=[self.diskData[diskId]['DeviceID']]
				for field in fields:
					if self.diskData[diskId].has_key(field):
	
						#print "  %s: %s" % (field,self.diskData[diskId][field])
						values.append(self.diskData[diskId][field].strip())
					else:
						values.append("N/A")
				print "\t\t".join(values)
			else:
				continue
		if short:
			print ",".join(deviceids)
	def get_disk_path_size_lunid(self,filter=""):
		keys=self.diskData.keys()
		keys.sort()
		fields=['Size','SCSILogicalUnit']
		#if not short:
		#	print "#DEVICE\t\t\t"nn+ "\t\t".join(fields)

		deviceids=[]

		devicedata=[]

		for diskId in keys:
			#print ""
			#print "Device: %s" % self.diskData[diskId]['DeviceID']

			#if short and self.diskData[diskId]['Caption'].find(filter) > -1:
			#	deviceids.append(self.diskData[diskId]['DeviceID'])
			if self.diskData[diskId]['Caption'].find(filter) > -1:
				values=[self.diskData[diskId]['DeviceID']]
				for field in fields:
					if self.diskData[diskId].has_key(field):
	
						value=self.diskData[diskId][field].strip()
						if value:
							values.append(self.diskData[diskId][field].strip())
						else:
							values=[]
							break
					else:
						values.append("N/A")
						values=[]
						break
				#print "\t\t".join(values)
				if values:
					devicedata.append(values)
			else:
				continue
		return devicedata




class HPUXDisk:
	def __init__(self,csv_row):
		self.csv_row=csv_row.split(":")
		self.get_device_file()

	def run(self,cmd):
		result=commands.getoutput(cmd)
		result=result.strip()
		return result

	def get_device_file(self):
		self.dev="/dev/disk/disk%s" % self.csv_row[12]
		self.rdev="/dev/rdisk/disk%s" % self.csv_row[12]
	def get_vendor(self):
		self.vendor=self.run("scsimgr get_attr -D %s -a vid -p" % self.rdev).replace('"','').strip()
		return self.vendor

	def get_product(self):
		self.product=self.run("scsimgr get_attr -D %s -a pid -p" % self.rdev).replace('"','').strip()
		return self.product

	def get_path_count(self):
		self.path_count=self.run("scsimgr get_attr -D %s -a total_path_cnt -p" % self.rdev)
		return self.path_count

	def get_alua_enabled(self):
		self.alua_enabled=self.run("scsimgr get_attr -D %s -a alua_enabled -p" % self.rdev)
		return self.alua_enabled

	def get_queue_depth(self):
		qdepth=self.run("scsimgr get_attr -D %s -a max_q_depth -p" % self.rdev)
		return qdepth

	def set_queue_depth(self,qdepth):
		return self.run("scsmigr set_attr -D %s -a max_q-depth=%s" % qdepth)

	def get_size(self):
		output=self.run("diskinfo %s" % self.rdev)
		for row in output.split("\n"):
			row=row.split(":")
			if row[0].strip() == "size":
				return row[1].strip().replace(" Kbytes","KB")
		return "0KBkbytes"

	def get_print_data(self):
		data=[
			self.rdev,
			self.get_size(),
			self.get_vendor() + " " + self.get_product(),
			self.get_alua_enabled(),
			self.get_path_count(),
			self.get_queue_depth()
			]
		return data

	def print_discovery_summary(self):
		output="%s\t%s\t%s\t%s\t%s\t%s" % self.get_print_data()
		print output
class DeviceHPUXDisks:
	def __init__(self,dev_list=[]):
		self.devices=dev_list
		self.diskData={}

	def parse_ioscan_disks(self,filtertext=None):
		print "Running IOSCAN"
		output=self.run("ioscan -FNCdisk")
		devices=[]
		for row in output.split("\n"):
			#Only evaluate CLAIMED hardware
			if row.find("CLAIMED") > -1:
				if filtertext and row.find(filtertext) > -1:
					dev=HPUXDisk(row)
				elif not filtertext:
					dev=HPUXDisk(row)
				else:
					dev=None
				if dev:
					self.devices.append(dev)

	def print_discovery_summary(self,short=1,filtertext=""):
		data=["#device","size","vendor","product","alua_enabled","path_count","queue_depth"]
		for dev in self.devices:
			data.append(dev.get_print_data())
	def get_discovery_summary(self,short=1,filtertext=""):
		data=["#device","size","vendor","product","alua_enabled","path_count","queue_depth"]
		for dev in self.devices:
			data.append(dev.get_print_data())
		return data

#	def discover_device_data(self,short=1,cleanup=0):
#		#Assumine dev is a fully qualified device file
#		#if cleanup:
####		#	self.rescan_scsi_bus()
#		#self.parse_ioscan_disks(filtertext="3PAR")
##		self.print_discovery_summary(filter="3PAR")



class hwDiscover_Linux(hwDiscover):
	def __init__(self,*args,**kargs):
		hwDiscover.__init__(self,*args,**kargs)

	def version(self):
		self.add_data("linux version",self.run("uname -a"))


	def rescan_scsi_bus(self):
		scsihostDir="/sys/class/scsi_host/"
		for hostDir in os.listdir(scsihostDir):
			path="%s/%s/scan" % (scsihostDir,hostDir)
			print "Scanning %s" % path
			dev=file(path,'w')
			dev.write("- - -")
			dev.flush()
			dev.close()
	
		print "Sleeping 10 seconds to scan"
		time.sleep(10)
		print "Rescanning multipath"
		self.run("multipath -F")
		time.sleep(10)
		print "Resetting multipath devices"
		self.run("multipath -r")
		time.sleep(10)
		
	def discover_device_data(self,short=1,cleanup=0,configure=0):
		#Assumine dev is a fully qualified device file
		if cleanup:
			self.rescan_scsi_bus()
		self.print_discovery_summary(filter="",configure=configure)

	def print_discovery_summary(self,short=1,filter="",configure=0):
		for dev in self.devList:
			if dev.find("/mpath") > -1:
				print "ERROR: Please use /dev/dm-* paths.  Run 'multipath -l' to find appropriate mapping"
				#myDev=DeviceLinuxDisk(dev)
				myDev=DeviceLinuxFile(dev)
			elif dev.find("/dm-") > -1:
				myDev=DeviceLinuxMapper(dev)
				if configure:
					myDev.configure()
			elif dev.find("/sd") > -1:
				myDev=DeviceLinuxDisk(dev)
			elif dev.find("/cciss") > -1:
				myDev=DeviceLinuxCCISS(dev)
			elif dev.find("/rbd") > -1:
				myDev=DeviceLinuxRBD(dev)
			elif dev.find("/dev") > -1:
				myDev=DeviceLinuxDisk(dev)
			else:
				myDev=DeviceLinuxFile(dev)
				#print "ERROR: Unable to understand what kind of device %s is" % dev
				#continue
			if filter and myDev.get_vendor().find(filter) == -1:
				continue
			try:
				if not short:
					myDev.print_discovery()
				else:
					myDev.print_discovery_summary()
			except: 
				pass
				#print "#%s - NoDiscovery" % dev
				#print "Unable to complete discovery of %s" % dev 
				#traceback.print_exc()

	def discover_device_files(self,modelName="",size=""):
		self.devList=self.__discover_mapper_files__(modelName,size)
		self.devList+=self.__discover_rbd_paths__()
		if not self.devList:
			print "No Devices found after scanning device mapper"

	def __discover_rbd_paths__(self):
		#Get a list of all the pools available via ceph
		import os
		devList=[]
		for row in commands.getoutput("ceph osd lspools").strip().split(","):
			if not row.strip(): continue
			poolId,poolName=row.split()
			#get a list of all the "rbd images" available to this host in each pool
			for rbdimageRaw in commands.getoutput("rbd --pool %s ls" % poolName).strip().split("\n"):
				rbdimageRaw=rbdimageRaw.strip()
				if not rbdimageRaw: continue

				rbdimagePath="/dev/rbd/%(poolName)s/%(rbdimageRaw)s" % vars()
				#dev=DeviceLinux(rbdimagePath)
				#rbdimageRealpath=os.path.realpath(rbdimagePath)
				devList.append(rbdimagePath)
				#print dev
				#print rbdimagePath
		return devList
	def __discover_mapper_files__(self,modelName="",size=""):
		devList=[]
		for devFile in commands.getoutput("ls /dev/dm*").split():
			dev=DeviceLinuxMapper(devFile)
			match=1

			if modelName and (dev.get_model().find(modelName) == -1):
				match=0
			if size and (dev.get_size().find(size) == -1):
				match=0
			if match:
				devList.append(devFile)
		return devList

class hwDiscover_Windows(hwDiscover):				
	def __init__(self,*args,**kargs):
		hwDiscover.__init__(self,*args,**kargs)

	def version(self):
		self.add_data("linux version",self.run("uname -a"))
	def discover_device_data(self,short=0,cleanup=0):
		#Assumine dev is a fully qualified device file
		#for dev in self.devList:
		dev=DeviceWindowsDisks(self.remote,self.hostname,self.username,self.password)
		dev.print_discovery_summary(short,filter="")

		if cleanup:
			dev.diskpart_cleanup()
		#return dev.get_disk_path_size_lunid()
	def get_disk_path_size_lunid(self,filter=""):
		dev=DeviceWindowsDisks(self.remote,self.hostname,self.username,self.password)
		return dev.get_disk_path_size_lunid(filter)



class hwDiscover_Sun(hwDiscover):				
	def __init__(self,*args,**kargs):
		hwDiscover.__init__(self,*args,**kargs)

	def version(self):
		self.add_data("sunos version",self.run("uname -a"))
	def discover_device_data(self,short=0,cleanup=0):
		#Assumine dev is a fully qualified device file
		#for dev in self.devList:
		dev=DeviceSunDisks(self.devList)
		if short:
			dev.print_short_discovery()
		else:
			dev.print_discovery_summary(short,filter="")
class hwDiscover_HPUX(hwDiscover):				
	def __init__(self,*args,**kargs):
		hwDiscover.__init__(self,*args,**kargs)

		self.hostname=self.get_hostname()
		self.password="hellboy!"
		self.username="perf"
		self.persona=7
		self.ipaddress="127.0.0.1"
		self.devices=[]

	def version(self):
		self.add_data("hpux version",self.run("uname -a"))

	def cleanup(self):
		print "Running cleanup on HP-UX"
		self.cleanup_stm_disk_em()
		self.cleanup_dsf()


	def cleanup_stm_disk_em(self):
		for row in self.run("ps -ef").split("\n"):
			if row.find("disk_em") > -1:
				pid=row.split()[1]
				print "Kill -9 disk_em PID %s" % pid
				self.run("kill -9 %s" % pid)


	def cleanup_dsf(self):
		print "Running ioscan"
		for row in self.run("ioscan -fnN").split("\n"):
			#print row
			if row.find("") > -1 and row.find("NO_HW") > -1:
				dsf=row.split()[2]
				print "Found and clearing %s" % dsf
				print self.run("rmsf -k -H %s" % dsf)
		print "removing stale device files"
		self.run("rmsf -x")
		print "done removing stale device files"



	def get_hostname(self):
		return self.run("hostname")
	def parse_ioscan_disks(self,filtertext=None):
		print "Running IOSCAN"
		output=self.run("ioscan -FNCdisk")
		self.devices=[]
		for row in output.split("\n"):
			#Only evaluate CLAIMED hardware
			if row.find("CLAIMED") > -1:
				if filtertext and row.find(filtertext) > -1:
					dev=HPUXDisk(row)
				elif not filtertext:
					dev=HPUXDisk(row)
				else:
					dev=None
				if dev:
					self.devices.append(dev)

	def print_discovery_summary(self,short=1,filter=""):

		self.print_columns_justified(self.get_discovery_summary())

		#for dev in self.devices:
		#	dev.print_discovery_summary()

	def get_discovery_summary(self,short=1,filtertext=""):
		data=[
			["#device","size","SCSI_Inq","alua_enabled","path_count","queue_depth"] #column names
		]
		for dev in self.devices:
			data.append(dev.get_print_data())
		return data

#	def discover_device_data(self,short=1,cleanup=0):
#		#Assumine dev is a fully qualified device file
#		#if cleanup:
	def discover_device_data(self,short=0,cleanup=0):
		#Assumine dev is a fully qualified device file
		print "Starting Discovery"
		#for dev in self.devList:
		self.parse_ioscan_disks(filtertext="")
		#dev=DeviceHPUXDisks(self.devList)
		#dev.parse_ioscan_disks(filtertext="3PAR")
		self.print_discovery_summary()
		#dev.print_discovery_summary(short,filter="3PAR")
		#if short:
		#	dev.print_short_discovery()
		#else:
		#	dev.print_discovery_summary(short,filter="3PAR")

	def get_hba_wwns(self,silent=0):
		wwnList=[]
		#dev=DeviceHPUXDisks(self.devList)
		for row in self.run("ioscan -fnkCfc").split("\n"):
			if row.find("dev") > -1:
				devfile=row.strip()
				for row in self.run("fcmsutil %s"% devfile).split("\n"):
					if row.find("N_Port Port World Wide Name") > -1:
						wwn=row.split("=")[1].strip()
						wwn=wwn.replace("0x","")
						wwnList.append(wwn.upper())

		if not silent:
			for wwn in wwnList:
				print wwn

		return wwnList


	def make_host_file(self):
		hosts_config="""
#This configuration file lists the hosts to use and configure for the test
#
#This file can be generated from an existing array configuration, or defined
#
#WWN's are 16 characters wide
#
#Persona is a numeral that represents a specific Host Mode/Host Persona.  See below for valid numbers
#Run showhost -listpersona on the Inserv for the currently supported Persona's
##
#Persona_Id Persona_Name   Persona_Caps
#         1 Generic        UARepLun,SESLun
#         2 Generic-ALUA   UARepLun,RTPG,SESLun
#         6 Generic-legacy --
#         7 HPUX-legacy    VolSetAddr
#         8 AIX-legacy     NACA
#         9 EGENERA        SoftInq
#        10 ONTAP-legacy   SoftInq
#        11 VMware         SubLun,ALUA
#
#Each Host is seperated by a line of "=" signs
=======================
hostname: %(hostname)s
ipaddress: %(ipaddress)s
username: %(username)s
password: %(password)s
persona: %(persona)s
%(wwns)s
======================="""

		wwns=[]

		for wwn in self.get_hba_wwns(silent=1):
			wwns.append("wwn: %s" % wwn)


		self.wwns="\n".join(wwns)

		config=hosts_config % vars(self)

		print config

		return config
	
		
def main():
	devList=sys.argv[1:]
	#print devList

	if os.name == "nt":
		discover=hwDiscover_Windows(devList=[])
	else:
		osname,hostname,version,kernel,spec = os.uname()
		if osname.lower() == "hp-ux":
			discover=hwDiscover_HPUX(devList=[])
		elif osname.lower() == "sunos":
			discover=hwDiscover_Sun(devList=[])
		else:
			discover=hwDiscover_Linux(devList=[])

	#Some work on Windows/Linux/Sun compatibility should be done here
	#TODO Add HP-UX support - strange that it's not here yet.  Must be because dpx rocks!  Thank you Memo
	##############
	if devList:
		if devList[0] == '-s':
			discover.discover_device_data(short=1)
		elif devList[0] == '-config':
			print "configuring"
			discover.discover_device_files()
			discover.discover_device_data(short=1,configure=1)
			#discover.make_host_file()
		elif devList[0] == '-c':
			discover.discover_device_data(short=1,cleanup=1)
		elif devList[0] == '-cleanup':
			discover.cleanup()
		elif devList[0] == '-hba':
			print "HBA"
			discover.get_hba_wwns()
		#elif devList[0] == '-config':
		#	print "config"
		#	discover.make_host_file()
		else:
			discover.discover_device_data()
	else:
		discover.discover_device_files()
		discover.discover_device_data()



