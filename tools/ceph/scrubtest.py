import os
import time
import commands

#root@hp380-37:~# ceph pg dump|grep -i active |head -50
#dumped all in format plain
#2.1af	711	0	0	0	0	2949857299	3081	3081	active+clean	2018-07-20 04:00:11.189857	956'6143899	956:7074795	[17,0,10]	17	[17,0,10]	17	956'5824651	2018-07-20 04:00:11.188563	956'5824651	2018-07-20 04:00:11.188563
#4.1a9	620	0	0	0	0	2600468480	3027	3027	active+clean	2018-07-20 04:13:38.084333	956'81561	956:95257	[9,26,27]	9	[9,26,27]	9	956'69144	2018-07-20 04:13:38.079190	956'69144	2018-07-20 04:13:38.079190

def get_pg_status():
	pglist=[]
	scrublist=[]
	command="ceph pg dump"
	for row in commands.getoutput(command).split("\n"):
		if row.find("active") == -1:
			continue
		row=row.split()
		if len(row) > 6:
			try:
				pgid=row[0]
				object_count=int(row[1])
				scrubstamp=row[-1]
				status=row[9]
				#ignore very small PG populations
				if object_count < 5:
					continue
				if status.find("scrub") > -1:
					scrublist.append([scrubstamp,pgid])
				else:
					pglist.append([scrubstamp,pgid])
			except:continue

	#should use an presorted list
	pglist.sort()		
	pglist.reverse()
	return pglist,scrublist

def start_scrub(pgid):
	print "starting scrub for %s" % pgid
	command="ceph pg deep-scrub %s" % pgid
	commands.getoutput(command)
	return
def run_test(scrubcount=1,timeout=3600):
	starttime=time.time()
	endtime=starttime+timeout
	while time.time() < endtime:
		pglist,scrublist=get_pg_status()
		delta=scrubcount-len(scrublist)
		while delta > 0:
			scrubstamp,pgid=pglist.pop()
			start_scrub(pgid)
			delta-=1
			time.sleep(5)
		else:
			for pgrow in scrublist:
				print "%s in progress %s - remaining seconds %s" % (pgrow[0],pgrow[1],(endtime-time.time()))
		time.sleep(10)

if __name__ == "__main__":
	run_test(4,3600)
