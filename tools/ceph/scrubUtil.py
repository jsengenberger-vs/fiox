import os
import sys
import time
import commands
import traceback
from optparse import OptionParser

#
# Author: John Sengenberger 
#
#TODO: Add pool overrides onto CLI
#TODO: simplifly run on code
#TODO: Poll status of deep-scrub on specific PGs instead of polling entire cluster
############

#root@hp380-37:~# ceph pg dump|grep -i active |head -50
#dumped all in format plain
#2.1af	711	0	0	0	0	2949857299	3081	3081	active+clean	2018-07-20 04:00:11.189857	956'6143899	956:7074795	[17,0,10]	17	[17,0,10]	17	956'5824651	2018-07-20 04:00:11.188563	956'5824651	2018-07-20 04:00:11.188563
#4.1a9	620	0	0	0	0	2600468480	3027	3027	active+clean	2018-07-20 04:13:38.084333	956'81561	956:95257	[9,26,27]	9	[9,26,27]	9	956'69144	2018-07-20 04:13:38.079190	956'69144	2018-07-20 04:13:38.079190

def get_pools():
    pools={}
    try:
        pooldata=commands.getoutput("ceph osd lspools")
        for pool in pooldata.strip().split(","):
            if not pool.strip(): continue
            poolid,poolname=pool.split()
            pools[poolid]=poolname
    except:
        traceback.print_exc()
        print "Error: Unable to parse output of ceph osd lspools"
    
    return pools


def get_pg_status():
        pools=get_pools()
        pool_pgs={}
	pglist=[]
	scrublist=[]
	command="ceph pg dump pgs"
	for row in commands.getoutput(command).split("\n"):
		if row.find("active") == -1:
			continue
		row=row.split()
		if len(row) > 6:
			try:
				pgid=row[0]

                                #pgid format looks like "<poolid>.<pgid>", e.g. "14.2d6"
                                #############
                                poolid=pgid.split(".")[0]

                                poolname=pools[poolid]

                                if not pool_pgs.has_key(poolname):
                                    pool_pgs[poolname]={'scrublist':[],'pglist':[]}

				object_count=int(row[1])
                                #Hammer
				scrubstamp=row[-1]
                                #Luminous
                                scrubstamp="%s %s" % (row[-3],row[-2])
				status=row[9]
				#ignore very small PG populations
				if object_count < 5:
					continue
				if status.find("scrub") > -1:
                                        pool_pgs[poolname]['scrublist'].append([scrubstamp,pgid,object_count])
					scrublist.append([scrubstamp,pgid,object_count])
				else:
                                        pool_pgs[poolname]['pglist'].append([scrubstamp,pgid,object_count])
					pglist.append([scrubstamp,pgid,object_count])
			except:continue

	#should use an presorted list
	pglist.sort()		
	pglist.reverse()

        #Return the original pglist,scrublist, and the per pool pg dictionary
        ##########
	return pglist,scrublist,pool_pgs

def start_scrub(pgid,silent=0):
        if not silent:print "starting scrub for %s" % pgid
	#print "starting scrub for %s" % pgid
	command="ceph pg deep-scrub %s" % pgid
	commands.getoutput(command)
	return

def is_master():
    hostname=commands.getoutput('hostname').strip().split(".")[0]
    primary=None
    try:
        primary=commands.getoutput("ceph status|grep mgr").strip().split("mgr:")[1].split("(")[0].strip()
        if len(primary) == 0:
            primary=commands.getoutput("ceph status|grep quorum").strip().split(",")[-1]
    except: 
        if primary == None or len(primary) == 0:
            primary=commands.getoutput("ceph status|grep quorum").strip().split(",")[-1]
        #traceback.print_exc()
        #print "Error: Unable to parse cephstatus."
        return False
    if primary != None and primary.strip() == hostname.strip():
        return True
    return False


def get_number_active_scrubs(poolname):
    pglist,scrublist,pool_pgs=get_pg_status()
    scrublist=pool_pgs[poolname]['scrublist']
    return len(scrublist)

def wait_scrubbing_complete(silent=0):
    while 1:
        pgN,scrublist,pool_pgs=get_pg_status()
        if len(scrublist) <= 0:
            break
        if not silent:
            print "Waiting for %s scrubs to complete before starting" % len(scrublist)
        time.sleep(5)
    return 


def run_production_scrubbing(numDays=56,runOnce=False,runForever=True):

    if not is_master():
        print "Exiting... Not master server"
        sys.exit()

    wait_scrubbing_complete(silent=0)

    seconds=numDays*24*3600
    numDeepScrubs=0

    while 1:
        pglist,scrublist,pool_pgs=get_pg_status()
        num_pgs=len(pglist)
        if num_pgs < 1:
            print "Error: Unable to find any PGS to scrub. Waiting 60 seconds to try again"
            time.sleep(60)
            continue

        time_delay=seconds/num_pgs

        if len(scrublist) > 0:
            print "Deep Scrub in progress, skipping deep-scrub operation"

        scrubstamp,pgid,object_count=pglist.pop()
        print "%s -- Starting deep-scrub %s" % (time.ctime(),pgid)
        start_scrub(pgid)
        numDeepScrubs+=1
        starttime=time.time()

        #Wait 10 seconds after requesting deep-scrub operation to start monitoring for its completion
        time.sleep(10)

        wait_scrubbing_complete(silent=1)


        scrubtime=int(time.time()-starttime)
        nexttime=(time.time()-scrubtime)+time_delay
        time_remaining=nexttime-time.time()
        print "%s -- pgid: %s, deep-scrub-time: %s,next_start: %s" % (time.ctime(),pgid,scrubtime,time.ctime(nexttime))

        if runOnce:
            break
        else:
            time.sleep(time_remaining)
    return numDeepScrubs


def run_scrub_measurement_test(poolname=None,sampleCount=30,iosize=512):
    pg_counts=[1]

    for pg_count in pg_counts:

        pglist,scrublist,pool_pgs=get_pg_status()
        pglist=pool_pgs[poolname]['pglist']
        scrublist=pool_pgs[poolname]['scrublist']
        pglist.sort()
        pglist.reverse()
        #print "waiting for active scrubs"
        #while get_number_active_scrubs(poolname):
        #    time.sleep(1)

        print "#"
        print "#"
        print "Starting test"
        print "#"
        print "#"
        print "pgid\tobjcount\tscrub_ops\tscrub_svt\tseconds"
        while sampleCount > 0:
            scrubstamp,pgid,object_count=pglist.pop()
            start_scrub(pgid,silent=1)
            starttime=time.time()
            time.sleep(5)

            while get_number_active_scrubs(poolname) > 0:
                time.sleep(1)

            deltatime=time.time()-starttime

            est_scrub_read_ops=(int(object_count)*4000)/iosize
            est_scrub_op_servicetime=deltatime/float(est_scrub_read_ops)

            print "%s\t%s\t%s\t%s\t%s" % (pgid,object_count,est_scrub_read_ops,est_scrub_op_servicetime,deltatime)
            time.sleep(60)

            sampleCount-=1
    return 1


def run_time_test(scrubcount=1,timeout=3600,poolname=None):
	starttime=time.time()
	endtime=starttime+timeout
	while time.time() < endtime:
		pglist,scrublist,pool_pgs=get_pg_status()

                if poolname != None:
                    print "Restricting PGS to pool %s" % poolname
                    if not pool_pgs.has_key(poolname):
                        print "Unable to find pool %s in cluster" % poolname
                        sys.exit()
                    else:
                        pglist=pool_pgs[poolname]['pglist']
                        scrublist=pool_pgs[poolname]['scrublist']
	                pglist.sort()		
	                pglist.reverse()
		delta=scrubcount-len(scrublist)
		while delta > 0:
                    #pay or may not contain object count
                    scrubstamp,pgid,object_count=pglist.pop()
                    scrubstamp,pgid
                    start_scrub(pgid)
                    delta-=1
                    time.sleep(5)
		else:
			for pgrow in scrublist:
				print "%s in progress %s - remaining seconds %s" % (pgrow[0],pgrow[1],(endtime-time.time()))
		time.sleep(10)
def config_cli():
    usage="""
%prog [option]
    """
    parser=OptionParser()

    parser.add_option("--scrub_one",dest="scrub_one",help="Runs one deep-scrub operation on one PG (oldest unscrubbed).  Terminates after deep-scrub completed",action='store_true')
    parser.add_option("--scrub_all",dest="scrub_all",help="Runs deep-scrub operations in a loop designed to execute over a 56 day schedule",action='store_true')
    parser.add_option("--dev_tt",dest="run_time_test",help="Internal/Dev for performance profiling: runs deep-scrub non-stop for 1800s",action='store_true')
    parser.add_option("--dev_pt",dest="run_scrub_perf_test",help="Interal/Dev for performance profiling: Measure length of time for Deep-Scrub",action='store_true')

    (options,args) = parser.parse_args()

    if len(args) >1:
        print "Error: Missing argument: 1 argument required"
        sys.exit()

    return options

def main():
    parser=config_cli()
    if parser.scrub_one:
        print "Running deep-scrub against one PG"
        run_production_scrubbing(numDays=56,runOnce=True,runForever=False)
    elif parser.scrub_all:
        print "Running deep-scrub in 56 day loop"
        run_production_scrubbing(numDays=56,runOnce=False,runForever=True)
    elif parser.run_time_test:
        print "Running Time Test"
        #Note: Expand parser CLI options to make parms inputs
        run_time_test(scrubcount=1,timeout=1800,poolname="volumes")
    elif parser.run_scrub_perf_test:
        print "Running scrub perf test"
        #Note: Expand parser CLI options to make parms inputs
        run_scrub_measurement_test(poolname="volumes",sampleCount=10)
    else:
        print "Error: Missing valid argument: 1 argument required"
    return 1

if __name__ == "__main__":
    main()
