import multiprocessing
from multiprocessing import Pool
import boto3
import logging
 
logging.basicConfig(level=logging.INFO)
Bucket = 'emr-sample-data'
Prefix = 'marker/5MBGZ'
MaxKeys = 1000
ProcessCount =  2
 
log = logging.getLogger(__name__)
 
class ListS3(multiprocessing.Process):
    def __init__(self, queue, bucket, prefix, maxKeys):
        multiprocessing.Process.__init__(self)
        log.debug("List Worker: Init")
        self.queue = queue
        self.bucket = bucket
        self.prefix = prefix
        self.maxKeys = maxKeys
        self.client = boto3.client('s3')
        self.isTruncated = True
        self.keyMarker = None
        self.version_list = None
        self.exit = False
 
    def run(self):
        log.info("List Worker: Getting lists")
        while self.isTruncated == True:
            log.debug("List Worker: Looping")
            if not self.keyMarker:
                log.debug("List Worker: not self.keyMarker = 1")
                self.version_list = self.client.list_object_versions(Bucket=self.bucket, MaxKeys=self.maxKeys,Prefix=self.prefix)
            else:
                log.debug("List Worker: not self.keyMarker = 0 ")
                self.version_list = self.client.list_object_versions(Bucket=self.bucket, MaxKeys=self.maxKeys, KeyMarker=self.keyMarker, Prefix=self.prefix)
            
            if self.version_list['IsTruncated']:
                self.keyMarker = self.version_list['NextKeyMarker']
 
            isTruncated = self.version_list['IsTruncated']
            log.info("List Worker: Put items into Queue")
            if 'DeleteMarkers' in self.version_list:
                self.queue.put(self.version_list['DeleteMarkers'])
            else:
                log.error("No delete makers found, exiting")
                for i in range(ProcessCount+1):
                    self.queue.put('STOP')
                break
 
            if not self.version_list['IsTruncated']:
               # self.version_list['IsTruncated'] = False
                for i in range(ProcessCount+1):
                    log.debug("List Worker: STOPPING")
                    self.queue.put('STOP')
                break
        return 0
        
 
def recoverS3(q, empt=None):
    client = boto3.client('s3')
    delete_markers = q.get()
    log.info("Restore Worker: Getting list from Queue")
    while delete_markers != "STOP":
        objects_to_delete = []
        for d in delete_markers:
           if (d['IsLatest'] == True):
                objects_to_delete.append({'VersionId':d['VersionId'],'Key':d['Key']})
        log.debug("Restore Worker: Running restores")
        client.delete_objects(Bucket=Bucket,Delete={'Objects': objects_to_delete})
        delete_markers = q.get()
    return 0
 
 
if __name__ == '__main__':
 
    log.info("MAIN: Creating Queue")
    q = multiprocessing.Queue()
 
    log.info("MAIN: Created list processes")
    listprocess = ListS3(q, Bucket, Prefix, MaxKeys)
    log.info("MAIN: Start listing")
    listprocess.start()
 
    log.info("MAIN: Starting Restore workers")
    recs = []
    for i in range(ProcessCount):
        log.info("MAIN: Spinning up recs")
        recs.append(multiprocessing.Process(target=recoverS3, args=(q,)).start())
 
 
    listprocess.join()
    log.info("MAIN: listing completed")
    for rec in recs:
        if rec != None:
            rec.join()
   
log.info("Restore complete")
