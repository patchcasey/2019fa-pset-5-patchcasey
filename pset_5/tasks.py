import os
from luigi import ExternalTask
from csci_utils.luigi.dask import target as csci_task
from luigi import BoolParameter, Task
from csci_utils.luigi import task as csci_utils_req
from s3fs.core import S3File, _fetch_range as _backend_fetch
from csci_utils import hash_str

# A bug! _fetch_range does not pass through request kwargs
def _fetch_range(self, start, end):
    # Original _fetch_range does not pass req_kw through!
    return _backend_fetch(self.fs.s3, self.bucket, self.key, self.version_id, start, end, req_kw=self.fs.req_kw)
S3File._fetch_range = _fetch_range

DATA_ROOT = 's3://cscie29-data/'
HASH_ID= hash_str.get_csci_salt(keyword="HASHED_ID",convert_to_bytes=False)
S3DIRECTORY = '/pset_5/yelp_data/'
FULL_S3_DIRECTORY = DATA_ROOT + HASH_ID + S3DIRECTORY

class YelpReviews(ExternalTask):

    S3ROOT = 's3://cscie29-data/46a4bb62/pset_5/yelp_data'
    # S3ROOT = 's3://pset5data/data'
    # uploaded to personal bucket - this does not work either

    output = csci_utils_req.TargetOutput(target_class=csci_task.CSVTarget,
                                         filepattern=S3ROOT,
                                         flag=None,
                                         storage_options=dict(requester_pays=True),
                                         ext="/")

class CleanedReviews(Task):
    subset = BoolParameter(default=True)

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure
    requires = csci_utils_req.Requires()
    other = csci_utils_req.Requirement(YelpReviews)
    print(other)
    path = os.path.abspath('data/subset') + '/'

    output = csci_utils_req.TargetOutput(file_pattern=path,
                                         target_class=csci_task.ParquetTarget,
                                         ext="",
                                         storage_options=dict(requester_pays=True))

    def run(self):

        numcols = ["funny", "cool", "useful", "stars"]
        dsk = self.input().read_dask(self)
        print(dsk)

        if self.subset:
            dsk = dsk.get_partition(0)

        out = ...
        self.output().write_dask(out, compression='gzip')



