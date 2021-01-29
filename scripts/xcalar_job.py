#!/usr/bin/env python3

import os
import sys
import json
import optparse
import uuid
import traceback
from contextlib import contextmanager
try:
    import boto3
except ImportError:
    pass


class XcalarJob:
    def __init__(self, workbucket: str, app_name: str, s3_path: str, module_name: str, sched_exp: str, aws_target_stack: str, params: str, job_name: str, keep_cluster: bool, aws_instance_type: str, cluster_size: int):
        """Xcalar Job contains the logic to construct and upload a job configuration JSON to AWS Infrastructure to trigger scheduled xcalar jobs.

        Args:
            workbucket (str): The workbucket associated with the instance or job.
            app_name (str): The application name that the job is going to run.
            s3_path (str): The S3 path to the packaged application tar.gz
            module_name (str): The module name that the application will run.
            sched_exp (str): The AWS compatible schedule expression that the job will adhere to.
            aws_target_stack (str): The target stack to run the application if stack is not ephemeral.
            params (str): The comma delimited list of key=value params to pass to the application.
            job_name (str): The job name unique to the instance of the application/scheduler expresison.
            keep_cluster (bool): Whether to keep the cluster after the application has run (ephemeral case).
            aws_instance_type(str): The AWS Instance Type. i.e. r5d.xlarge
            cluster_size(int): The number of nodes for the cluster.

        Raises:
            ValueError: If the workbucket environment variable is not setup. 
        """
        if job_name:
            self.job_name = job_name
        else:
            self.job_name = str(uuid.uuid4())[:-29]
        if workbucket:
            self.workbucket = workbucket
        else:
            # eg "sharedinf-workbucket-559166403383-us-west-2"
            self.workbucket = os.environ.get("WORKBUCKET")
        if not self.workbucket:
            raise ValueError("${WORKBUCKET} empty")
        self.eventprefix = os.environ.get("EVENTPREFIX", ".events/")
        self.s3_path = s3_path
        self.dfpath = None
        self.uuid = str(uuid.uuid4())
        self.app_name = app_name
        self.module_name = module_name
        self.sched_exp = sched_exp
        self.aws_target_stack = aws_target_stack
        self.params = [] if params is None else params
        self.keep_cluster = False if keep_cluster is None else keep_cluster
        self.aws_instance_type = aws_instance_type
        self.cluster_size = cluster_size

    @staticmethod
    @contextmanager
    def get_reader(inp='', mode='r'):
        assert mode == 'r'
        try:
            fildes = None
            if inp in ('-', ''):
                yield sys.stdin
            else:
                fildes = open(inp, mode)
                yield fildes
        finally:
            if fildes:
                fildes.close()

    @staticmethod
    def _pprint(doc):
        print(json.dumps(doc, indent=4))

    def create(self):
        parray = self.params.split(",")
        paramarray = "{} {}".format(
            "--params", ','.join(parray) if len(parray) > 0 else '')
        s3workbook = self.s3_path
        dataflow_name = self.module_name
        base_workbook = os.path.basename(s3workbook)
        wparts = base_workbook.split('.')
        workbook_name = wparts[0]
        query = '-'.join([self.job_name, self.uuid[:8]])
        tmp_workbook = '{}_{}.{}'.format(workbook_name, self.job_name,
                                         '.'.join(wparts[1:]))
        uid = os.getuid()
        write_dir = f"/var/tmp/{uid}"
        if not os.path.exists(write_dir):
            os.makedirs(write_dir)
        xcstr = (f"xc2 workbook delete {workbook_name}_{query} && "
                 f"aws s3 cp {s3workbook} {write_dir}/{tmp_workbook} && "
                 f"xc2 workbook run --workbook-file {write_dir}/{tmp_workbook} "
                 f"--query-name {query} --dataflow-name "
                 f"{dataflow_name} {paramarray} --sync")
        final_json = {
            'command': xcstr,
            'schedule': self.sched_exp,
            'keep_cluster': self.keep_cluster,
            'aws_target_stack': self.aws_target_stack,
            'aws_instance_type': self.aws_instance_type,
            'cluster_size': self.cluster_size
        }
        return final_json

    def upload(self, jobspec):
        jobkey = os.path.join(self.eventprefix, self.job_name + ".json")
        s3object = boto3.resource('s3').Object(self.workbucket, jobkey)
        jobjson = json.dumps(jobspec, indent=4) + "\n"
        s3object.put(Body=(bytes(jobjson.encode('UTF-8'))))
        sys.stderr.write(
            f'Wrote {self.job_name}.json to s3://{self.workbucket}/{jobkey}\n')
        sys.stdout.write(jobjson + '\n')
        return f"s3://{self.workbucket}/{jobkey}"

    def list(self):
        kwargs = {'Bucket': self.workbucket, 'Prefix': self.eventprefix, 'Delimiter': '/'}
        client = boto3.client('s3')
        while True:
            try:
                resp = client.list_objects_v2(**kwargs)
                if 'Contents' in resp:
                    for content in resp['Contents']:
                        basename = os.path.basename(content['Key'])
                        if not basename.endswith('.json'):
                            continue
                        if basename.startswith('.'):
                            continue
                        yield {
                            "Job": os.path.splitext(basename)[0],
                            "ModTime":
                                content["LastModified"].strftime(
                                    "%Y-%m-%d %H-%M-%S")
                        }
                if 'IsTruncated' in resp:
                    if not resp['IsTruncated']:
                        break
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def delete(self, job):
        boto3.resource('s3').Object(
            self.workbucket,
            "{}.json".format(os.path.join(self.eventprefix, job))).delete()

    def view(self, job):
        obj = boto3.resource('s3').Object(
            self.workbucket,
            "{}.json".format(os.path.join(self.eventprefix, job)))
        return obj.get()['Body'].read().decode('utf-8')

    @staticmethod
    def verify_schedule(schedule):
        # 'cron' and 'rate' are the same length, so just pick one
        lenp = len('rate')
        valid = len(schedule) >= len('rate(1 day)') and \
                schedule[lenp] == '(' and schedule.endswith(')')
        if valid:
            sched_type = schedule[:lenp]
            sched_expr = schedule[lenp+1:-1].split(' ')
            if (sched_type, len(sched_expr)) in [('cron', 6), ('rate', 2)]:
                return
        raise ValueError(\
                f"Unrecognized schedule expression: {schedule}\n" \
                "Must use one of: cron(min hour dom month dow year) or rate(value unit) formats\n" \
                "See https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html\n")


if __name__ == "__main__":
    # yapf: disable
    parser = optparse.OptionParser()
    parser.add_option('-j', '--jobName', action="store", dest="jobName", help="job name")
    parser.add_option('-a', '--app_name', action="store", dest="appName", help="app name")
    parser.add_option('-p', '--s3_path', action="store", dest="s3Path", help="app location on s3")
    parser.add_option('-m', '--module_name', action="store", dest="moduleName", help="module to be run")
    parser.add_option('-s', '--schedule_exp', action="store", dest="schedExp", help="AWS compatible schedule expression")
    parser.add_option('-r', '--params', action="store", dest="params", help="comma delimited list of key=value params")
    parser.add_option('-i', '--aws_instance_type', action="store", dest="awsInstanceType", help="The AWS instance type (i.e. r5d.xlarge)")
    parser.add_option('-z', '--cluster_size', type=int, action="store", dest="clusterSize", help="The number of nodes for the cluster")
    parser.add_option('-t', '--aws_target_stack', action="store", dest="awsTargetStack", help="the AWS target stack if always-on")
    parser.add_option('-k', '--keep_cluster', action="store_true", dest="keepCluster", help="prevent shutdown of cluster (for debug)")
    # parser.add_option('-c', '--jobConfig', action="store", dest="jobConfig", help="job config json")
    parser.add_option('-c', '--createJob', action="store_true", dest="createJob", help="create a job")
    parser.add_option('-l', '--listJobs', action="store_true", dest="listJobs", help="list jobs")
    parser.add_option('-d', '--deleteJob', action="store", dest="deleteJob", help="delete job")
    parser.add_option('-v', '--viewJob', action="store", dest="viewJob", help="view job")
    parser.add_option('-w', '--workbucket', action="store", dest="workbucket", help="workbucket")
    # yapf: endable
    options, args = parser.parse_args()
    if not options.createJob and not options.listJobs and not options.deleteJob and not options.viewJob:
        parser.print_help()
        sys.exit(1)
    xcalarJob = XcalarJob(options.workbucket, options.appName, options.s3Path, options.moduleName, options.schedExp, options.awsTargetStack, options.params, options.jobName, options.keepCluster, options.awsInstanceType, options.clusterSize)
    try:
        if options.createJob:
            jobSpec = xcalarJob.create()
            xcalarJob.verify_schedule(jobSpec['schedule'])
            output = xcalarJob.upload(jobSpec)
        elif options.listJobs:
            for xcjob in xcalarJob.list():
                print(xcjob)
        elif options.deleteJob:
            sys.stderr.write(f'Deleting job {options.deleteJob}\n')
            xcalarJob.delete(options.deleteJob)
        elif options.viewJob:
            print(xcalarJob.view(options.viewJob))
    except Exception as e:
        print(e)
        traceback.print_exc()
        sys.exit(1)
    sys.exit(0)
