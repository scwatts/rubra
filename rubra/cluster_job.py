# Generate a SGE script for a job, and general utilities for
# waiting for a job to complete.

from shell_command import shellCommand
import sys
from time import sleep
from tempfile import NamedTemporaryFile
import os
import re


JOBID_RE = re.compile(r'^Your job ([0-9]+) .+$')


# this assumes that qstat info for a job will stick around for a while after
# the job has finished.

class Runnable_Script(object):
    def __init__(self, qstat_max_tries = 5, qstat_error_delay = 2, qstat_delay = 10):
        self.qstat_max_tries = qstat_max_tries      # number of times to try qstat before failing
        self.qstat_error_delay = qstat_error_delay  # seconds to sleep while waiting for qstat to recover
        self.qstat_delay = qstat_delay              # seconds to sleep while waiting for job to complete

    def jobCompletionStatus(self, jobID):
        count = 0
        while True:
            # TEMP: hack get accounting info from qmaster
            import subprocess
            host = 'bio21cluster1.bio21.unimelb.edu.au'
            filepath = '/opt/gridengine/default/common/accounting'
            account_file_command = 'ssh %s "cat %s"' % (host, filepath)
            status_command = 'qacct -j %s -f -' % jobID

            account_file_proc = subprocess.Popen(account_file_command, stdout=subprocess.PIPE, shell=True)
            status_proc = subprocess.Popen(status_command, stdin=account_file_proc.stdout,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

            account_file_proc.stdout.close()
            (stdout, stderr) = status_proc.communicate()
            exitStatus = status_proc.returncode

            # qacct appears to have worked correctly, we can stop trying.
            if exitStatus == 0 or count >= self.qstat_max_tries:
                break
            count += 1
            sleep(self.qstat_error_delay)
        if exitStatus != 0:
            raise Exception("qacct -j %s returned non-zero exit status %d times,\
                             panicking" % (jobID, count))
        else:
            # try to fetch the exit status of the job command from the output of
            # qstat.
            jobState = None
            exitStatus = None
            line_token_gen = (line.split() for line in stdout.rstrip().split('\n'))
            for line_tokens in line_token_gen:
                if line_tokens[0] == 'exit_status' and line_tokens[1].isdigit():
                    return int(line_tokens[1])
            else:
                raise Exception('Error parsing qacct output, couldn\'t find exit_status for %s' %
                        jobID)


    # returns exit status of job (or None if it can't be determined)
    def waitForJobCompletion(self, jobID):
        # Wait until jobID no longer appears in qstat
        while self.jobInQstat(jobID):
            sleep(self.qstat_delay)

        # After job finishes, wait for qmaster to update account info and for the new file to
        # propogate throughout nfs
        sleep(5)

        # Ensure that the job finished successfully
        exitCode = self.jobCompletionStatus(jobID)
        return exitCode


    # returns exit status of job (or None if it can't be determined)
    def runJobAndWait(self, stage, logDir='', verbose=0):
        # Launch job and ensure that it is visible in qstat output
        jobID = self.launch()
        prettyJobID = jobID.split('.')[0]
        if not self.jobInQstat(prettyJobID):
            raise Exception('Job %s appears to have failed, consult logs' % prettyJobID)
        logFilename = os.path.join(logDir, stage + '.' + prettyJobID + '.pbs')
        with open(logFilename, 'w') as logFile:
            logFile.write(self.__str__())
        if verbose > 0:
            print('stage = %s, jobID = %s' % (stage, prettyJobID))
        return self.waitForJobCompletion(jobID)


    def jobInQstat(self, jobID):
        (stdout, stderr, exitStatus) = shellCommand('qstat')
        if exitStatus != 0:
            raise Exception('qstat returned a non-zero exit status:' + stdout)
        if not stdout:
            return False
        line_token_gen = (line.split() for line in stdout.rstrip().split('\n'))
        jobids = {lts[0] for lts in line_token_gen}
        return jobID in jobids


# Generate a SGE script for a job.
class SGE_Script(Runnable_Script):
    def __init__(self, command, walltime=None, name=None, memInGB=None,
                 queue='batch', moduleList=None, logDir=None, literals=None, **kw):
        self.command = command
        self.queue = queue
        self.name = name
        self.memInGB = memInGB
        self.walltime = walltime
        self.moduleList = moduleList
        self.logDir = logDir
        self.literals = literals
        super(SGE_Script, self).__init__(**kw)

    # render the job script as a string.
    def __str__(self):
        script = ['#!/bin/bash']
        # Set login shell to BASH
        script.append('#$ -S /bin/bash')
        script.append('#$ -cwd')
        # should include job id in the output name.
        # should use the proper log directory.
        script.append('#$ -q %s' % self.queue)
        if self.logDir:
            stdout_fp = os.path.join(self.logDir, self.name + '.$JOB_ID.stdout')
            stderr_fp = os.path.join(self.logDir, self.name + '.$JOB_ID.stderr')
            script.append('#$ -o %s' % stdout_fp)
            script.append('#$ -e %s' % stderr_fp)
        # should put the name of the file in here if possible
        if self.name:
            script.append('#$ -N %s' % self.name)
        if self.memInGB:
            script.append('#$ -l h_vmem=%sG' % self.memInGB)
        if self.walltime:
            script.append('#$ -l h_rt=%s' % self.walltime)
        # copy the literal text verbatim into the end of the SGE options
        # section.
        if self.literals:
            script.append(self.literals)
        if type(self.moduleList) == list and len(self.moduleList) > 0:
            for item in self.moduleList:
                script.append('module load %s' % item)
        script.append(self.command)
        return '\n'.join(script) + '\n'

    # create a temporary file to store the job script and then
    # launch it with qsub.
    def launch(self):
        file = NamedTemporaryFile()
        file.write(str(self))
        file.flush()
        command = 'qsub ' + file.name
        (stdout, stderr, returnCode) = shellCommand(command)
        file.close()
        if returnCode == 0:
            # Get jobid from stdout (bio21 hpc sge)
            try:
                return JOBID_RE.match(stdout).group(1)
            except AttributeError:
                raise(Exception('qsub returned unexpected stdout:' +
                    str(returnCode)))
        else:
            raise(Exception('qsub command failed with exit status: ' +
                  str(returnCode)))
