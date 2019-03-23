# Generate a PBS script for a job, and general utilities for
# waiting for a job to complete.

from shell_command import shellCommand
import sys
from time import sleep
from tempfile import NamedTemporaryFile
import os


# this assumes that qstat info for a job will stick around for a while after
# the job has finished.

class Runnable_Script(object):
    def __init__(self, qstat_max_tries = 5, qstat_error_delay = 1, qstat_delay = 10):
        self.qstat_max_tries = qstat_max_tries      # number of times to try qstat before failing
        self.qstat_error_delay = qstat_error_delay  # seconds to sleep while waiting for qstat to recover
        self.qstat_delay = qstat_delay              # seconds to sleep while waiting for job to complete
        pass
        
    def isJobCompleted(self, jobID):
        count = 0
        while True:
            (stdout, stderr, exitStatus) = shellCommand("qstat -f %s" % jobID)
            # qstat appears to have worked correctly, we can stop trying.
            if exitStatus == 0 or count >= self.qstat_max_tries:
                break
            count += 1
            sleep(self.qstat_error_delay)
        if exitStatus != 0:
            raise Exception("qstat -f %s returned non-zero exit status %d times,\
                             panicking" % (jobID, count))
        else:
            # try to fetch the exit status of the job command from the output of
            # qstat.
            jobState = None
            exitStatus = None
            for line in stdout.split('\n'):
                ws = line.split()
                if len(ws) == 3:
                    if ws[0] == 'job_state' and ws[1] == '=':
                        jobState = ws[2]
                    elif ws[0] == 'exit_status' and ws[1] == '=' and \
                            ws[2].isdigit():
                        exitStatus = int(ws[2])
            if jobState.upper() == 'C':
                # Job has completed.
                return (True, exitStatus)
            else:
                # Job has not completed.
                return (False, exitStatus)


    # returns exit status of job (or None if it can't be determined)
    def waitForJobCompletion(self, jobID):
        isFinished, exitCode = self.isJobCompleted(jobID)
        while(not isFinished):
            sleep(self.qstat_delay)
            isFinished, exitCode = self.isJobCompleted(jobID)
        return exitCode


    # returns exit status of job (or None if it can't be determined)
    def runJobAndWait(self, stage, logDir='', verbose=0):
        jobID = self.launch()
        prettyJobID = jobID.split('.')[0]
        logFilename = os.path.join(logDir, stage + '.' + prettyJobID + '.pbs')
        with open(logFilename, 'w') as logFile:
            logFile.write(self.__str__())
        if verbose > 0:
            print('stage = %s, jobID = %s' % (stage, prettyJobID))
        return self.waitForJobCompletion(jobID)


# Generate a PBS script for a job.
class PBS_Script(Runnable_Script):
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
        super(PBS_Script, self).__init__(**kw)
        pass

    # render the job script as a string.
    def __str__(self):
        script = ['#!/bin/bash']
        # XXX fixme
        # should include job id in the output name.
        # should use the proper log directory.
        if self.queue == 'terri-smp':
            script.append('#PBS -q terri')
            script.append('#PBS -l procs=8,tpn=8')
        else:
            script.append('#PBS -q %s' % self.queue)
        if self.logDir:
            script.append('#PBS -o %s' % self.logDir)
            script.append('#PBS -e %s' % self.logDir)
        # should put the name of the file in here if possible
        if self.name:
            script.append('#PBS -N %s' % self.name)
        if self.memInGB:
            if self.queue in ['smp', 'terri-smp']:
                script.append('#PBS -l mem=%sgb' % self.memInGB)
            else:
                script.append('#PBS -l pvmem=%sgb' % self.memInGB)
        if self.walltime:
            script.append('#PBS -l walltime=%s' % self.walltime)
        # copy the literal text verbatim into the end of the PBS options
        # section.
        if self.literals:
            script.append(self.literals)
        if type(self.moduleList) == list and len(self.moduleList) > 0:
            for item in self.moduleList:
                script.append('module load %s' % item)
        script.append('cd $PBS_O_WORKDIR')
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
            return stdout
        else:
            raise(Exception('qsub command failed with exit status: ' +
                  str(returnCode)))


class SLURM_Job(object):
    def __init__(self, command, walltime=None, name=None, memInGB=None,
        queue=None, moduleList=None, logDir=None, literals=None, **kw):
        def is_int_str(x):
            return type(x) == str and x.isdigit()
        self.command = command
        self.walltime = '--time=' + walltime if walltime is not None else ''
        self.name = '--job-name=' + name if name is not None else ''
        # XXX should really check that memInGB is an int
        self.mem = '--mem=' + str(memInGB * 1024) if memInGB is not None else ''
        self.moduleList = moduleList
        self.logDir = logDir if logDir is not None else ''
        self.queue = ''
        self.literals = literals if literals is not None else ''

    def __str__(self):
        script = ['#!/bin/bash']
        if type(self.moduleList) == list and len(self.moduleList) > 0:
            for item in self.moduleList:
                script.append('module load %s' % item)
        script.append(self.command)
        return '\n'.join(script) + '\n'

    def run_job_and_wait(self, stage, verbose=0):
        logFilename = os.path.join(self.logDir, stage + '.sh')
        file = NamedTemporaryFile(dir='')
        file.write(str(self))
        file.flush()
        stderr_file = os.path.join(self.logDir, stage + '.%j.stderr')
        stdout_file = os.path.join(self.logDir, stage + '.%j.stdout')
        command = 'srun --error={stderr} --output={stdout} {memory} {literals} {queue} {jobname} {walltime} bash {file_name}'.format(
            stderr=stderr_file, stdout=stdout_file, memory=self.mem,
            literals=self.literals, queue=self.queue, jobname=self.name,
            walltime=self.walltime, file_name=file.name)
        with open(logFilename, 'w') as logFile:
            logFile.write(self.__str__())
            logFile.write('\n# ' + command + '\n')
        if verbose > 0:
            print('stage = ' + stage)
        (stdout, stderr, returnCode) = shellCommand(command)
        file.close()
        return returnCode

#class SGE_Script(Runnable_Script):
#    def __init__(self, command, walltime=None, name=None, memInGB=None,
#                 queue='batch', moduleList=None, logDir=None, **kw):
#        self.command = command
#        self.queue = queue
#        self.name = name
#        self.memInGB = memInGB
#        self.walltime = walltime
#        self.moduleList = moduleList
#        self.logDir = logDir
#        self.Runnable_Script.__init__(**kw)
#        pass
