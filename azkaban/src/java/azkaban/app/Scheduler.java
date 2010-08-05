/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.app;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.ReadablePartial;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.PeriodFormat;

import azkaban.common.utils.Props;
import azkaban.common.utils.Utils;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowCallback;
import azkaban.flow.FlowManager;
import azkaban.flow.Status;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * A scheduler that kicks off jobs at a given time on a repeating schedule.
 *
 * @author jkreps
 */
public class Scheduler
{
    private static DateTimeFormatter FILE_DATEFORMAT = DateTimeFormat.forPattern("yyyy-MM-dd.HH.mm.ss.SSS");

    private final JobManager _jobManager;
    private final Mailman _mailman;
    private final Multimap<String, ScheduledJob> _scheduled;
    private final Multimap<String, ScheduledJob> _executing;
    private final Multimap<String, ScheduledJob> _completed;
    private final DateTimeFormatter _dateFormat = DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss:SSS");
    private final ClassLoader _baseClassLoader;
    private final String _jobSuccessEmail;
    private final String _jobFailureEmail;
    private final File _scheduleFile;
    private final File _scheduleBackupFile;

    private static Logger logger = Logger.getLogger(Scheduler.class);

    private final ScheduledThreadPoolExecutor _executor;
    private final FlowManager allKnownFlows;

    public Scheduler(JobManager jobManager,
                     FlowManager allKnownFlows,
                     Mailman mailman,
                     String jobSuccessEmail,
                     String jobFailureEmail,
                     ClassLoader classLoader,
                     File scheduleFile,
                     File backupScheduleFile,
                     int numThreads)
    {
        this.allKnownFlows = allKnownFlows;

        _scheduleFile = scheduleFile;
        _scheduleBackupFile = backupScheduleFile;
        _jobManager = Utils.nonNull(jobManager);
        _mailman = mailman;
        _completed = Multimaps.synchronizedMultimap(HashMultimap.<String, ScheduledJob>create());
        _scheduled = Multimaps.synchronizedMultimap(HashMultimap.<String, ScheduledJob>create());
        _executing = Multimaps.synchronizedMultimap(HashMultimap.<String, ScheduledJob>create());
        _baseClassLoader = classLoader;
        _jobSuccessEmail = jobSuccessEmail;
        _jobFailureEmail = jobFailureEmail;
        _executor = new ScheduledThreadPoolExecutor(numThreads,
                                                    new SchedulerThreadFactory());

        // Don't, by default, keep running scheduled tasks after shutdown.
        _executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        loadSchedule();
    }

    private void loadSchedule()
    {
        if (_scheduleFile != null && _scheduleBackupFile != null) {
            if (_scheduleFile.exists()) {
                loadFromFile(_scheduleFile);
            }
            else if (_scheduleBackupFile.exists()) {
                _scheduleBackupFile.renameTo(_scheduleFile);
                loadFromFile(_scheduleFile);

                logger.warn("Scheduler attempting to recover from backup file.");
            }
            else {
                logger.warn("No schedule files found looking for " + _scheduleFile.getAbsolutePath());
            }
        }
    }

    private void loadFromFile(File schedulefile)
    {
        Props schedule = null;
        try {
            schedule = new Props(null, schedulefile.getAbsolutePath());
        }
        catch (Exception e) {
            throw new RuntimeException("Error loading schedule from " + schedulefile);
        }

        for (String key : schedule.keySet()) {
            ScheduledJob job = parseScheduledJob(key, schedule.get(key));
            if (job != null) {
                this.schedule(job, false);
            }
        }
    }

    private ScheduledJob parseScheduledJob(String name, String job)
    {
        // name contains the job name and the time it's scheduled to run
        String[] namePieces = name.split("\\s+");
        String[] jobPieces = job.split("\\s+");
        if (namePieces.length != 2 || jobPieces.length != 2) {
            logger.warn("Error loading schedule from file " + name);
            return null;
        }

        String jobname = namePieces[0];
        DateTime time = FILE_DATEFORMAT.parseDateTime(namePieces[1]);
        ReadablePeriod period = parsePeriodString(name, jobPieces[0]);
        Boolean dependency = Boolean.parseBoolean(jobPieces[1]);
        if (dependency == null) {
            dependency = false;
        }
        if (period == null) {
            if (time.isAfterNow()) {
                return new ScheduledJob(jobname, time, period, dependency);
            }
            else {
                logger.warn("Non recurring job scheduled in past. Will not reschedule " + name);
                return null;
            }
        }

        // Update the time with the period.
        DateTime date = updatedTime(time, period);
        return new ScheduledJob(jobname, date, period, dependency);
    }

    /**
     * Schedule this job to run one time at the specified date
     *
     * @param jobId An id of the job to run
     * @param date  The date at which to kick off the job
     */
    public ScheduledFuture<?> schedule(String jobId, DateTime date, boolean ignoreDep)
    {
        logger.info("Scheduling job '" + jobId + "' for " + _dateFormat.print(date));
        return schedule(new ScheduledJob(jobId, _jobManager, date, ignoreDep), true);
    }

    /**
     * Schedule this flow to run one time now.
     *
     * @param flow The ExecutableFlow to run
     */
    public ScheduledFuture<?> scheduleNow(ExecutableFlow flow)
    {
        logger.info("Scheduling job '" + flow.getName() + "' for now");

        final ScheduledJob schedJob = new ScheduledJob(flow.getName(), _jobManager, new DateTime(), true);

        // mark the job as scheduled
        _scheduled.put(flow.getName(), schedJob);

        return _executor.schedule(new ScheduledFlow(flow, schedJob), 1, TimeUnit.MILLISECONDS);
    }

    /**
     * Schedule this job to run on a recurring basis beginning at the given dateTime and
     * repeating every period units of time forever
     *
     * @param jobId    The id for the job to schedule
     * @param dateTime The date on which to first start the job
     * @param period   The period on which the job repeats
     */
    public ScheduledFuture<?> schedule(String jobId, DateTime dateTime, ReadablePeriod period, boolean ignoreDep)
    {
        logger.info("Scheduling job '" + jobId + "' for " + _dateFormat.print(dateTime) +
                    " with a period of " + PeriodFormat.getDefault().print(period));
        return schedule(new ScheduledJob(jobId, dateTime, period, ignoreDep), true);
    }

    /**
     * Schedule the given job to run at the next occurance of the partially specified date, and repeating
     * on the given period. For example if the partial date is 12:00pm then the job will kick of the next time it is
     * 12:00pm
     *
     * @param jobId   An id for the job
     * @param partial A description of the date to run on
     * @param period  The period on which the job should repeat
     */
    public ScheduledFuture<?> schedule(String jobId, ReadablePartial partial, ReadablePeriod period, boolean ignoreDep)
    {
        // compute the next occurrence of this date
        DateTime now = new DateTime();
        DateTime date = now.withFields(partial);
        if (period != null) {
            date = updatedTime(date, period);
        }
        else if (now.isAfter(date)) {
            // Will try to schedule non recurring for tomorrow
            date = date.plusDays(1);
        }

        if (now.isAfter(date)) {
            // Schedule is non recurring.
            logger.info("Scheduled Job " + jobId + " was originally scheduled for " + _dateFormat.print(date));
            return null;
        }

        logger.info("Scheduling job '" + jobId + "' for " + _dateFormat.print(date) + (period != null ?
                                                                                       " with a period of " + PeriodFormat.getDefault().print(period) : ""));
        return schedule(new ScheduledJob(jobId, date, period, ignoreDep), true);
    }

    private ScheduledFuture<?> schedule(final ScheduledJob schedJob, boolean saveResults)
    {
        // fail fast if there is a problem with this job
        _jobManager.validateJob(schedJob.getId());

        Duration wait = new Duration(new DateTime(), schedJob.getScheduledExecution());
        if (wait.getMillis() < -1000) {
            logger.warn("Job " + schedJob.getId() + " is scheduled for " + DateTimeFormat.shortDateTime().print(schedJob.getScheduledExecution()) +
                        " which is " + (PeriodFormat.getDefault().print(wait.toPeriod())) + " in the past, adjusting scheduled date to now.");
            wait = new Duration(0);
        }

        // mark the job as scheduled
        _scheduled.put(schedJob.getId(), schedJob);

        if (saveResults) {
            try {
                saveSchedule();
            }
            catch (IOException e) {
                throw new RuntimeException("Error saving schedule after scheduling job " + schedJob.getId());
            }
        }

        ScheduledRunnable runnable = new ScheduledRunnable(schedJob);
        schedJob.setScheduledRunnable(runnable);
        return _executor.schedule(runnable, wait.getMillis(), TimeUnit.MILLISECONDS);
    }

    private DateTime updatedTime(DateTime scheduledDate, ReadablePeriod period)
    {
        DateTime now = new DateTime();
        DateTime date = new DateTime(scheduledDate);
        int count = 0;
        while (now.isAfter(date)) {
            if (count > 100000) {
                throw new IllegalStateException("100000 increments of period did not get to present time.");
            }

            if (period == null) {
                break;
            }
            else {
                date = date.plus(period);
            }

            count += 1;
        }

        return date;
    }

    private void saveSchedule() throws IOException
    {
        // Save if different
        if (_scheduleFile != null && _scheduleBackupFile != null) {
            // Delete the backup if it exists and a current file exists.
            if (_scheduleBackupFile.exists() && _scheduleFile.exists()) {
                _scheduleBackupFile.delete();
            }

            // Rename the schedule if it exists.
            if (_scheduleFile.exists()) {
                _scheduleFile.renameTo(_scheduleBackupFile);
            }

            Props prop = createScheduleProps();
            // Create the new schedule file

            prop.storeLocal(_scheduleFile);
        }
    }

    private Props createScheduleProps()
    {
        Props props = new Props();
        for (ScheduledJob job : _scheduled.values()) {
            String name = job.getId();
            ReadablePeriod period = job.getPeriod();
            String periodStr = createPeriodString(period);

            DateTime time = job.getScheduledExecution();
            String nextScheduledStr = time.toString(FILE_DATEFORMAT);

            String dependency = String.valueOf(job.isDependencyIgnored());

            props.put(name + " " + nextScheduledStr, periodStr + " " + dependency);
        }

        return props;
    }

    private ReadablePeriod parsePeriodString(String jobname, String periodStr)
    {
        ReadablePeriod period;
        char periodUnit = periodStr.charAt(periodStr.length() - 1);
        if (periodUnit == 'n') {
            return null;
        }

        int periodInt = Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
        switch (periodUnit) {
            case 'd':
                period = Days.days(periodInt);
                break;
            case 'h':
                period = Hours.hours(periodInt);
                break;
            case 'm':
                period = Minutes.minutes(periodInt);
                break;
            case 's':
                period = Seconds.seconds(periodInt);
                break;
            default:
                throw new IllegalArgumentException("Invalid schedule period unit '" + periodUnit + "' for job " + jobname);
        }

        return period;
    }

    private String createPeriodString(ReadablePeriod period)
    {
        String periodStr = "n";

        if (period == null) {
            return "n";
        }

        if (period.get(DurationFieldType.days()) > 0) {
            int days = period.get(DurationFieldType.days());
            periodStr = days + "d";
        }
        else if (period.get(DurationFieldType.hours()) > 0) {
            int hours = period.get(DurationFieldType.hours());
            periodStr = hours + "h";
        }
        else if (period.get(DurationFieldType.minutes()) > 0) {
            int minutes = period.get(DurationFieldType.minutes());
            periodStr = minutes + "m";
        }
        else if (period.get(DurationFieldType.seconds()) > 0) {
            int seconds = period.get(DurationFieldType.seconds());
            periodStr = seconds + "s";
        }

        return periodStr;
    }

    private void sendErrorEmail(ScheduledJob job, Throwable e, String senderAddress, List<String> emailList)
    {
        if ((emailList == null || emailList.isEmpty()) && _jobFailureEmail != null) {
            emailList = Arrays.asList(_jobFailureEmail);
        }

        if (senderAddress == null) {
            logger.error("Parameter mail.sender needs to be set to send emails.");
        }
        else if (emailList != null && _mailman != null) {
            try {
                _mailman.sendEmailIfPossible(senderAddress,
                                             emailList,
                                             "Job '" + job.getId() + "' has failed!",
                                             "The job '" + job.getId() + "' running on " + InetAddress.getLocalHost().getHostName() +
                                             " has failed with the following error: \r\n\r\n" +
                                             Utils.stackTrace(e) + "\r\n\r\n" +
                                             " See log for detailed message.");
            }
            catch (UnknownHostException uhe) {
                logger.error(uhe);
            }
        }
    }

    private void sendSuccessEmail(ScheduledJob job, Duration duration, String senderAddress, List<String> emailList)
    {
        if ((emailList == null || emailList.isEmpty()) && _jobSuccessEmail != null) {
            emailList = Arrays.asList(_jobSuccessEmail);
        }

        if (senderAddress == null) {
            logger.error("Parameter mail.sender needs to be set to send emails.");
        }
        else if (emailList != null && _mailman != null) {
            try {
                _mailman.sendEmailIfPossible(senderAddress,
                                             emailList,
                                             "Job '" + job.getId() + "' has completed on " + InetAddress.getLocalHost().getHostName() + "!",
                                             "The job '" + job.getId() + "' completed in " +
                                             PeriodFormat.getDefault().print(duration.toPeriod()) + ".");
            }
            catch (UnknownHostException uhe) {
                logger.error(uhe);
            }
        }
    }

    /*
     * getScheduledJobs() and getExecutingJobs() are implemented this way 
     * because of the warnings here:
     * 
     * http://google-collections.googlecode.com/svn/trunk/javadoc/com/google/common/collect/Multimaps.html#synchronizedMultimap(com.google.common.collect.Multimap)
     * 
     * The client shouldn't have to deal with synchronization.
     */

    public Collection<ScheduledJob> getScheduledJobs()
    {
        ArrayList<ScheduledJob> scheduledJobs = null;
        synchronized (_scheduled) {
            scheduledJobs = new ArrayList<ScheduledJob>(_scheduled.values());
        }
        return scheduledJobs;
    }

    public Collection<ScheduledJob> getExecutingJobs()
    {
        ArrayList<ScheduledJob> executingJobs = null;
        synchronized (_executing) {
            executingJobs = new ArrayList<ScheduledJob>(_executing.values());
        }
        return executingJobs;
    }

    public Multimap<String, ScheduledJob> getCompleted()
    {
        return _completed;
    }

    public boolean unschedule(String name, String scheduledExecutionString) {
        return unschedule(name, scheduledExecutionString, ISODateTimeFormat.dateTime());
    }

    public boolean unschedule(String name, String scheduledExecutionString, DateTimeFormatter f) {
        DateTime scheduledExecution = f.parseDateTime(scheduledExecutionString);
        return unschedule(name, scheduledExecution);
    }

    public boolean unschedule(String name, DateTime scheduledExecution)
    {
        Collection<ScheduledJob> scheduledJobs = _scheduled.get(name);
        ScheduledJob job = null;
        synchronized (scheduledJobs) {
            for (ScheduledJob scheduledJob : scheduledJobs) {
                if (scheduledJob.getScheduledExecution().equals(scheduledExecution)) {
                    job = scheduledJob;
                    break;
                }
            }
        }

        _scheduled.remove(name, job);
        if (job != null) {
            job.markInvalid();
            Runnable runnable = job.getScheduledRunnable();
            _executor.remove(runnable);
        }
        try {
            saveSchedule();
        }
        catch (IOException e) {
            throw new RuntimeException("Error saving schedule after unscheduling job " + name);
        }

        return job != null;
    }

    /**
     * A thread factory that sets the correct classloader for the thread
     */
    public class SchedulerThreadFactory implements ThreadFactory
    {
        private final AtomicInteger threadCount = new AtomicInteger(0);

        public Thread newThread(Runnable r)
        {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("scheduler-thread-" + threadCount.getAndIncrement());
            return t;
        }
    }

    /*
    public class ScheduledJobAndInstance
    {
        private final ExecutableFlow flow;
        private final ScheduledJob _scheduledJob;

        private ScheduledJobAndInstance(ExecutableFlow flow, ScheduledJob scheduledJob)
        {
            this.flow = flow;
            _scheduledJob = scheduledJob;
        }

        public ExecutableFlow getExecutableFlow()
        {
            return flow;
        }

        public ScheduledJob getScheduledJob()
        {
            return _scheduledJob;
        }
    }
    */

    /**
     * A runnable adapter for a Job
     */
    private class ScheduledRunnable implements Runnable
    {

        private final ScheduledJob _scheduledJob;
        private final boolean _ignoreDep;

        private ScheduledRunnable(ScheduledJob schedJob)
        {
            this._scheduledJob = schedJob;
            this._ignoreDep = schedJob.isDependencyIgnored();
        }

        public void run()
        {
            List<String> emailList = null;
            String senderAddress = null;
            try {
                if (_scheduledJob.isInvalid()) {
                    return;
                }

                JobDescriptor desc = _jobManager.loadJobDescriptors(null, null, _ignoreDep).get(_scheduledJob.getId());
                emailList = desc.getEmailNotificationList();

                final List<String> finalEmailList = emailList;
                
                final ExecutableFlow flowToRun = allKnownFlows.createNewExecutableFlow(_scheduledJob.getId(), new Props());

                if (_ignoreDep) {
                    for (ExecutableFlow subFlow : flowToRun.getChildren()) {
                        subFlow.markCompleted();
                    }
                }

                senderAddress = desc.getSenderEmail();
                final String senderEmail = senderAddress;
                
                // mark the job as executing
                _scheduled.remove(_scheduledJob.getId(), _scheduledJob);
                _scheduledJob.setStarted(new DateTime());
                _scheduledJob.setExecutableFlow(flowToRun);
                _executing.put(flowToRun.getName(), _scheduledJob);
                flowToRun.execute(new FlowCallback()
                {
                    @Override
                    public void progressMade()
                    {
                        allKnownFlows.saveExecutableFlow(flowToRun);
                    }

                    @Override
                    public void completed(Status status)
                    {
                        _scheduledJob.setEnded(new DateTime());

                        try {
                            allKnownFlows.saveExecutableFlow(flowToRun);
                            switch (status) {
                                case SUCCEEDED:
                                    sendSuccessEmail(_scheduledJob, _scheduledJob.getExecutionDuration(), senderEmail, finalEmailList);
                                    break;
                                case FAILED:
                                    sendErrorEmail(_scheduledJob, flowToRun.getException(), senderEmail, finalEmailList);
                                    break;
                                default:
                                    sendErrorEmail(_scheduledJob, new RuntimeException(String.format("Got an unknown status[%s]", status)), senderEmail, finalEmailList);
                            }
                        }
                        catch (RuntimeException e) {
                            logger.warn("Exception caught while saving flow/sending emails", e);
                            throw e;
                        }
                        finally {
                            // mark the job as completed
                            _executing.remove(_scheduledJob.getId(), _scheduledJob);
                            _completed.put(_scheduledJob.getId(), _scheduledJob);

                            // if this is a recurring job, schedule the next execution as well
                            if (_scheduledJob.isRecurring() && !_scheduledJob.isInvalid()) {
                                DateTime nextRun = _scheduledJob.getScheduledExecution().plus(_scheduledJob.getPeriod());
                                // This call will also save state.
                                schedule(_scheduledJob.getId(),
                                         nextRun,
                                         _scheduledJob.getPeriod(),
                                         _ignoreDep);
                            }
                            else {
                                try {
                                    saveSchedule();
                                }
                                catch (IOException e) {
                                    logger.warn("Error trying to update schedule.");
                                }
                            }
                        }
                    }
                });

                allKnownFlows.saveExecutableFlow(flowToRun);
            }
            catch (Throwable t) {
                if (emailList != null) {
                    sendErrorEmail(_scheduledJob, t, senderAddress, emailList);
                }
                _scheduled.remove(_scheduledJob.getId(), _scheduledJob);
                _executing.remove(_scheduledJob.getId(), _scheduledJob);
                logger.warn(String.format("An exception almost made it back to the ScheduledThreadPool from job[%s]", _scheduledJob), t);
            }
        }
    }

        /**
     * A runnable adapter for a Job
     */
    private class ScheduledFlow implements Runnable
    {
        private final ExecutableFlow _flow;
        private final ScheduledJob _scheduledJob;


        private ScheduledFlow(
                ExecutableFlow flow,
                ScheduledJob scheduledJob
        )
        {
            this._flow = flow;
            this._scheduledJob = scheduledJob;
        }

        public void run()
        {
            logger.info("Starting run of " + _flow.getName());

            List<String> emailList = null;
            String senderAddress = null;
            try {
                emailList = _jobManager.getJobDescriptor(_flow.getName()).getEmailNotificationList();
                final List<String> finalEmailList = emailList;

                senderAddress = _jobManager.getJobDescriptor(_flow.getName()).getSenderEmail();
                final String senderEmail = senderAddress;
                
                // mark the job as executing
                _scheduled.remove(_scheduledJob.getId(), _scheduledJob);
                _scheduledJob.setStarted(new DateTime());
                _scheduledJob.setExecutableFlow(_flow);
                _executing.put(_flow.getName(), _scheduledJob);
                _flow.execute(new FlowCallback()
                {
                    @Override
                    public void progressMade()
                    {
                        allKnownFlows.saveExecutableFlow(_flow);
                    }

                    @Override
                    public void completed(Status status)
                    {
                        _scheduledJob.setEnded(new DateTime());

                        try {
                            allKnownFlows.saveExecutableFlow(_flow);
                            switch (status) {
                                case SUCCEEDED:
                                    sendSuccessEmail(_scheduledJob, _scheduledJob.getExecutionDuration(), senderEmail, finalEmailList);
                                    break;
                                case FAILED:
                                    sendErrorEmail(_scheduledJob, _flow.getException(), senderEmail, finalEmailList);
                                    break;
                                default:
                                    sendErrorEmail(_scheduledJob, new RuntimeException(String.format("Got an unknown status[%s]", status)), senderEmail, finalEmailList);
                            }
                        }
                        catch (RuntimeException e) {
                            logger.warn("Exception caught while saving flow/sending emails", e);
                            throw e;
                        }
                        finally {
                            // mark the job as completed
                            _executing.remove(_scheduledJob.getId(), _scheduledJob);
                            _completed.put(_scheduledJob.getId(), _scheduledJob);
                        }
                    }
                });

                allKnownFlows.saveExecutableFlow(_flow);
            }
            catch (Throwable t) {
                if (emailList != null) {
                    sendErrorEmail(_scheduledJob, t, senderAddress, emailList);
                }
                _scheduled.remove(_scheduledJob.getId(), _scheduledJob);
                _executing.remove(_scheduledJob.getId(), _scheduledJob);
                logger.warn(String.format("An exception almost made it back to the ScheduledThreadPool from job[%s]", _scheduledJob), t);
            }
        }
    }
}
