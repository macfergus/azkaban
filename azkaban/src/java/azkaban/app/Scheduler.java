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
import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
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
import azkaban.util.JSONToJava;
import azkaban.serialization.Verifier;
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

    private void loadFromFile(File scheduleFile)
    {
        InputStream in = null;
        byte[] buf = new byte[(int)scheduleFile.length()];
        String scheduleStr = null;
        try {
            in = new BufferedInputStream(new FileInputStream(scheduleFile.getAbsolutePath()));
            in.read(buf);
            scheduleStr = new String(buf);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Trying to load schedule file at path " + scheduleFile.getAbsolutePath() + ", which does not exist.");
        } catch (IOException e) {
            throw new RuntimeException("Error reading schedule from file at path " + scheduleFile.getAbsolutePath() + ".");
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // Ignore.
            }
        }
        
        if (scheduleStr.charAt(0) != '{') {
            logger.info("The schedule file isn't serialized using JSON. Loading using properties instead and converting it to JSON.");
            propsToSchedule(scheduleFile);
            try {
                saveSchedule();
            } catch (IOException e) {
                throw new RuntimeException("Error saving schedule after loading schedule from props.");
            }
            logger.info("Successfully read the schedule file using properties and converted it to JSON.");
        } else {
            JSONObject scheduleJSON = null;
            try {
                scheduleJSON = new JSONObject(scheduleStr);
            } catch (JSONException e) {
                throw new RuntimeException("Error reading JSON from the schedule file at path " + scheduleFile.getAbsolutePath() + ".");
            }
            JSONToSchedule(scheduleJSON);
        }
    }

    /**
     * Schedules jobs from a schedule JSON object.
     */
    private void JSONToSchedule(JSONObject scheduleJSON) {
        Map<String, Object> scheduleMap = (new JSONToJava()).apply(scheduleJSON);
        
        for (String jobName : scheduleMap.keySet()) {
            ArrayList<Object> schedJobs = (ArrayList<Object>) scheduleMap.get(jobName);
            for (Object schedJobObject : schedJobs) {
                Map<String, Object> schedJob = (Map<String, Object>) schedJobObject;
                Verifier.verifyKeysExist(schedJob, "nextScheduled", "period", "ignoreDeps", "recurImmediately");
                DateTime nextScheduled = FILE_DATEFORMAT.parseDateTime((String)schedJob.get("nextScheduled"));
                ReadablePeriod period = parsePeriodString(jobName, (String)schedJob.get("period"));
                if (period == null && !nextScheduled.isAfterNow()) {
                    logger.warn("Non recurring job scheduled in past. Will not reschedule (" + jobName + "," + nextScheduled + ")");
                    continue;
                }
                Boolean ignoreDeps = Boolean.parseBoolean((String)schedJob.get("ignoreDeps"));
                Boolean recurImmediately = Boolean.parseBoolean((String)schedJob.get("recurImmediately"));

                ScheduledJob toSchedule = new ScheduledJob(jobName, nextScheduled, period, ignoreDeps, recurImmediately);
                schedule(toSchedule, false);
            }
        }
    }

    /**
     * Schedules jobs in a schedule properties file.
     *
     * This should only be called when upgrading Azkaban from a version that serializes
     * the schedule using properties seen in 0.4 to a version that serializes the 
     * schedule using JSON
     */
    private void propsToSchedule(File schedulefile) {
        Props schedule = null;
        try {
            schedule = new Props(null, schedulefile.getAbsolutePath());
        }
        catch (Exception e) {
            throw new RuntimeException("Error loading schedule from " + schedulefile);
        }
        
        for (String key : schedule.keySet()) {
            ScheduledJob job = parseScheduledJobProps(key, schedule.get(key));
            if (job != null) {
                this.schedule(job, false);
            }
        }
    }

    /**
     * Parse scheduled job properties as they're serialized in Azkaban 0.4.
     *
     * This should only be called when upgrading. See propsToSchedule().
     */
    private ScheduledJob parseScheduledJobProps(String name, String job) {
        String[] pieces = job.split("\\s+");

        if (pieces.length != 3) {
            logger.warn("Error loading schedule from file " + name);
            return null;
        }
        
        DateTime nextScheduled = FILE_DATEFORMAT.parseDateTime(pieces[0]);
        ReadablePeriod period = parsePeriodString(name, pieces[1]);
        Boolean ignoreDeps = Boolean.parseBoolean(pieces[2]);
        if (ignoreDeps == null) {
            ignoreDeps = false;
        }

        if (period == null && !nextScheduled.isAfterNow()) {
            logger.warn("Non recurring job scheduled in past. Will not reschedule (" + name + "," + period.toString() + ")");
            return null;
        }
        return new ScheduledJob(name, nextScheduled, period, ignoreDeps, false);
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
        return schedule(new ScheduledJob(jobId, _jobManager, date, ignoreDep, false), true);
    }

    /**
     * Restart this flow to run now.
     *
     * @param flow The ExecutableFlow to run
     */
    public ScheduledFuture<?> restartFlow(ExecutableFlow flow)
    {
        logger.info("Scheduling job (" + flow.getName() + "," + flow.getId() + ") for now");

        String scheduledTimeString = flow.getOverrideProps().get("azkaban.flow.scheduled.timestamp");
        DateTime now = new DateTime();
        DateTime scheduledTime =
            scheduledTimeString == null
            ? now
            : ISODateTimeFormat.dateTime().parseDateTime(scheduledTimeString);

        Duration gap = new Duration(now, scheduledTime);
        if (gap.getMillis() > 0) {
            // scheduledTime is greater than now, which shouldn't be the case
            throw new IllegalStateException("Scheduled time for restarted flow (" + flow.getName() + "," + flow.getId() + ") is after the current time. This shouldn't happen.");
        }
        
        final ScheduledJob schedJob = new ScheduledJob(flow.getName(), _jobManager, scheduledTime, true, false);
        schedJob.setExecutableFlow(flow);
        schedJob.markRestarted();
        
        // mark the job as scheduled
        _scheduled.put(schedJob.getId(), schedJob);

        ScheduledRunnable runnable = new ScheduledRunnable(schedJob);
        schedJob.setScheduledRunnable(runnable);
        return _executor.schedule(runnable, 1, TimeUnit.MILLISECONDS);
    }

    /**
     * Schedule this job to run on a recurring basis beginning at the given dateTime and
     * repeating every period units of time forever
     *
     * @param jobId    The id for the job to schedule
     * @param dateTime The date on which to first start the job
     * @param period   The period on which the job repeats
     */
    public ScheduledFuture<?> schedule(String jobId, DateTime dateTime, ReadablePeriod period, boolean ignoreDep, boolean recurImmediately)
    {
        logger.info("Scheduling job '" + jobId + "' for " + _dateFormat.print(dateTime) +
                    " with a period of " + PeriodFormat.getDefault().print(period));
        return schedule(new ScheduledJob(jobId, dateTime, period, ignoreDep, recurImmediately), true);
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
    public ScheduledFuture<?> schedule(String jobId, ReadablePartial partial, ReadablePeriod period, boolean ignoreDep, boolean recurImmediately)
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
        return schedule(new ScheduledJob(jobId, date, period, ignoreDep, recurImmediately), true);
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
            // Update ScheduledJob executionTime
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

            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(_scheduleFile));
            try {
                JSONObject schedule = scheduleToJSON();
                out.write(schedule.toString().getBytes());
            } catch (JSONException e) {
                throw new IOException("There was an error converting the schedule to JSON.");
            } finally {
                out.close();
            }
        }
    }

    /**
     * Converts a job schedule to JSON like so:
     *
     * {
     *   "myjob": [{"period": "...", "nextScheduled": "...", "ignoreDeps": true/false, "recurImmediately": true/false},
     *             {"period": "...", "nextScheduled": "...", "ignoreDeps": true/false, "recurImmediately": true/false}]
     *   
     *   "anotherjob": [{"period": "...", "nextScheduled": "...", "ignoreDeps": true/false, "recurImmediately": true/false},
     *                  {"period": "...", "nextScheduled": "...", "ignoreDeps": true/false, "recurImmediately": true/false},
     *                  {"period": "...", "nextScheduled": "...", "ignoreDeps": true/false, "recurImmediately": true/false}]
     *   ...
     * }
     */
    private JSONObject scheduleToJSON() throws JSONException {
        JSONObject schedule = new JSONObject();
        Set<String> jobNames = _scheduled.keySet();

        synchronized (_scheduled) {
            for (String jobName : jobNames) {
                JSONArray jobs = new JSONArray();

                Collection<ScheduledJob> schedJobs = _scheduled.get(jobName);
                for (ScheduledJob schedJob : schedJobs) {
                    String periodStr = createPeriodString(schedJob.getPeriod());
                    String nextScheduledStr = schedJob.getScheduledExecution().toString(FILE_DATEFORMAT);

                    JSONObject properties = new JSONObject();
                    properties.put("period", periodStr);
                    properties.put("nextScheduled", nextScheduledStr);
                    properties.put("ignoreDeps", schedJob.isDependencyIgnored());
                    properties.put("recurImmediately", schedJob.doesRecurImmediately());
                    jobs.put(properties);
                }

                schedule.put(jobName, jobs);
            }
        }

        return schedule;
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
        ScheduledJob jobToUnschedule = null;
        synchronized (scheduledJobs) {
            for (ScheduledJob scheduledJob : scheduledJobs) {
                if (scheduledJob.getScheduledExecution().equals(scheduledExecution)) {
                    jobToUnschedule = scheduledJob;
                    break;
                }
            }
        }

        _scheduled.remove(name, jobToUnschedule);
        if (jobToUnschedule != null) {
            jobToUnschedule.markInvalid();
            Runnable runnable = jobToUnschedule.getScheduledRunnable();
            _executor.remove(runnable);
        }
        try {
            saveSchedule();
        }
        catch (IOException e) {
            throw new RuntimeException("Error saving schedule after unscheduling job " + name);
        }

        return jobToUnschedule != null;
    }

    public boolean cancel(String name, String startTimeString) {
        return cancel(name, startTimeString, ISODateTimeFormat.dateTime());
    }

    public boolean cancel(String name, String startTimeString, DateTimeFormatter f) {
        DateTime startTime = f.parseDateTime(startTimeString);
        return cancel(name, startTime);
    }

    public boolean cancel(String name, DateTime startTime) {
        Collection<ScheduledJob> executingJobs = _executing.get(name);
        ScheduledJob jobToCancel = null;
        synchronized (executingJobs) {
            for (ScheduledJob executingJob : executingJobs) {
                if (executingJob.getStarted().equals(startTime)) {
                    jobToCancel = executingJob;
                    break;
                }
            }
        }

        if (jobToCancel != null) {
            ExecutableFlow flow = jobToCancel.getExecutableFlow();
            return (flow != null && flow.cancel());
        } else {
            return false;
        }
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

    /**
     * A runnable adapter for a Job
     */
    private class ScheduledRunnable implements Runnable
    {

        private final ScheduledJob _scheduledJob;
        private final boolean _ignoreDep;
        private final boolean _restarted;
        private final boolean _recurImmediately;

        private ScheduledRunnable(ScheduledJob schedJob)
        {
            this._scheduledJob = schedJob;
            this._ignoreDep = schedJob.isDependencyIgnored();
            this._recurImmediately = schedJob.doesRecurImmediately();
            this._restarted = schedJob.isRestarted();
        }

        public void run()
        {
            logger.info("Starting run of " + _scheduledJob.getId());
            if (_restarted) {
                logger.info("This is a restart of flow with ID " + _scheduledJob.getExecutableFlow().getId());
            }
            List<String> emailList = null;
            String senderAddress = null;
            try {
                if (_scheduledJob.isInvalid()) {
                    return;
                }

                JobDescriptor desc = 
                    _restarted
                    ? _jobManager.getJobDescriptor(_scheduledJob.getId())
                    : _jobManager.loadJobDescriptors(null, null, _ignoreDep).get(_scheduledJob.getId());

                emailList = desc.getEmailNotificationList();
                final List<String> finalEmailList = emailList;

                senderAddress = desc.getSenderEmail();
                final String senderEmail = senderAddress;
                
                final ExecutableFlow flowToRun;
                if (_restarted) {
                    // Already have an executable flow we're restarting.
                    flowToRun = _scheduledJob.getExecutableFlow();
                } else {
                    // Get a new executable flow.
                    Props overrideProps = new Props();

                    DateTime scheduledTime = _scheduledJob.getScheduledExecution();
                    overrideProps.put("azkaban.flow.scheduled.timestamp", scheduledTime.toString());
                    overrideProps.put("azkaban.flow.scheduled.year", scheduledTime.toString("yyyy"));
                    overrideProps.put("azkaban.flow.scheduled.month", scheduledTime.toString("MM"));
                    overrideProps.put("azkaban.flow.scheduled.day", scheduledTime.toString("dd"));
                    overrideProps.put("azkaban.flow.scheduled.hour", scheduledTime.toString("HH"));
                    overrideProps.put("azkaban.flow.scheduled.minute", scheduledTime.toString("mm"));
                    overrideProps.put("azkaban.flow.scheduled.seconds", scheduledTime.toString("ss"));
                    overrideProps.put("azkaban.flow.scheduled.milliseconds", scheduledTime.toString("SSS"));
                    overrideProps.put("azkaban.flow.scheduled.timezone", scheduledTime.toString("ZZZZ"));
                    
                    flowToRun = allKnownFlows.createNewExecutableFlow(_scheduledJob.getId(), overrideProps);
                    if (_ignoreDep) {
                        for (ExecutableFlow subFlow : flowToRun.getChildren()) {
                            subFlow.markCompleted();
                        }
                    }
                    _scheduledJob.setExecutableFlow(flowToRun);
                }

                // Schedule the next job if it's recurring and recurs immediately.
                if (_scheduledJob.isRecurring() && !_scheduledJob.isInvalid() && _recurImmediately) {
                    DateTime nextRun = _scheduledJob.getScheduledExecution().plus(_scheduledJob.getPeriod());
                    // This call will also save state.
                    schedule(_scheduledJob.getId(),
                             nextRun,
                             _scheduledJob.getPeriod(),
                             _ignoreDep,
                             _recurImmediately);
                }

                // mark the job as executing
                _scheduled.remove(_scheduledJob.getId(), _scheduledJob);
                _scheduledJob.setStarted(new DateTime());
                _executing.put(_scheduledJob.getId(), _scheduledJob);
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

                            // If this is a recurring job and it doesn't recur immediately, schedule the next execution as well
                            if (_scheduledJob.isRecurring() && !_scheduledJob.isInvalid() && !_recurImmediately) {
                                DateTime nextRun = _scheduledJob.getScheduledExecution().plus(_scheduledJob.getPeriod());
                                // This call will also save state.
                                schedule(_scheduledJob.getId(),
                                         nextRun,
                                         _scheduledJob.getPeriod(),
                                         _ignoreDep,
                                         _recurImmediately);
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
}
