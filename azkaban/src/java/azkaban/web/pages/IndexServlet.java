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

package azkaban.web.pages;

import azkaban.app.AzkabanApplication;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.flow.ExecutableFlow;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;

import azkaban.app.JobDescriptor;
import azkaban.app.ScheduledJob;
import azkaban.web.AbstractAzkabanServlet;

import azkaban.common.web.Page;

/**
 * The main page
 * 
 * @author jkreps
 * 
 */
public class IndexServlet extends AbstractAzkabanServlet {

    private static final Logger logger = Logger.getLogger(IndexServlet.class.getName());

    private static final long serialVersionUID = 1;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        AzkabanApplication app = getApplication();
        Map<String, JobDescriptor> descriptors = app.getJobManager().loadJobDescriptors();
        Page page = newPage(req, resp, "azkaban/web/pages/index.vm");
        page.add("logDir", app.getLogDirectory());
        page.add("flows", app.getAllFlows());
        page.add("scheduled", app.getScheduler().getScheduledJobs());
        page.add("executing", app.getScheduler().getExecutingJobs());
        page.add("completed", app.getScheduler().getCompleted());
        page.add("rootJobNames", app.getAllFlows().getRootFlowNames());
        page.add("jobDescComparator", JobDescriptor.NAME_COMPARATOR);
        page.render();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        AzkabanApplication app = getApplication();
        String action = getParam(req, "action");
        if("unschedule".equals(action)) {
            String job = getParam(req, "job");
            String scheduledExecution = getParam(req, "scheduled_execution");
            app.getScheduler().unschedule(job, scheduledExecution);
        } else if("cancel".equals(action)) {
            cancelJob(app, req);
        } else if("schedule".equals(action)) {
            scheduleJobs(app, req, resp);
        } else {
            throw new ServletException("Unknown action: " + action);
        }
        resp.sendRedirect(req.getContextPath());
    }

    private void cancelJob(AzkabanApplication app, HttpServletRequest req) throws ServletException {
        String job = getParam(req, "job");
        String startTime = getParam(req, "start_time");
        try {
            if (app.getScheduler().cancel(job, startTime)) {
                addMessage(req, "Cancelled " + job);
                logger.info("Job ('" + job + "','" + startTime + "') cancelled from gui.");
            } else {
                logger.info("Couldn't cancel flow ('" + job + "','" + startTime + "')" + "' for some reason.");
                addError(req, "Failed to cancel flow ('" + job + "','" + startTime + "')");
            }
        } catch (Exception e) {
            logger.error("Exception while attempting to cancel flow ('" + job + "','" + startTime + "')'", e);
            addError(req, "Failed to cancel flow ('" + job + "','" + startTime + "'): " + e.getMessage());
        }
    }

    private void scheduleJobs(AzkabanApplication app, HttpServletRequest req, HttpServletResponse resp)
            throws IOException, ServletException {
        String[] jobNames = req.getParameterValues("jobs");
        if(!hasParam(req, "jobs")) {
            addError(req, "You must select at least one job to run.");
            return;
        }
        for(String job: jobNames) {
            if(hasParam(req, "schedule")) {
                int hour = getIntParam(req, "hour");
                boolean isPm = getParam(req, "am_pm").equalsIgnoreCase("pm");
                if(isPm && hour < 12)
                    hour += 12;
                hour %= 24;

                int minutes = getIntParam(req, "minutes");
                
				String dateString = getParam(req, "date");
				DateTime date = DateTimeFormat.forPattern("MM/dd/yyyy").parseDateTime(dateString)
                                                                       .withHourOfDay(hour)
                                                                       .withMinuteOfHour(minutes);

                ReadablePeriod thePeriod = null;
                if(hasParam(req, "is_recurring")) {
                    thePeriod = parsePeriod(req);
                }

                boolean recurImmediately = hasParam(req, "recur_immediately");

                app.getScheduler().schedule(job,
                                            date,
                                            thePeriod,
                                            false,
                                            recurImmediately);
                addMessage(req, job + " scheduled.");
            } else if(hasParam(req, "run_now")) {
                boolean ignoreDeps = !hasParam(req, "include_deps");
                ScheduledFuture<?> f = app.getScheduler().schedule(job, new DateTime(), ignoreDeps);
                addMessage(req, "Running " + job);
            } else {
                addError(req, "Neither run_now nor schedule param is set.");
            }
        }

    }

    private ReadablePeriod parsePeriod(HttpServletRequest req) throws ServletException {
        int period = getIntParam(req, "period");
        String periodUnits = getParam(req, "period_units");
        if("d".equals(periodUnits))
            return Days.days(period);
        else if("h".equals(periodUnits))
            return Hours.hours(period);
        else if("m".equals(periodUnits))
            return Minutes.minutes(period);
        else if("s".equals(periodUnits))
            return Seconds.seconds(period);
        else
            throw new ServletException("Unknown period unit: " + periodUnits);
    }

}
