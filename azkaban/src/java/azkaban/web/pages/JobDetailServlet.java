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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.app.AzkabanApplication;
import azkaban.app.JobDescriptor;
import azkaban.app.JobExecution;
import azkaban.app.JobManager;
import azkaban.web.AbstractAzkabanServlet;

import azkaban.common.utils.Props;
import azkaban.common.web.Page;

/**
 * Show the details of a job
 * 
 * @author jkreps
 * 
 */
public class JobDetailServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1;

    private static final Logger logger = Logger.getLogger(JobDetailServlet.class);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        AzkabanApplication app = getApplication();
        String jobId = req.getParameter("id");
        JobManager jobManager = app.getJobManager();
        Map<String, JobDescriptor> descriptors = jobManager.loadJobDescriptors();
        boolean isEditing = req.getParameter("edit") != null;
        if(jobId == null) {
            Page page = newPage(req, resp, "azkaban/web/pages/edit_job.vm");
            page.render();
        } else if(isEditing) {
            Page page = newPage(req, resp, "azkaban/web/pages/edit_job.vm");
            page.add("job", descriptors.get(jobId));
            page.render();
        } else {
            Page page = newPage(req, resp, "azkaban/web/pages/job_detail.vm");
            page.add("job", descriptors.get(jobId));
            page.add("descriptors", descriptors);
            List<JobExecution> execs = jobManager.loadJobExecutions(jobId);
            int successes = 0;
            for(JobExecution exec: execs)
                if(exec.isSucceeded())
                    successes++;
            page.add("executions", execs);
            page.add("successful_executions", successes);

            page.render();
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        AzkabanApplication app = getApplication();
        JobManager jobManager = app.getJobManager();
        String jobName = req.getParameter("job_name");
        String jobPath = req.getParameter("job_path");
        Map<String, String> props = new HashMap<String, String>();
        for(int i = 0;; i++) {
            String key = req.getParameter("key" + i);
            String val = req.getParameter("val" + i);
            if(key == null || val == null)
                break;
            if(key.length() > 0)
                props.put(key, val);
        }
        try {
            jobManager.deployJob(jobName, jobPath, new Props(null, props));
            addMessage(req, jobName + " has been deployed to " + jobPath);
            resp.sendRedirect(req.getContextPath() + "/job?id=" + jobName);
        } catch(Exception e) {
            logger.error("Failed to deploy job.", e);
            addError(req, "Failed to deploy job: " + e.getMessage());
            resp.sendRedirect(req.getContextPath() + "/job?id=" + jobName + "&edit");
        }
    }

}
