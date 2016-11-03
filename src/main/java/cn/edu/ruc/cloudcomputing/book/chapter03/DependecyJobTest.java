package cn.edu.ruc.cloudcomputing.book.chapter03;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class DependecyJobTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        // create all jobs
        Configuration jobAconf = new Configuration();
        String jobAname = "Hadoop Test JobName A";
        Job jobA = Job.getInstance(jobAconf, jobAname);

        Configuration jobBconf = new Configuration();
        String jobBname = "Hadoop Test JobName B";
        Job jobB = Job.getInstance(jobBconf, jobBname);

        Configuration jobCconf = new Configuration();
        String jobCname = "Hadoop Test JobName B depends on JobA&&JobB";
        Job jobC = Job.getInstance(jobCconf, jobCname);

        // add dependingJobs for jobC
        ArrayList<ControlledJob> dependencyJobs = new ArrayList<ControlledJob>();
        ControlledJob controlledJobC = new ControlledJob(jobC, dependencyJobs);

        ControlledJob controlledJobA = new ControlledJob(jobA, new ArrayList<ControlledJob>());
        dependencyJobs.add(controlledJobA);

        ControlledJob controlledJobB = new ControlledJob(jobB, new ArrayList<ControlledJob>());
        dependencyJobs.add(controlledJobB);

        // create JobControl object jobControl
        JobControl jobControl = new JobControl("JobControlName_test");
        jobControl.addJob(controlledJobC);

        // call run method in JobControl object jobControl
        Thread thread = new Thread(jobControl);
        thread.start();
        while (!jobControl.allFinished()) {
            Thread.sleep(10000);
        }
        jobControl.stop();
    }
}
