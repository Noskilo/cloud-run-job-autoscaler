import { Injectable } from '@nestjs/common';
import { MetricServiceClient } from '@google-cloud/monitoring';
import { JobsClient } from '@google-cloud/run';
import { CloudSchedulerClient } from '@google-cloud/scheduler';

@Injectable()
export class AppService {
  private metricServiceClient: MetricServiceClient;
  private jobsClient: JobsClient;
  private cloudSchedulerClient: CloudSchedulerClient;

  private projectId = process.env.APP_PROJECT_ID;

  constructor() {
    this.metricServiceClient = new MetricServiceClient({
      projectId: this.projectId,
    });

    this.jobsClient = new JobsClient({
      projectId: this.projectId,
    });

    this.cloudSchedulerClient = new CloudSchedulerClient({
      projectId: this.projectId,
    });
  }

  async pauseScheduler(jobId: string) {
    if (!this.projectId) {
      throw new Error('APP_PROJECT_ID is not set');
    }

    const name = this.cloudSchedulerClient.jobPath(
      this.projectId,
      'us-central1',
      jobId,
    );

    const [job] = await this.cloudSchedulerClient.getJob({
      name,
    });

    if (job.state === 'PAUSED') {
      return;
    }

    await this.cloudSchedulerClient.pauseJob({
      name: name,
    });

    console.log(`Paused job: ${name}`);
  }

  async resumeScheduler(jobId: string) {
    if (!this.projectId) {
      throw new Error('APP_PROJECT_ID is not set');
    }

    const name = this.cloudSchedulerClient.jobPath(
      this.projectId,
      'us-central1',
      jobId,
    );

    const [job] = await this.cloudSchedulerClient.getJob({
      name,
    });

    if (job.state === 'ENABLED') {
      return;
    }

    await this.cloudSchedulerClient.resumeJob({
      name: name,
    });

    console.log(`Resumed job: ${name}`);
  }

  async getNumUndeliverdMessages(subscriptionId: string) {
    if (!this.projectId) {
      throw new Error('APP_PROJECT_ID is not set');
    }

    const [response] = await this.metricServiceClient.listTimeSeries({
      name: this.metricServiceClient.projectPath(this.projectId),
      filter: `metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" AND resource.labels.subscription_id="${subscriptionId}"`,
      interval: {
        startTime: {
          // Start time (e.g., 1 minutes ago)
          seconds: Date.now() / 1000 - 1 * 60,
        },
        endTime: {
          // End time (e.g., now)
          seconds: Date.now() / 1000,
        },
      },
    });

    for (const series of response) {
      const { points } = series;

      return (
        points?.reduce((acc, point) => {
          const { value } = point;

          return acc + Number(value?.int64Value ?? 0);
        }, 0) ?? 0
      );
    }

    return 0;
  }

  async modifyJobParallelism(newParallelism: number) {
    if (!this.projectId) {
      throw new Error('APP_PROJECT_ID is not set');
    }

    // Construct the fully qualified job name
    const name = this.jobsClient.jobPath(
      this.projectId,
      'us-central1',
      'cloud-run-job-test',
    );

    try {
      // Fetch the existing job
      const [job] = await this.jobsClient.getJob({ name });

      // Update the job
      const [operation] = await this.jobsClient.updateJob({
        job: {
          ...job,
          template: {
            ...job.template,
            parallelism: newParallelism,
            taskCount: newParallelism,
          },
        },
      });

      const [response] = await operation.promise();

      console.log(
        `Job ${response.name} updated with parallelism: ${newParallelism}`,
      );
    } catch (error) {
      console.error('Failed to update the job:', error);
    }
  }
}
