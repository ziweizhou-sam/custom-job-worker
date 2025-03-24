import boto3
import time
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CodePipelinePoller:
    def __init__(self):
        self.codepipeline_client = boto3.client('codepipeline')
        self.polling_interval = 60  # 60 seconds = 1 minute

    def poll_jobs(self):
        try:
            response = self.codepipeline_client.poll_for_jobs(
                actionTypeId={
                    'category': 'Approval',  # Modify as per your needs
                    'owner': 'Custom',
                    'provider': 'AmazonQ',
                    'version': '1'
                },
                maxBatchSize=10
            )

            jobs = response.get('jobs', [])

            for job in jobs:
                job_id = job['id']
                logger.info(f"Found job: {job_id}")

                # Process the job here
                self.process_job(job)

        except ClientError as e:
            logger.error(f"Error polling for jobs: {str(e)}")

    def process_job(self, job):
        try:
            # Add your job processing logic here
            job_id = job['id']

            # Example: Acknowledge the job
            self.codepipeline_client.acknowledge_job(
                jobId=job_id,
                nonce=job['nonce']
            )

            # Your job processing logic goes here

            # After successful processing, mark the job as succeeded
            self.codepipeline_client.put_job_success_result(
                jobId=job_id
            )

            logger.info(f"Successfully processed job: {job_id}")

        except ClientError as e:
            logger.error(f"Error processing job: {str(e)}")

            # Mark the job as failed
            self.codepipeline_client.put_job_failure_result(
                jobId=job_id,
                failureDetails={
                    'type': 'JobFailed',
                    'message': str(e)
                }
            )

    def start_polling(self):
        logger.info("Starting CodePipeline job poller...")

        while True:
            try:
                self.poll_jobs()
                time.sleep(self.polling_interval)
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                time.sleep(self.polling_interval)

def main():
    poller = CodePipelinePoller()
    poller.start_polling()

if __name__ == "__main__":
    main()