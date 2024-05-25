import subprocess
import logging
from google.cloud import pubsub_v1
from google.cloud import scheduler_v1
from google.cloud import functions_v2
from google.cloud.functions_v1.types import ListFunctionsRequest
from google.protobuf import duration_pb2
from utils.bq_utils import BqUtils
from jobs.cloud_schemas import CloudSchemas


class GcpUtils:
    def __init__(self, bq_client):
        self.__bq = BqUtils(bq_client=bq_client)
        self.__cs = CloudSchemas()
        self.__pubsub_client = pubsub_v1.PublisherClient()
        self.__scheduler_client = scheduler_v1.CloudSchedulerClient()
        self.__functions_client = functions_v2.FunctionServiceClient()

    def does_pubsub_topic_exist(self, project_id, topic_name):
        topic_path = self.__pubsub_client.topic_path(project_id, topic_name)

        try:
            self.__pubsub_client.get_topic(request={"topic": topic_path})
            return True
        except Exception as e:
            if "Resource not found" in str(e):
                return False
            else:
                logging.error("\n" + str(e))

    def list_pubsub_topics(self, project_id):
        project_path = f"projects/{project_id}"
        return self.__pubsub_client.list_topics(request={"project": project_path})

    def delete_pubsub_topic(self, project_id, topic_name, confirm=True):
        if self.does_pubsub_topic_exist(project_id, topic_name):
            if confirm:
                user_decision = self.__bq.user_prompt(
                    prompt=f'Pub/Sub topic, "{topic_name}", already exists. Do you want to delete it?',
                    action_response=f'deleting "{topic_name}"',
                    non_action_response=f'will not delete "{topic_name}"',
                )
            else:
                user_decision = True

            if user_decision:
                topic_path = self.__pubsub_client.topic_path(project_id, topic_name)
                self.__pubsub_client.delete_topic(request={"topic": topic_path})
                print(f'SUCCESS: Pub/Sub topic, "{topic_name}", successfully deleted!')
        else:
            print(f'Pub/Sub topic, "{topic_name}", does not exist!')

    def create_pubsub_topic(
        self, project_id, topic_name, labels=None, kms_key_name=None, message_retention_duration=None, confirm=True
    ):
        if self.does_pubsub_topic_exist(project_id, topic_name):
            self.delete_pubsub_topic(project_id, topic_name, confirm)

        topic_path = self.__pubsub_client.topic_path(project_id, topic_name)
        topic = pubsub_v1.types.Topic(name=topic_path, labels=labels, kms_key_name=kms_key_name)

        if message_retention_duration:
            message_retention = duration_pb2.Duration(seconds=message_retention_duration)
            topic.message_retention_duration = message_retention

        response = self.__pubsub_client.create_topic(request=topic)
        print(f'SUCCESS: Pub/Sub topic, "{topic_name}", created!')
        return response

    def does_scheduler_job_exist(self, project_id, location, job_name):
        for job in self.list_scheduler_jobs(project_id, location):
            if job.name == f"projects/{project_id}/locations/{location}/jobs/{job_name}":
                return True

        return False

    def list_scheduler_jobs(self, project_id, location):
        parent = f"projects/{project_id}/locations/{location}"
        return self.__scheduler_client.list_jobs(parent=parent)

    def delete_scheduler_job(self, project_id, location, job_name, confirm=True):
        if self.does_scheduler_job_exist(project_id, location, job_name):
            if confirm:
                user_decision = self.__bq.user_prompt(
                    prompt=f'Cloud Scheduler job, "{job_name}", already exists. Do you want to delete it?',
                    action_response=f'deleting "{job_name}"',
                    non_action_response=f'will not delete "{job_name}"',
                )
            else:
                user_decision = True

            if user_decision:
                job_path = f"projects/{project_id}/locations/{location}/jobs/{job_name}"
                self.__scheduler_client.delete_job(name=job_path)
                print(f'SUCCESS: Cloud Scheduler job, "{job_name}", successfully deleted!')
        else:
            print(f'Cloud Scheduler job, "{job_name}", does not exist!')

    def create_scheduler_job(
        self, project_id, location, job_name, schedule, timezone, topic_name, payload=None, confirm=True
    ):
        if self.does_scheduler_job_exist(project_id, location, job_name):
            self.delete_scheduler_job(project_id, location, job_name, confirm)

        parent = f"projects/{project_id}/locations/{location}"
        job = {
            "name": f"{parent}/jobs/{job_name}",
            "schedule": schedule,
            "time_zone": timezone,
            "pubsub_target": {
                "topic_name": f"projects/{project_id}/topics/{topic_name}",
                "data": payload.encode() if payload else None,
            },
        }

        response = self.__scheduler_client.create_job(parent=parent, job=job)
        print(f'SUCCESS: Cloud Scheduler job, "{job_name}", created!')
        return response

    def does_cloud_function_exist(self, project_id, location, function_name):
        for function in self.list_cloud_functions(project_id, location):
            if function.name == f"projects/{project_id}/locations/{location}/functions/{function_name}":
                return True

        return False

    def list_cloud_functions(self, project_id, location):
        parent = f"projects/{project_id}/locations/{location}"
        return self.__functions_client.list_functions(parent=parent)

    def delete_cloud_function(self, project_id, location, function_name, confirm=True):
        if self.does_cloud_function_exist(project_id, location, function_name):
            if confirm:
                user_decision = self.__bq.user_prompt(
                    prompt=f'Cloud Function, "{function_name}", already exists. Do you want to delete it?',
                    action_response=f'deleting "{function_name}"',
                    non_action_response=f'will not delete "{function_name}"',
                )
            else:
                user_decision = True

            if user_decision:
                function_path = f"projects/{project_id}/locations/{location}/functions/{function_name}"
                self.__functions_client.delete_function(name=function_path)
                print(f'SUCCESS: Cloud Function, "{function_name}", successfully deleted!')
        else:
            print(f'Cloud Function, "{function_name}", does not exist!')

    def create_cloud_function(
        self,
        function_name,
        trigger_topic,
        source,
        runtime,
        entry_point,
        region,
        timeout,
        memory,
        service_account,
        show_output=False,
    ):
        command = (
            f"gcloud functions deploy {function_name} \\\n"
            f"    --trigger-topic={trigger_topic} \\\n"
            f"    --source={source} \\\n"
            f"    --runtime={runtime} \\\n"
            f"    --entry-point={entry_point} \\\n"
            f"    --region={region} \\\n"
            f"    --timeout={timeout} \\\n"
            f"    --memory={memory} \\\n"
            f"    --service-account={service_account} \\\n"
            f"    --gen2"
        )
        # print(command)

        self.run_cli_command(command, show_output)

    def run_cli_command(self, command, show_output=True):
        # Run the command
        process = subprocess.Popen(
            command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # Get the output and error
        output, error = process.communicate()

        # Print the output and error if show_output = True
        if show_output:
            print("\nCommand:")
            print(command)

            # Check if the command was successful (return code 0)
            if process.returncode == 0 and error:
                # Append the stderr output to the output variable
                output += error

            if output:
                print("\nOutput:")
                print(output)

            if error and process.returncode != 0:  # Check if there's an actual error
                print("\nError:")
                print(error)

        return output
