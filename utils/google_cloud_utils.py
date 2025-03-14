import subprocess
from google.cloud import pubsub_v1
from google.cloud import scheduler_v1
from google.cloud import functions_v2
from google.protobuf import duration_pb2
from utils.bq_utils import BqUtils
from schemas.cloud_schemas import CloudSchemas


class GcpUtils:
    def __init__(self, bq_client):
        self._bq = BqUtils(bq_client=bq_client)
        self._cs = CloudSchemas()
        self._pubsub_client = pubsub_v1.PublisherClient()
        self._scheduler_client = scheduler_v1.CloudSchedulerClient()
        self._functions_client = functions_v2.FunctionServiceClient()

    def does_pubsub_topic_exist(self, project_id, trigger_topic):
        topic_path = self._pubsub_client.topic_path(project_id, trigger_topic)

        try:
            self._pubsub_client.get_topic(request={"topic": topic_path})
            return True
        except Exception as e:
            if "Resource not found" in str(e):
                return False
            else:
                print("\n" + str(e))

    def list_pubsub_topics(self, project_id):
        project_path = f"projects/{project_id}"
        return self._pubsub_client.list_topics(request={"project": project_path})

    def delete_pubsub_topic(self, project_id, trigger_topic, confirm=True):
        if not self.does_pubsub_topic_exist(project_id, trigger_topic):
            print(f'Pub/Sub topic, "{trigger_topic}", does not exist!')
            return

        user_decision = True
        if confirm:
            user_decision = self._bq.user_prompt(
                prompt=f'Pub/Sub topic, "{trigger_topic}", already exists. Do you want to delete it?',
                action_response=f'deleting "{trigger_topic}"',
                non_action_response=f'will not delete "{trigger_topic}"',
            )

        if user_decision:
            topic_path = self._pubsub_client.topic_path(project_id, trigger_topic)
            self._pubsub_client.delete_topic(request={"topic": topic_path})
            print(f'SUCCESS: Pub/Sub topic, "{trigger_topic}", successfully deleted!')

    def create_pubsub_topic(self, schema, confirm=True):
        if self.does_pubsub_topic_exist(schema.project_id, schema.trigger_topic):
            self.delete_pubsub_topic(schema.project_id, schema.trigger_topic, confirm)

        topic_path = self._pubsub_client.topic_path(schema.project_id, schema.trigger_topic)
        topic = pubsub_v1.types.Topic(
            name=topic_path, labels=schema.labels, kms_key_name=schema.kms_key_name
        )

        if schema.message_retention_duration:
            message_retention = duration_pb2.Duration(seconds=schema.message_retention_duration)
            topic.message_retention_duration = message_retention

        response = self._pubsub_client.create_topic(request=topic)
        print(f'SUCCESS: Pub/Sub topic, "{schema.trigger_topic}", created!')
        return response

    def does_scheduler_job_exist(self, project_id, region, job_name):
        for job in self.list_scheduler_jobs(project_id, region):
            if job.name == f"projects/{project_id}/locations/{region}/jobs/{job_name}":
                return True

        return False

    def list_scheduler_jobs(self, project_id, region):
        parent = f"projects/{project_id}/locations/{region}"
        return self._scheduler_client.list_jobs(parent=parent)

    def delete_scheduler_job(self, project_id, region, job_name, confirm=True):
        if not self.does_scheduler_job_exist(project_id, region, job_name):
            print(f'Cloud Scheduler job, "{job_name}", does not exist!')
            return

        user_decision = True
        if confirm:
            user_decision = self._bq.user_prompt(
                prompt=f'Cloud Scheduler job, "{job_name}", already exists. Do you want to delete it?',
                action_response=f'deleting "{job_name}"',
                non_action_response=f'will not delete "{job_name}"',
            )

        if user_decision:
            job_path = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
            self._scheduler_client.delete_job(name=job_path)
            print(f'SUCCESS: Cloud Scheduler job, "{job_name}", successfully deleted!')

    def _create_pubsub_target(self, schema):
        return {
            "pubsub_target": {
                "topic_name": f"projects/{schema.project_id}/topics/{schema.trigger_topic}",
                "data": schema.payload.encode() if schema.payload else None,
            }
        }

    def _create_http_target(self, schema):
        return {
            "http_target": {
                "http_method": schema.http_method,
                "uri": self.get_cloud_function_http_uri(
                    schema.project_id, schema.region, schema.function_name
                ),
                "headers": {"Content-Type": "application/json"},
            }
        }

    def create_scheduler_job(self, schema, confirm=True):
        if self.does_scheduler_job_exist(schema.project_id, schema.region, schema.job_name):
            self.delete_scheduler_job(schema.project_id, schema.region, schema.job_name, confirm)

        parent = f"projects/{schema.project_id}/locations/{schema.region}"

        # Determine the job target based on the type specified
        if schema.target_type == "pubsub":
            target = self._create_pubsub_target(schema)
        elif schema.target_type == "http":
            target = self._create_http_target(schema)
        else:
            raise ValueError("Invalid target type. Must be either 'pubsub' or 'http'.")

        job = {
            "name": f"{parent}/jobs/{schema.job_name}",
            "schedule": schema.schedule,
            "time_zone": schema.timezone,
            **target,
        }

        response = self._scheduler_client.create_job(parent=parent, job=job)
        print(
            f'SUCCESS: Cloud Scheduler job, "{schema.job_name}", created with a {schema.target_type} target!'
        )
        return response

    def does_cloud_function_exist(self, project_id, region, function_name):
        for function in self.list_cloud_functions(project_id, region):
            if (
                function.name
                == f"projects/{project_id}/locations/{region}/functions/{function_name}"
            ):
                return True

        return False

    def list_cloud_functions(self, project_id, region):
        parent = f"projects/{project_id}/locations/{region}"
        return self._functions_client.list_functions(parent=parent)

    def delete_cloud_function(self, project_id, region, function_name, confirm=True):
        if not self.does_cloud_function_exist(project_id, region, function_name):
            print(f'Cloud Function, "{function_name}", does not exist!')
            return

        user_decision = True
        if confirm:
            user_decision = self._bq.user_prompt(
                prompt=f'Cloud Function, "{function_name}", already exists. Do you want to delete it?',
                action_response=f'deleting "{function_name}"',
                non_action_response=f'will not delete "{function_name}"',
            )

        if user_decision:
            function_path = f"projects/{project_id}/locations/{region}/functions/{function_name}"
            self._functions_client.delete_function(name=function_path)
            print(f'SUCCESS: Cloud Function, "{function_name}", successfully deleted!')

    def get_cloud_function_http_uri(self, project_id, region, function_name):
        function_path = self._functions_client.function_path(project_id, region, function_name)
        function = self._functions_client.get_function(name=function_path)
        if function.url:
            return function.url
        else:
            raise ValueError(f"Function '{function_name}' does not have an HTTP trigger.")

    def create_cloud_function(self, schema, show_output=False):
        # Determine the job target based on the type specified
        if schema.target_type == "pubsub":
            target = f"--trigger-topic={schema.trigger_topic} \\\n"
        elif schema.target_type == "http":
            target = "--allow-unauthenticated \\\n    --trigger-http \\\n"
        else:
            raise ValueError("Invalid target type. Must be either 'pubsub' or 'http'.")

        command = (
            f"gcloud functions deploy {schema.function_name} \\\n"
            f"    {target}"
            f"    --source={schema.source} \\\n"
            f"    --runtime={schema.runtime} \\\n"
            f"    --entry-point={schema.entry_point} \\\n"
            f"    --region={schema.region} \\\n"
            f"    --timeout={schema.timeout} \\\n"
            f"    --memory={schema.memory} \\\n"
            f"    --service-account={schema.service_account} \\\n"
            f"    --gen2"
        )
        # print(command)

        self.run_cli_command(command, show_output)

    def run_cli_command(self, command, show_output=True):
        # Run the command
        process = subprocess.Popen(
            command,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
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
