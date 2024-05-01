import subprocess
import json


class CliUtils:
    def __init__(self):
        pass

    def does_pubsub_topic_exist(self, pubsub_topic_name, project_id="zsc-personal"):
        topics = self.list_pubsub_topics(show_output=False)
        for topic in topics:
            if topic["name"] == f"projects/{project_id}/topics/{pubsub_topic_name}":
                return True
        return False

    def delete_pubsub_topic(self, pubsub_topic_name, project_id="zsc-personal"):
        if self.does_pubsub_topic_exist(pubsub_topic_name, project_id):
            self.run_cli_command(f"gcloud pubsub topics delete {pubsub_topic_name}")
        else:
            print(f"{pubsub_topic_name} Pub/Sub topic does not exist!")

    def create_pubsub_topic(self, pubsub_topic_name, project_id="zsc-personal"):
        if self.does_pubsub_topic_exist(pubsub_topic_name, project_id):
            print("Pub/Sub topic already exists. Deleting then re-creating")
            self.delete_pubsub_topic(pubsub_topic_name, project_id)

        self.run_cli_command(f"gcloud pubsub topics create {pubsub_topic_name}")

    def list_pubsub_topics(self, show_output=True):
        return json.loads(self.run_cli_command("gcloud pubsub topics list --format=json", show_output))

    def deploy_cloud_scheduler(
        self,
        schedule_name,
        trigger_topic,
        schedule_cron,
        time_zone,
        region,
        message_body,
        show_output=True,
    ):
        command = (
            f"gcloud scheduler jobs create pubsub {schedule_name} \\\n"
            f"    --topic={trigger_topic} \\\n"
            f"    --schedule='{schedule_cron}' \\\n"
            f"    --time-zone={time_zone} \\\n"
            f"    --location={region} \\\n"
            f"    --message-body='{message_body}'"
        )
        print(command)

    def deploy_cloud_function(
        self,
        function_name,
        trigger_topic,
        source,
        runtime,
        entry_point,
        region,
        timeout,
        service_account,
        show_output=True,
    ):
        command = (
            f"gcloud functions deploy {function_name} \\\n"
            f"    --trigger-topic={trigger_topic} \\\n"
            f"    --source={source} \\\n"
            f"    --runtime={runtime} \\\n"
            f"    --entry-point={entry_point} \\\n"
            f"    --region={region} \\\n"
            f"    --timeout={timeout} \\\n"
            f"    --service-account={service_account} \\\n"
            f"    --gen2"
        )

        print(command)

        # self.run_cli_command(command, show_output)

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
