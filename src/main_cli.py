from utils.cli_utils import CliUtils


def main(context, data):
    cli = CliUtils()
    pubsub_topic_name = "test-cloud-topic"
    region = "us-west1"
    service_account = "zsc-service-account@zsc-personal.iam.gserviceaccount.com"

    # pub_command = cli.create_pubsub_topic(pubsub_topic_name)
    # pub_command_2 = cli.delete_pubsub_topic(pubsub_topic_name)
    # print(pub_command)

    # cli.deploy_cloud_function(
    #     function_name="test-function",
    #     trigger_topic=pubsub_topic_name,
    #     source=".",
    #     runtime="python38",
    #     entry_point="main",
    #     region=region,
    #     timeout="540",
    #     service_account=service_account,
    # )

    cli.deploy_cloud_scheduler(
        schedule_name="test-schedule",
        trigger_topic=pubsub_topic_name,
        schedule_cron="* * * * *",
        time_zone="UTC",
        region=region,
        message_body="Hello World Schedule Test",
        show_output=True,
    )


if __name__ == "__main__":
    main("data", "context")
