from typing import List, Dict, Any, Tuple
import pandas as pd
from sendgrid import SendGridAPIClient  # type: ignore
from sendgrid.helpers.mail import Mail  # type: ignore


class SendgridUtils:

    def __init__(self, sendgrid_api_key: str) -> None:
        self._sendgrid_api_key = sendgrid_api_key

    def create_html_message_with_pandas_df(self, intro_text: str, df: pd.DataFrame) -> str:
        html_content_df = df.to_html(index=False)
        return f"""<p>{intro_text}</p>
        {html_content_df}"""

    def chain_html_messages(self, messages: List[str]) -> str:
        final_message = ""
        for message in messages:
            final_message += message

            # add break if not last message
            if message != messages[-1]:
                final_message += "<br><br>"
        return final_message

    def construct_email_message(
        self, from_email: str, to_emails: List[str], email_subject: str, html_message: str
    ) -> Mail:
        return Mail(
            from_email=from_email,
            to_emails=to_emails,
            subject=email_subject,
            html_content=html_message,
        )

    def send_email(self, email_message: Mail) -> None:
        sg = SendGridAPIClient(self._sendgrid_api_key)
        sg.send(email_message)

        email_parameters = self.get_email_parameters(email_message)
        email_subject = self.get_email_subject(email_parameters)
        email_recipients = ", ".join(self.get_to_emails(email_parameters))

        print(f'SUCCESS: email with subject, "{email_subject}" sent to -> {email_recipients}')

    def get_email_parameters(self, mail_message: Mail) -> Dict[str, Any]:
        return mail_message.get()

    def get_to_emails(self, email_parameters: Dict[str, Any]) -> List[str]:
        to_emails = []
        for personalization in email_parameters.get("personalizations", []):
            for to_email in personalization.get("to", []):
                to_emails.append(to_email["email"])
        return to_emails

    def get_from_email(self, email_parameters: Dict[str, Any]) -> str:
        return email_parameters.get("from", {}).get("email", None)

    def get_email_subject(self, email_parameters: Dict[str, Any]) -> str:
        return email_parameters.get("subject", "")

    def get_email_content(self, email_parameters: Dict[str, Any]) -> List[Tuple[str, str]]:
        email_content = []
        for content in email_parameters.get("content", []):
            email_content.append((content["type"], content["value"]))
        return email_content
