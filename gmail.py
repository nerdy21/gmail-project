import httplib2
from apiclient.discovery import build
import pymysql.cursors
import base64
from bs4 import BeautifulSoup

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

# initialize db connection
connection = pymysql.connect(host='db4free.net', user='gmail_test',
    password='gmail_test', db="gmail_test", cursorclass=pymysql.cursors.DictCursor
)

CLIENT_SECRET = '/Users/Newmac/.credentials/client_secret.json'
SCOPE = 'https://mail.google.com/'
STORAGE = Storage('credentials.storage')
RESULT_COUNT = 50

# start the oauth flow to retrieve credentials
def authorize_credentials():
    # fetch credentials from storage
    credentials = STORAGE.get()
    # if the credentials doesn't exist in the storage location then run the flow
    if credentials is None or credentials.invalid:
        flow = flow_from_clientsecrets(CLIENT_SECRET, scope=SCOPE)
        http = httplib2.Http()
        credentials = run_flow(flow, STORAGE, http=http)

    return credentials

# retrieve messages from a given thread_id
def get_messages(thread_id):

    # retrieve and authorize credentials
    credentials = authorize_credentials()
    http = credentials.authorize(httplib2.Http())

    # create gmail instance
    service = build('gmail', 'v1', http=http)
    messages = service.users().threads().get(userId='me', id=thread_id).execute().get('messages')

    return messages


# save messages to database
def save_to_db(messages):

    for message in messages:

        sql_command = "INSERT INTO `messages` (`id`, `message`, `subject`,"\
            "`read_receipt`, `received_at`, `from_id`) VALUES (%s, %s, %s, %s)"

        connection.cursor().execute(sql_command, (
            message['id'], message['message'], message['subject'],
            message['read_receipt'], message['received_at'], message['from_id']
        ))
        # commit the changes
        connection.commit()

    return True


# retrieve emails and store them in database
def get_emails():

    print("Email Retrival Process Started!")

    # retrieve and credentials
    credentials = authorize_credentials()
    http = credentials.authorize(httplib2.Http())
    # create gmail instance
    service = build('gmail', 'v1', http=http)

    # loop through pages to read all the emails
    nextPageToken = True
    while nextPageToken:
        # check for nextpagetoken and use it if available
        if type(nextPageToken) == bool:
            threads = service.users().threads().list(userId='me',
                maxResults=RESULT_COUNT
            ).execute()
        else:
            threads = service.users().threads().list(userId='me',
                maxResults=RESULT_COUNT, pageToken=nextPageToken
            ).execute()

        # extract nextPageToken
        if 'nextPageToken' in threads:
            nextPageToken = threads['nextPageToken']
        else:
            nextPageToken = False

        # loop through threads and read messages
        for thread in threads['threads']:
            thread_messages = get_messages(thread['id'])

            messages = []
            # save new messages to the database
            for message in thread_messages:

                sql_command = "SELECT `id` FROM `messages` WHERE `id`=%s"
                result = connection.cursor().execute(sql_command,
                    (message['id'])
                )
                if result != 0:
                    continue

                for header in message['payload']['headers']:
                    if header['name'] == "Subject":
                        subject = header['value']
                    elif header['name'] == "From":
                        from_id = header['value']
                    elif header['name'] == "Date":
                        received_at = header['value']

                # decode body text
                msg_str = base64.b64encode(
                    bytes(message['payload']['parts'][0]['body']['data'], 'UTF-8')
                )

                # parse decoded body text using html parser
                body_message = BeautifulSoup(msg_str, "html.parser")

                messages.append({
                    "id": message['id'],
                    "subject": subject,
                    "message": body_message,
                    "read_receipt": "FALSE",
                    "received_at": received_at,
                    "from_id": from_id
                })

            save_to_db(messages)

        print("Page Completed!")

    print("Email Retrival Process Completed!")

    return True


get_emails()
