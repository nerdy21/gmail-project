import pymysql.cursors

# read rules json and create a rules dict
rules_data = open("rules.json").read()

RULES = json.loads(rules_data)
BATCH_SIZE = 50

# initialize connection to the database
connection = pymysql.connect(host='db4free.net', user='gmail_test',
    password='gmail_test', db="gmail_test", cursorclass=pymysql.cursors.DictCursor
)

# check for contains condition
def check_contains(input_data):

    # loop through the values and check if it occurs in the input string
    for entry in RULES['from_sub_conditions']['value']:
        if entry in input_data:
            return True

    return False


# check for is equal to condition
def check_is_equal(input_data):

    # loop through the values and check if it's equal to the input_data
    for entry in RULES['from_sub_conditions']['value']:
        if input_data == entry:
            return True

    return False


# check from data
def check_from(input_data):

    if RULES['from_sub_conditions']['condition'] == "CONTAINS":
        return check_contains(input_data)

    elif RULES['from_sub_conditions']['condition'] == "IS_EQUAL":
        return check_is_equal(input_data))


# check subject data
def check_subject(input_data):

    if RULES['subject_sub_conditions']['condition'] == "CONTAINS":
        return check_contains(input_data)

    elif RULES['subject_sub_conditions']['condition'] == "IS_EQUAL":
        return check_is_equal(input_data))


def check_received_at(input_data):

    # check input data and time range to see if message was received in between

    return True


# process message by checking with RULES json
def process_message(message):

    # check rules and process message accordingly
    if RULES['primary_condition'] == "AND":

        if (
            check_from(message.from_id) and
            check_subject(message.subject) and
            check_received_at(message.received_at)
            ):
            process = True

        else:
            process = False

    elif RULES['primary_condition'] == "OR":

        if (
            check_from(message.from_id) or
            check_subject(message.subject) or
            check_received_at(message.received_at)
            ):
            process = True

        else:
            process = False

    if process == True:
        # add label, mark as processed and save it
        pass
    else:
        # mark as read, mark as processed and save it
        pass


def read_messages():
    print("Starting Message Processing!")

    # get the count of un processed messages
    sql_command = "SELECT count(*) FROM `messages` WHERE `processed_flag`=%s"
    result = connection.cursor().execute(sql_command, ("False"))
    count = cursor.fetchone()[0]

    # us the count to paginate the messages
    for offset in xrange(0, count, BATCH_SIZE):
        sql_command = "SELECT * FROM `messages` WHERE `processed_flag`=%s LIMIT %s OFFSET %s
        result = connection.cursor().execute(sql_command, ("False"))
        messages = result.fetchall()

        # loop through messages to process it
        for message in messages:
            process_message(message)

    print("Processing Done!")

read_messages()
