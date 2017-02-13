from simple_salesforce import Salesforce
import os
import csv
import requests
import psycopg2


def make_csv_report(domain, report_id, csv_filename):
    sf = Salesforce(username=os.environ.get("SF_USERNAME"),
                    password=os.environ.get("SF_PASSWORD"),
                    security_token=os.environ.get("SF_TOKEN"),
                    )

    url = "https://" + domain + ".salesforce.com/" + report_id + "?view=d&snip&export=1&enc=UTF-8&xf=csv"

    response = requests.get(url, headers=sf.headers, cookies={'sid': sf.session_id})

    decoded_content = response.content.decode('utf-8')
    data = list(csv.reader(decoded_content.splitlines(), delimiter=','))
    print("records downloaded: ", len(data))

    try:
        # check if file exists, if so, delete it and make a new file
        writer = csv.writer(open(csv_filename, "r"))
        if writer:
            os.remove(csv_filename)
            print(csv_filename, " removed!")
        write_csv_file(csv_filename, data)

    except IOError:
        write_csv_file(csv_filename, data)



def write_csv_file(filename, data):
    with open(filename, "w") as f:
        writer = csv.writer(f)
        for row in data:
            if len(str(row)) > 3:
                writer.writerow(row)
            else:
                print("reached end of report")
                break
        print(filename, "created!")


def import_csv_data_into_database(db_name, user, host='localhost', port='5432'):

    database_params = "host={0} port={1} db_name={2} user={3}".format(host, port, db_name, user)

    connection = pyscopg2.connect(database_params)

make_csv_report('na42', '00OF0000006tIhr', 'KA_SFExport_SUEvents_Attendees_Contact_20170213')
make_csv_report('na42', '00OF0000006tIhh', 'KA_SFExport_Speakers_Contacts_20170213')
make_csv_report('na42', '00OF0000006tIhX', 'KA_SFExport_SUEvents_20170130')
make_csv_report('na42', '00OF0000006tIhm', 'KA_SFExport_Applicationforms_Attendees_20170123')
make_csv_report('na42', '00OF0000006tHZu', 'KA_SFExport_Contacts_Applicationforms_Attendees_20170123')


