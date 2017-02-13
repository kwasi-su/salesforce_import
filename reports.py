from simple_salesforce import Salesforce
import os
import csv
import requests
import psycopg2


def make_csv_report(sub_domain, report_id, csv_filename):
    """

    :param sub_domain: the first part of your salesforce report url. Comes after the https:// and before the salesforce.com
    :param report_id:
    :param csv_filename:
    :return:
    """
    sf = Salesforce(username=os.environ.get("SF_USERNAME"),
                    password=os.environ.get("SF_PASSWORD"),
                    security_token=os.environ.get("SF_TOKEN"),
                    )

    url = "https://" + sub_domain + ".salesforce.com/" + report_id + "?view=d&snip&export=1&enc=UTF-8&xf=csv"

    response = requests.get(url, headers=sf.headers, cookies={'sid': sf.session_id})

    decoded_content = response.content.decode('utf-8')
    data = list(csv.reader(decoded_content.splitlines(), delimiter=','))
    print("records downloaded: ", len(data))
    csv_path = "csv/" + csv_filename

    try:
        # check if file exists, if so, delete it and make a new file
        writer = csv.writer(open(csv_path, "r"))
        if writer:
            os.remove(csv_path)
            print(csv_filename, " removed!")
        write_csv_file(csv_path, data)

    except IOError:
        write_csv_file(csv_path, data)



def write_csv_file(filepath, data):
    with open(filepath, "w") as f:
        writer = csv.writer(f)
        for row in data:
            if len(str(row)) > 3:
                writer.writerow(row)
            else:
                print("reached end of report")
                break

        filename = filepath.replace("csv/", "")
        print(filename, "created!")


def import_csv_data_into_database(db_name, user, host='localhost', port='5432'):

    database_params = "host={0} port={1} db_name={2} user={3}".format(host, port, db_name, user)

    connection = pyscopg2.connect(database_params)

    # rest of the method coming soon



# Call your methods here. I have included sample calls
make_csv_report('na42', '00OF0000006tIhr', 'KA_SFExport_SUEvents_Attendees_Contact_20170213')
make_csv_report('na42', '00OF0000006tIhh', 'KA_SFExport_Speakers_Contacts_20170213')
make_csv_report('na42', '00OF0000006tIhX', 'KA_SFExport_SUEvents_20170130')
make_csv_report('na42', '00OF0000006tIhm', 'KA_SFExport_Applicationforms_Attendees_20170123')
make_csv_report('na42', '00OF0000006tHZu', 'KA_SFExport_Contacts_Applicationforms_Attendees_20170123')


